package com.flab.stocktradingengine.account.worker.messaging;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.agrona.DirectBuffer;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.springframework.kafka.core.KafkaTemplate;

import com.flab.stocktradingengine.account.disruptor.io.AccountFillReceiver;
import com.flab.stocktradingengine.account.worker.recovery.OrderResultForwardPositionStore;
import com.flab.stocktradingengine.codec.OrderResultCodec;
import com.flab.stocktradingengine.codec.OrderResultEntry;
import com.flab.stocktradingengine.kafka.KafkaTopics;
import com.flab.stocktradingengine.kafka.event.OrderResultEvent;

import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;

/**
 * Archive에 기록된 주문 결과를 replay로 읽어 {@code order-results} 토픽으로 옮기는 전용 스레드
 * (U3-a). {@link AccountFillReceiver}와 같은 결(전용 폴 스레드가 Subscription을 계속 폴링해
 * 디코딩·전달만 한다)이지만, 이 Subscription은 라이브 스트림이 아니라
 * {@code archive.replay(recordingId, 0, Long.MAX_VALUE, ...)}로 연 replay다 — 이유는
 * {@code OrderResultArchiveConfig}·LLD의 "왜 라이브 Subscription이 아니라 Archive replay인가"
 * 절 참고(느린 구독자가 U2의 결과 Publication을 막아 핫패스로 지연이 전파되는 것을 막기 위함,
 * `_investigation_order_result_backpressure.md`에 재현 근거).
 *
 * <h3>content-poison — 디코딩 실패는 skip, 스레드는 죽지 않는다</h3>
 * <p>{@link OrderResultCodec#tryDecode}가 빈 값을 돌려주면 그 fragment만 경고 로그와 함께
 * skip하고 계속 돈다 — {@link AccountFillReceiver}와 같은 정책. 예외를 던져 폴 스레드를 죽이지
 * 않는다(이 스트림은 저널이 아니라 클라이언트 알림용이라, 한 건이 손상됐다고 이후 처리를 전부
 * 멈출 이유가 없다).</p>
 *
 * <h3>Kafka send 결과 확인 — 건별, 실패하면 그 자리에서 무한 재시도</h3>
 * <p>{@link #sendBlocking}이 send의 결과({@code Future#get()})를 확인할 때까지 이 스레드를
 * 블록한다. {@code AccountStatePublisher}·{@code SettlementRequestPublisher}는 결과를 안 보고
 * fire-and-forget으로 보내는데, 그 둘은 "상태 전체"를 보내 다음 것이 실패한 것을 덮어쓰지만
 * (다음 상태 발행이 성공하면 이전 실패는 의미가 없어진다), 판정은 사건 하나라 대신할 것이 없다 —
 * 이 send가 실패한 채로 넘어가면 그 판정은 영영 사라진다. 그래서 여기만 다르게 간다.</p>
 * <p><b>건별 확인을 고른 이유</b>: 배치로 묶어 확인하면 "N건 중 몇 번째가 실패했나"를 찾는 로직이
 * 따로 필요하다. 이 스레드가 느려져도 계좌 엔진에는 영향이 없으므로(핫패스 밖) 처리량을 늦추는
 * 대가로 건별 확인의 단순함을 택했다.</p>
 * <p><b>재시도(대기, 재부팅에 안 미룸)를 고른 이유</b>: 멈추고 다음 기동에 맡기면 Kafka가 잠깐
 * 끊긴 것만으로도 사람이 재시작해야 이후 판정이 다시 흐른다. {@code AeronArchiveAccountJournal}이
 * offer 실패에 재시도로 대응하는 것과 같은 결로, 이 스레드가 살아있는 한 스스로 회복한다. 재시도
 * 중에도 position은 올라가지 않으므로({@link #maybeSavePosition}이 성공한 뒤에만 불린다) 재시작이
 * 실제로 필요해지면 U3-b의 캐치업이 그 지점부터 다시 읽어 보낸다 — 새로 만들 게 없다.</p>
 * <p><b>한계</b>: {@code OrderResultArchiveConfig}의 캐치업 단계도 같은 {@link #sendBlocking}을
 * 쓰는데, 그 호출은 Spring 빈 생성(앱 기동) 도중 일어난다 — Kafka가 기동 시점에 계속 죽어 있으면
 * 캐치업이 끝나지 않아 앱 기동 자체가 지연될 수 있다. 이 트랙 안에서 발견한 새 트레이드오프다.</p>
 */
public final class OrderResultForwarder implements AutoCloseable {

    private static final Logger log = System.getLogger(OrderResultForwarder.class.getName());
    private static final int FRAGMENT_LIMIT = 10;
    private static final long CLOSE_JOIN_TIMEOUT_MILLIS = 1000L;
    private static final String TOPIC = KafkaTopics.orderResults();
    // 재시작 중복 창의 상한(클래스 javadoc 참고) — 이 값보다 작게 잡을수록 창은 줄고 디스크 쓰기는 늘어난다.
    private static final int POSITION_SAVE_INTERVAL = 100;

    private final Subscription subscription;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final OrderResultForwardPositionStore positionStore;
    private final long recordingId;
    private final OrderResultCodec codec = new OrderResultCodec();
    private final IdleStrategy idleStrategy = new BackoffIdleStrategy();
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final FragmentHandler fragmentHandler = this::onFragment;

    private int entriesSinceLastSave = 0;
    private Thread pollThread;

    public OrderResultForwarder(Subscription subscription, KafkaTemplate<String, Object> kafkaTemplate,
            OrderResultForwardPositionStore positionStore, long recordingId) {
        this.subscription = subscription;
        this.kafkaTemplate = kafkaTemplate;
        this.positionStore = positionStore;
        this.recordingId = recordingId;
    }

    /** 폴링 스레드를 기동한다. */
    public void start() {
        running.set(true);
        pollThread = new Thread(this::pollLoop, "order-result-forwarder");
        pollThread.setDaemon(true);
        pollThread.start();
    }

    private void pollLoop() {
        while (running.get()) {
            int fragments = subscription.poll(fragmentHandler, FRAGMENT_LIMIT);
            idleStrategy.idle(fragments);
        }
    }

    /** 패키지 가시성 — 단위 테스트가 실제 Subscription 없이 이 메서드를 직접 호출한다. */
    void onFragment(DirectBuffer buffer, int offset, int length, Header header) {
        Optional<OrderResultEntry> decoded = codec.tryDecode(buffer, offset, length);
        if (decoded.isEmpty()) {
            log.log(Level.WARNING, "[계좌] 주문 결과 프레임 디코딩 실패로 skip: offset=" + offset + " length=" + length);
            return;
        }
        sendBlocking(kafkaTemplate, decoded.get());
        if (header != null) { // 단위 테스트가 실제 Aeron 전달 없이 이 메서드를 부를 때는 null(AccountFillReceiver와 같은 관례)
            maybeSavePosition(header.position());
        }
    }

    private void maybeSavePosition(long position) {
        entriesSinceLastSave++;
        if (entriesSinceLastSave >= POSITION_SAVE_INTERVAL) {
            positionStore.write(recordingId, position);
            entriesSinceLastSave = 0;
        }
    }

    /**
     * 엔트리 하나를 Kafka로 보내고, 성공(브로커 ack)할 때까지 이 스레드를 블록한다. 실패하면
     * 경고를 남기고 같은 엔트리로 재시도한다 — 클래스 javadoc "Kafka send 결과 확인" 절 참고.
     * {@code OrderResultArchiveConfig}의 캐치업 단계(U3-b)도 이 메서드를 그대로 쓴다.
     */
    public static void sendBlocking(KafkaTemplate<String, Object> kafkaTemplate, OrderResultEntry entry) {
        OrderResultEvent event = toEvent(entry);
        String key = String.valueOf(entry.accountId());
        IdleStrategy retryIdle = new BackoffIdleStrategy();
        while (true) {
            try {
                kafkaTemplate.send(TOPIC, key, event).get();
                return;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("주문 결과 Kafka 발행 대기 중 인터럽트됐습니다: accountId=" + entry.accountId(), e);
            } catch (ExecutionException e) {
                log.log(Level.WARNING,
                    "[계좌] 주문 결과 Kafka 발행 실패, 재시도합니다: accountId=" + entry.accountId() + " requestId=" + entry.requestId(),
                    e.getCause());
                retryIdle.idle();
            }
        }
    }

    private static OrderResultEvent toEvent(OrderResultEntry entry) {
        return new OrderResultEvent(
            entry.accountId(), entry.orderId(), entry.requestId(), entry.verdict(), entry.rejectReason(), entry.epochMillis());
    }

    /**
     * 폴링 스레드를 멈추고 종료를 기다린다. {@code CLOSE_JOIN_TIMEOUT_MILLIS} 안에 멈추지 않으면
     * 경고만 남긴다({@link AccountFillReceiver}와 같은 정책).
     */
    @Override
    public void close() {
        running.set(false);
        if (pollThread != null) {
            try {
                pollThread.join(CLOSE_JOIN_TIMEOUT_MILLIS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            if (pollThread.isAlive()) {
                log.log(Level.WARNING,
                    "[계좌] 주문 결과 forwarder 폴링 스레드가 " + CLOSE_JOIN_TIMEOUT_MILLIS + "ms 안에 멈추지 않았습니다");
            }
        }
    }
}
