package com.flab.stocktradingengine.account.worker.messaging;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.Optional;
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
 * <h3>position 저장 주기 = 중복 발행 창(U3-b)</h3>
 * <p>{@link #POSITION_SAVE_INTERVAL}건마다 한 번 (recordingId, position)을 디스크에 남긴다.
 * 마지막으로 저장한 뒤 Kafka로 보낸 것은 재시작하면 다시 보내진다(진짜 중복 — {@code
 * OrderResultCatchUpReplayer}가 저장된 position부터 다시 읽으므로) — 그래서 이 재시작 중복 창의
 * 크기는 **최대 {@code POSITION_SAVE_INTERVAL - 1}건**이다. 주기를 줄이면 이 창은 작아지지만
 * 디스크 쓰기(fsync 포함)가 그만큼 잦아진다. {@code AccountState.SETTLEMENT_RETENTION_LIMIT}처럼
 * 상한을 수치로 못 박아 둔다.</p>
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
        kafkaTemplate.send(TOPIC, String.valueOf(decoded.get().accountId()), toEvent(decoded.get()));
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

    /** {@code OrderResultArchiveConfig}의 캐치업 단계(U3-b)도 같은 매핑을 쓴다 — 중복 정의 방지. */
    public static OrderResultEvent toEvent(OrderResultEntry entry) {
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
