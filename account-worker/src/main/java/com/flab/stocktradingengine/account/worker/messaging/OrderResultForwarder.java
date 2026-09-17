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
import com.flab.stocktradingengine.codec.OrderResultCodec;
import com.flab.stocktradingengine.codec.OrderResultEntry;
import com.flab.stocktradingengine.kafka.KafkaTopics;
import com.flab.stocktradingengine.kafka.event.OrderResultEvent;

import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;

/**
 * Archive에 기록된 주문 결과를 replay로 읽어 {@code order-results} 토픽으로 옮기는 전용 스레드
 * (U3-a). {@link com.flab.stocktradingengine.account.disruptor.io.AccountFillReceiver}와 같은
 * 결(전용 폴 스레드가 Subscription을 계속 폴링해 디코딩·전달만 한다)이지만, 이 Subscription은
 * 라이브 스트림이 아니라 {@code archive.replay(recordingId, 0, Long.MAX_VALUE, ...)}로 연 replay다
 * — 이유는 {@code OrderResultArchiveConfig}·LLD의 "왜 라이브 Subscription이 아니라 Archive
 * replay인가" 절 참고(느린 구독자가 U2의 결과 Publication을 막아 핫패스로 지연이 전파되는 것을
 * 막기 위함, `_investigation_order_result_backpressure.md`에 재현 근거).
 *
 * <h3>한계 — 재시작하면 이전 실행 구간이 유실된다(U3-b에서 닫는다)</h3>
 * <p>이 replay는 "이번 실행에서 시작한 recording" 하나만 읽는다. 재시작하면 새 recording이
 * 생기고 이 forwarder는 그 새 recording만 보므로, 이전 실행에서 아직 Kafka로 못 보낸 판정은
 * 이 상태로는 영영 Kafka로 가지 않는다. {@code AccountJournalReplayer}가 이전 녹화를 전부
 * 시작 시각 순서로 이어 읽는 것과 같은 구조의 문제이고, U3-b가 recordingId·position을 이어받아
 * 닫는다. 완료 판정 아님 — 정상 동작의 한계다.</p>
 *
 * <h3>content-poison — 디코딩 실패는 skip, 스레드는 죽지 않는다</h3>
 * <p>{@link OrderResultCodec#tryDecode}가 빈 값을 돌려주면 그 fragment만 경고 로그와 함께
 * skip하고 계속 돈다 — {@link AccountFillReceiver}와 같은 정책. 예외를 던져 폴 스레드를 죽이지
 * 않는다(이 스트림은 저널이 아니라 클라이언트 알림용이라, 한 건이 손상됐다고 이후 처리를 전부
 * 멈출 이유가 없다).</p>
 */
public final class OrderResultForwarder implements AutoCloseable {

    private static final Logger log = System.getLogger(OrderResultForwarder.class.getName());
    private static final int FRAGMENT_LIMIT = 10;
    private static final long CLOSE_JOIN_TIMEOUT_MILLIS = 1000L;
    private static final String TOPIC = KafkaTopics.orderResults();

    private final Subscription subscription;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final OrderResultCodec codec = new OrderResultCodec();
    private final IdleStrategy idleStrategy = new BackoffIdleStrategy();
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final FragmentHandler fragmentHandler = this::onFragment;

    private Thread pollThread;

    public OrderResultForwarder(Subscription subscription, KafkaTemplate<String, Object> kafkaTemplate) {
        this.subscription = subscription;
        this.kafkaTemplate = kafkaTemplate;
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
        OrderResultEntry entry = decoded.get();
        OrderResultEvent event = new OrderResultEvent(
            entry.accountId(), entry.orderId(), entry.requestId(), entry.verdict(), entry.rejectReason(), entry.epochMillis());
        kafkaTemplate.send(TOPIC, String.valueOf(entry.accountId()), event);
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
