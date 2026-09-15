package com.flab.stocktradingengine.account.worker.messaging;

import java.math.BigDecimal;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.OneToOneConcurrentArrayQueue;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import com.flab.stocktradingengine.account.disruptor.domain.AccountResultListener;
import com.flab.stocktradingengine.account.disruptor.domain.RejectReason;
import com.flab.stocktradingengine.kafka.KafkaTopics;
import com.flab.stocktradingengine.kafka.event.AccountStateEvent;

import lombok.extern.slf4j.Slf4j;

/**
 * 잔고·보유 변경을 {@code account-state} 토픽에 full-state로 발행하는 {@link AccountResultListener}
 * 구현체(계좌 상태 영속/프로젝션 트랙 Unit 2). {@link AeronMatchingOrderSender}와 같은 SPSC
 * outbox 큐 + 전용 publisher 스레드 패턴을 미러한다.
 *
 * <h3>왜 {@link SettlementRequestPublisher}처럼 리스너에서 바로 send하지 않는가</h3>
 * <p>Kafka {@code send}는 보통 비동기지만, 프로듀서 버퍼가 차면 {@code max.block.ms}만큼
 * 호출 스레드를 블록할 수 있다. 정산(onUnpaidRecorded)은 저빈도라 그 리스크를 감수할 만하지만,
 * 체결로 인한 상태 변경은 빈도가 높아 계좌 엔진(single-writer) 스레드가 그 블로킹 리스크를
 * 지면 안 된다 — 그래서 {@link #onStateChanged}는 이미 캡처된(엔진 스레드에서 만든) full-state
 * 값을 outbox에 넣기만 하고(논블로킹), 실제 Kafka 전송은 {@link #publisherThread}가 큐를
 * 드레인하며 한다.</p>
 */
@Slf4j
@Component
public class AccountStatePublisher implements AccountResultListener, AutoCloseable {

    private static final String TOPIC = KafkaTopics.accountState();
    private static final int OUTBOX_CAPACITY = 65536;
    private static final long CLOSE_JOIN_TIMEOUT_MILLIS = 1000L;

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final OneToOneConcurrentArrayQueue<AccountStateEvent> outbox = new OneToOneConcurrentArrayQueue<>(OUTBOX_CAPACITY);
    private final IdleStrategy idleStrategy = new BackoffIdleStrategy();
    private final AtomicBoolean running = new AtomicBoolean(false);

    private Thread publisherThread;

    public AccountStatePublisher(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * 계좌 엔진 스레드가 호출한다 — 큐에 넣기만 하고 즉시 돌아간다(논블로킹, 예외 없음).
     * balance·holdings·seq는 이미 호출 시점(엔진 스레드)에 캡처된 값이다({@code AccountEventHandler} 참고).
     */
    @Override
    public void onStateChanged(long accountId, BigDecimal balance, Map<String, Integer> holdings, long seq) {
        AccountStateEvent event = new AccountStateEvent(accountId, balance, holdings, seq, System.currentTimeMillis());
        if (!outbox.offer(event)) {
            log.warn("[계좌] 상태 발행 큐가 가득 차 이번 상태를 버림(best-effort): accountId={} seq={}", accountId, seq);
        }
    }

    /** publisher 스레드를 기동한다. */
    public void start() {
        running.set(true);
        publisherThread = new Thread(this::publishLoop, "account-state-publisher");
        publisherThread.setDaemon(true);
        publisherThread.start();
    }

    private void publishLoop() {
        while (running.get()) {
            AccountStateEvent event = outbox.poll();
            if (event != null) {
                kafkaTemplate.send(TOPIC, String.valueOf(event.accountId()), event);
            }
            idleStrategy.idle(event != null ? 1 : 0);
        }
    }

    /** publisher 스레드를 멈추고 종료를 기다린다. 큐에 남은 상태는 드레인하지 않는다(best-effort). */
    @Override
    public void close() {
        running.set(false);
        if (publisherThread != null) {
            try {
                publisherThread.join(CLOSE_JOIN_TIMEOUT_MILLIS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public void onAccepted(long accountId, long orderId, String requestId, BigDecimal reservedMargin) {
    }

    @Override
    public void onSellAccepted(long accountId, long orderId, String requestId, int reservedQuantity) {
    }

    @Override
    public void onRejected(long accountId, long orderId, String requestId, RejectReason reason) {
    }

    @Override
    public void onFillApplied(long accountId, long orderId, long tradeId, boolean applied) {
    }

    @Override
    public void onSettlementApplied(long accountId, long settlementRef, boolean applied) {
    }

    @Override
    public void onUnpaidRecorded(long accountId, long tradeId, BigDecimal amount) {
    }

    @Override
    public void onDuplicateRequest(long accountId, long orderId, String requestId) {
    }
}
