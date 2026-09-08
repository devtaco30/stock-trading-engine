package com.flab.stocktradingengine.account.worker;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Duration;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import com.flab.stocktradingengine.account.disruptor.AccountResultListener;
import com.flab.stocktradingengine.account.disruptor.RejectReason;
import com.flab.stocktradingengine.kafka.KafkaTopics;
import com.flab.stocktradingengine.kafka.event.SettlementRequestEvent;

import lombok.RequiredArgsConstructor;

/**
 * 매수 체결이 남긴 미수금을 {@code settlement-requests} 토픽으로 발행하는 {@link AccountResultListener}
 * 구현체(a2-1). 미수금 금액은 여기서 재계산하지 않고 {@link #onUnpaidRecorded}로 받은 값 그대로
 * 흘려보낸다 — {@code AccountState.applyBuyFill}의 반올림 텔레스코핑을 밖에서 재계산하면 어긋난다.
 * settlementRef는 tradeId를 그대로 쓴다(체결 하나당 정산 하나).
 *
 * <p>onUnpaidRecorded 외 콜백은 이 리스너의 관심사가 아니라 no-op이다 — 로깅은
 * {@link LoggingAccountResultListener}가 별도로 담당하고, 두 리스너는
 * {@link CompositeAccountResultListener}로 묶여 함께 호출된다.</p>
 */
@Component
@RequiredArgsConstructor
public class SettlementRequestPublisher implements AccountResultListener {

    private static final String TOPIC = KafkaTopics.settlementRequests();
    private static final int SETTLEMENT_DAYS = 2; // T+2, 영업일 미고려(단순화 — 의도적)

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final Clock clock;

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
        long dueAtEpochMillis = clock.instant().plus(Duration.ofDays(SETTLEMENT_DAYS)).toEpochMilli();
        SettlementRequestEvent event = new SettlementRequestEvent(tradeId, accountId, amount, dueAtEpochMillis);
        kafkaTemplate.send(TOPIC, String.valueOf(accountId), event);
    }
}
