package com.flab.stocktradingengine.account.worker.messaging;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import com.flab.stocktradingengine.account.disruptor.engine.AccountEngine;
import com.flab.stocktradingengine.kafka.event.TradeFilledEvent;

import lombok.RequiredArgsConstructor;

/**
 * {@code account-fills} 토픽을 소비해 {@link AccountEngine}에 체결을 반영한다.
 *
 * <h3>fan-out 수신</h3>
 * <p>발행 쪽은 체결 하나당 매수·매도 계좌 앞으로 같은 {@link TradeFilledEvent}를 accountId 키만
 * 다르게 두 번 보낸다(ADR-018). 이 컨슈머는 어느 키로 온 메시지인지 구분하지 않고 매번 매수·매도
 * 양쪽을 다 {@link AccountEngine}에 전달한다 — 이 워커가 소유하지 않은 쪽은 엔진이 이미
 * {@code ACCOUNT_NOT_FOUND}로 걸러내고(격리), 같은 체결이 중복 도착해도 tradeId 멱등으로
 * 안전하다.</p>
 *
 * <h3>ack 시점 — 저널 기록 뒤에 커밋한다(A안)</h3>
 * <p>{@link AccountEngine#publishBuyFill}/{@link AccountEngine#publishSellFill}는 링버퍼에
 * 넣기만 하는 비동기 발행이라, 발행 직후엔 아직 저널에 durable하게 남지 않았다. 여기서 바로
 * ack하면 저널 기록 전에 Kafka 오프셋이 넘어가고, 그 사이 크래시하면 저널에도 Kafka에도 없어
 * 체결이 유실된다. 그래서 {@link AccountEngine#blockUntilJournaled}로 두 발행이 저널에 기록될
 * 때까지 기다린 뒤에 ack한다. 반대로 기록 뒤·ack 전에 죽으면 Kafka가 재전송하고 tradeId 멱등이
 * 중복을 흡수한다(유실보다 중복이 안전하다).</p>
 */
@Component
@RequiredArgsConstructor
public class AccountFillConsumer {

    private final AccountEngine accountEngine;

    @KafkaListener(topics = "account-fills", groupId = "account-worker")
    public void consume(TradeFilledEvent fill, Acknowledgment ack) {
        long buySequence = accountEngine.publishBuyFill(
            fill.tradeId(), fill.buyOrderId(), fill.buyAccountId(), fill.stockCode(),
            fill.matchPrice(), fill.filledQuantity());
        long sellSequence = accountEngine.publishSellFill(
            fill.tradeId(), fill.sellOrderId(), fill.sellAccountId(), fill.stockCode(),
            fill.filledQuantity());
        accountEngine.blockUntilJournaled(Math.max(buySequence, sellSequence));
        ack.acknowledge();
    }
}
