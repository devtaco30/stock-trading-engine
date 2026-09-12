package com.flab.stocktradingengine.matching.worker;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import com.flab.stocktradingengine.kafka.KafkaTopics;
import com.flab.stocktradingengine.kafka.event.TradeFilledEvent;
import com.flab.stocktradingengine.matching.disruptor.MatchListener;
import com.flab.stocktradingengine.support.SnowflakeIdGenerator;
import com.flab.stocktradingengine.trading.matching.FillResult;

import lombok.RequiredArgsConstructor;

/**
 * 매칭 코어의 체결을 {@code account-fills} 토픽으로 발행하는 글루(MatchListener 구현체).
 *
 * <h3>fan-out</h3>
 * <p>체결 하나를 매수 계좌·매도 계좌 앞으로 각각 accountId 키로 발행한다(ADR-018). 두 메시지는
 * 같은 {@link TradeFilledEvent}(같은 tradeId)다 — account-worker 쪽이 tradeId 멱등으로 중복을
 * 걸러내는 전제라, 여기서 tradeId를 다르게 발급하면 매수·매도 반영이 갈린다.</p>
 *
 * <p>{@code stockCode}·{@code tradeId}는 {@link FillResult}에 없다. stockCode는
 * {@link #onFill}의 인자로 받고, tradeId는 여기서 체결 1건당 한 번만 발급한다.</p>
 *
 * <h3>TODO — 전달 보장</h3>
 * <p>지금은 acks=all·retries만으로 발행한다. Kafka produce가 재시도까지 다 실패하면 체결이
 * 유실될 수 있다 — 매칭 쪽에 파일 저널(C6)이 붙기 전까지는 이 경로가 유일한 전달 보장 수단이
 * 아니다(ADR-019).</p>
 *
 * <h3>TODO — 핫패스에 Kafka 호출</h3>
 * <p>{@link #onFill}은 매칭 단일 소비자 스레드에서 직접 불린다. 지금은 그 스레드가 Kafka
 * produce(ms급)까지 동기로 기다린다 — 나중에 출력 전용 링(LMAX output disruptor)으로 분리해
 * 매칭 스레드를 이 지연에서 떼어낼 수 있다.</p>
 */
@Component
@RequiredArgsConstructor
public class AccountFillPublisher implements MatchListener {

    private static final String TOPIC = KafkaTopics.accountFills();

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final SnowflakeIdGenerator snowflakeIdGenerator;

    @Override
    public void onFill(String stockCode, FillResult fill) {
        long tradeId = snowflakeIdGenerator.nextId();
        TradeFilledEvent event = new TradeFilledEvent(
            tradeId, stockCode, fill.buyOrderId(), fill.buyAccountId(),
            fill.sellOrderId(), fill.sellAccountId(), fill.filledQuantity(), fill.matchPrice());

        kafkaTemplate.send(TOPIC, String.valueOf(fill.buyAccountId()), event);
        kafkaTemplate.send(TOPIC, String.valueOf(fill.sellAccountId()), event);
    }
}
