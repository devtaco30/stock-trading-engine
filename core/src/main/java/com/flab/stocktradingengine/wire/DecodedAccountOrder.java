package com.flab.stocktradingengine.wire;

import java.math.BigDecimal;

import com.flab.stocktradingengine.trading.entity.OrderSide;

/**
 * {@link AccountOrderCodec}가 디코딩한 매수·매도 주문 스냅샷. type=BUY일 때만 price가 있고
 * (매도는 담보가 보유 수량이라 price가 없다 — {@code AccountEngine.publishSell} 참고),
 * type=SELL이면 price는 null이다.
 *
 * <p>orderId는 없다(C5-2a) — 인바운드는 requestId만 싣고, orderId는 계좌 워커가 첫 접수 시점에
 * 발급한다. type은 계좌 엔진 내부 도메인({@code account.disruptor.EventType} — BUY_FILL·SETTLEMENT
 * 등 체결·정산까지 포함)이 아니라 {@link OrderSide}(BUY/SELL)를 쓴다 — 인테이크 와이어는 매수·매도
 * 접수만 실어보내고, 체결·정산은 별도 경로(Kafka)로 온다. account.disruptor.EventType은 계좌 엔진의
 * 내부 명령 종류라 공유 계약인 core로 옮기지 않는다.</p>
 */
public record DecodedAccountOrder(
    OrderSide type, long accountId, String stockCode,
    BigDecimal price, int quantity, String requestId
) {
}
