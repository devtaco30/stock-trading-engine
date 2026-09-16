package com.flab.stocktradingengine.codec;

import java.math.BigDecimal;

import com.flab.stocktradingengine.trading.entity.OrderSide;

/**
 * {@link AccountOrderCodec}가 디코딩한 매수·매도 주문 스냅샷. price는 매수·매도 둘 다 있다(②-a) —
 * 매칭이 지정가 엔진이라 호가창 가격레벨에 필요해서다. 계좌 예약(트리거)은 매도 price를 안 쓰지만,
 * 매칭으로 전달할 값이라 인바운드 와이어에는 항상 싣는다.
 *
 * <p>orderId는 없다(C5-2a) — 인바운드는 requestId만 싣고, orderId는 계좌 워커가 첫 접수 시점에
 * 발급한다. type은 계좌 엔진 내부 도메인({@code account.disruptor.EventType} — BUY_FILL·SETTLEMENT
 * 등 체결·정산까지 포함)이 아니라 {@link OrderSide}(BUY/SELL)를 쓴다 — 인테이크 와이어는 매수·매도
 * 접수만 실어보내고, 체결·정산은 별도 경로(Kafka)로 온다. account.disruptor.EventType은 계좌 엔진의
 * 내부 명령 종류라 공유 계약인 core로 옮기지 않는다.</p>
 *
 * <p>{@code publishedAtEpochNanos}는 v1/v2 전 과정 측정(decision_records/
 * v1-v2-e2e-measurement.md)의 끝점①(접수·예약) 지연 계산용이다. 발신자(API 게이트웨이)가
 * 발행 직전 {@link com.flab.stocktradingengine.time.EpochNanos#now()}로 찍는다 — 측정
 * 플래그와 무관하게 항상 채운다(측정 훅 자체는 소비자 쪽에서 게이트한다).</p>
 */
public record DecodedAccountOrder(
    OrderSide type, long accountId, String stockCode,
    BigDecimal price, int quantity, String requestId, long publishedAtEpochNanos
) {
}
