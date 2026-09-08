package com.flab.stocktradingengine.account.disruptor;

import java.math.BigDecimal;

/**
 * {@link AccountOrderCodec}가 디코딩한 매수·매도 주문 스냅샷. type=BUY일 때만 price가 있고
 * (매도는 담보가 보유 수량이라 price가 없다 — {@code AccountEngine.publishSell} 참고),
 * type=SELL이면 price는 null이다.
 *
 * <p>orderId는 없다(C5-2a) — 인바운드는 requestId만 싣고, orderId는 계좌 워커가 첫 접수 시점에
 * 발급한다({@link AccountState#rememberRequest}).</p>
 */
public record DecodedAccountOrder(
    EventType type, long accountId, String stockCode,
    BigDecimal price, int quantity, String requestId
) {
}
