package com.flab.stocktradingengine.trading.matching;

import java.math.BigDecimal;

/**
 * 단일 체결 결과 값 객체 (Value Object).
 *
 * <p>매칭 스레드(OrderBook)가 생성하고, DB 반영 스레드(MatchingFillHandler)가 소비한다.
 * record 로 선언해 불변성을 보장하므로 스레드 간 전달 시 별도 동기화가 필요 없다.</p>
 *
 * <p><b>accountId 를 포함하는 이유</b><br>
 * DB 반영 시 orderId 로 Order 를 조회하면 account 를 한 번 더 JOIN 해야 한다.
 * 매칭 시점에 이미 알고 있는 accountId 를 함께 전달해 불필요한 쿼리를 줄인다.</p>
 */
public record FillResult(
    Long buyOrderId,
    Long buyAccountId,
    Long sellOrderId,
    Long sellAccountId,
    int filledQuantity,
    BigDecimal matchPrice
) {}
