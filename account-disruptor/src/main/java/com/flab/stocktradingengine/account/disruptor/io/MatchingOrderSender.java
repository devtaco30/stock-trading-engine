package com.flab.stocktradingengine.account.disruptor.io;

import java.math.BigDecimal;

import com.flab.stocktradingengine.account.disruptor.engine.AccountEngine;
import com.flab.stocktradingengine.account.disruptor.handler.AccountEventHandler;
import com.flab.stocktradingengine.trading.entity.OrderSide;

/**
 * 계좌가 accept한 매수·매도 주문을 매칭 엔진으로 넘기는 발신 포트(②-b).
 *
 * <p>{@link AccountEventHandler}가 accept 순간(onAccepted·onSellAccepted 뒤)에만 호출한다 —
 * 거부·중복·invalid는 매칭이 알 필요가 없으니 호출하지 않는다. orderAt(주문 시각)은 파라미터로
 * 받지 않는다 — 코어는 시간을 모르므로({@link AccountEngine} 어디에도 Clock이 없다) 호스트
 * 구현체(예: {@code AeronMatchingOrderSender})가 호출 시점에 직접 찍는다.</p>
 *
 * <h3>구현 계약 — best-effort·non-blocking·예외 던지지 않음</h3>
 * <p>이 메서드는 계좌 엔진의 단일 상시 컨슈머 스레드 안에서 동기 호출된다(fire-and-forget). 구현체는
 * 절대 블로킹하거나(재시도 루프 등) 예외를 던지면 안 된다 — 던지면 fail-fast 정책이 계좌 엔진 전체를
 * 죽인다. 발신 실패는 매칭에 이 주문이 전달되지 않았다는 뜻일 뿐, 계좌 예약은 그대로 유효하다.
 * 보장 전달·내구성은 이 포트의 책임이 아니다(이후 단위).</p>
 */
public interface MatchingOrderSender {

    /** 매수·매도 접수 하나를 매칭으로 전달한다. side로 매수·매도를 구분한다. */
    void forwardPlace(long orderId, long accountId, String stockCode, OrderSide side, BigDecimal price, int quantity);
}
