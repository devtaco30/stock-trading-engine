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

    /**
     * 매수·매도 접수 하나를 매칭으로 전달한다. side로 매수·매도를 구분한다.
     *
     * <h3>구현 계약(I2 발신측 D1~D3 이후) — 버리지 않는다, 그래서 블로킹·예외를 던질 수 있다</h3>
     * <p>이 메서드는 계좌 엔진의 단일 상시 컨슈머 스레드 안에서 동기 호출된다. 매칭으로 보내지
     * 못한 주문은 계좌 예약은 그대로인데 매칭 장부엔 없어서 체결·취소가 영영 안 온다 — 그래서
     * {@code AeronMatchingOrderSender}는 더 이상 best-effort가 아니다: 발신 큐가 가득 차면
     * 자리가 날 때까지 이 스레드에서 대기하고(그동안 이 샤드의 다른 계좌 접수도 같이 멈춘다),
     * 발신 스트림이 복구 불가 상태(CLOSED 등)면 이 호출에서 예외를 던져
     * {@code AccountExceptionHandler}가 계좌 엔진 전체를 fail-fast로 멈추게 한다. 매칭 전달
     * 자체를 검증하지 않는 테스트(계좌 단독 복구·저널 등)는 진짜 구현 대신 아무것도 안 하는
     * 테스트 더블({@code NoOpMatchingOrderSender})을 쓴다.</p>
     */
    void forwardPlace(long orderId, long accountId, String stockCode, OrderSide side, BigDecimal price, int quantity);
}
