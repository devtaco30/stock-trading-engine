package com.flab.stocktradingengine.trading.matching;

import java.math.BigDecimal;
import java.time.Instant;

import com.flab.stocktradingengine.trading.entity.OrderSide;

import lombok.Getter;

/**
 * 매칭 엔진 전용 인메모리 주문 표현 (POJO).
 *
 * <p><b>왜 JPA 엔티티(Order)를 직접 쓰지 않는가?</b><br>
 * 매칭 스레드는 DB 세션 밖에서 동작한다. JPA 엔티티를 들고 다니면
 * 지연 로딩(LazyInitializationException)이나 detached 상태 오류가 발생한다.
 * 이 객체는 매칭 판단에 필요한 최소 정보만 담고, DB 반영은
 * {@link FillResult} → {@code MatchingFillHandler} 경로로 위임한다.</p>
 *
 * <p><b>패턴: Value Object + Mutable State 분리</b><br>
 * orderId·price·quantity 등 식별/가격 정보는 final 불변,
 * filledQuantity·cancelled 등 상태 정보만 가변으로 설계했다.
 * 매칭 스레드만 이 객체를 접근하므로 별도 동기화는 필요 없다.</p>
 */
@Getter
public class OrderEntry {

    private final Long orderId;
    private final Long accountId;   // Snowflake 계좌 ID — FillResult 에 담아 DB 반영 시 사용
    private final String stockCode;
    private final OrderSide side;
    private final BigDecimal price;
    private final int quantity;
    private final Instant orderAt;  // 동일 가격 레벨 내 시간 우선순위 기준

    private int filledQuantity;
    private boolean cancelled;

    public OrderEntry(Long orderId, Long accountId, String stockCode,
                      OrderSide side, BigDecimal price, int quantity, Instant orderAt) {
        this.orderId = orderId;
        this.accountId = accountId;
        this.stockCode = stockCode;
        this.side = side;
        this.price = price;
        this.quantity = quantity;
        this.orderAt = orderAt;
        this.filledQuantity = 0;
        this.cancelled = false;
    }

    /** 미체결 잔량. 매칭 루프에서 체결 가능 여부와 체결량 계산에 사용. */
    public int getRemainingQuantity() {
        return quantity - filledQuantity;
    }

    /**
     * 인메모리 체결 수량 반영.
     * 매칭 스레드만 호출하므로 동기화 불필요.
     * 실제 DB 반영은 이후 {@link FillResult} 를 통해 비동기 처리된다.
     */
    public void addFilled(int qty) {
        this.filledQuantity += qty;
    }

    /**
     * 취소 마킹. OrderBook 의 Lazy Removal 전략과 함께 동작.
     * 즉시 큐에서 제거하지 않고 마킹만 해두면,
     * 매칭 루프에서 해당 엔트리를 건너뛸 때 큐 앞에서 일괄 정리된다.
     */
    public void cancel() {
        this.cancelled = true;
    }

    public boolean isFullyFilled() {
        return filledQuantity >= quantity;
    }
}
