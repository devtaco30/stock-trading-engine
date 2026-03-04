package com.flab.stocktradingengine.trading.repository;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;

import com.flab.stocktradingengine.trading.entity.Order;
import com.flab.stocktradingengine.trading.entity.OrderStatus;

/**
 * 주문 저장소
 */
public interface OrderRepository extends JpaRepository<Order, Long> {

    /**
     * 주문 ID로 주문 조회
     * @param orderId 주문 ID
     * @return 주문
     */
    Optional<Order> findByOrderId(Long orderId);

    List<Order> findByAccount_AccountIdAndStatus(Long accountId, OrderStatus status);

    /** 증거금 홀딩 중인 주문 (PENDING/FILLED 공통, T+2 정산 전까지 출금 불가 반영용) */
    List<Order> findByAccount_AccountIdAndReservedMarginIsNotNull(Long accountId);

    /**
     * 파티션 할당 시 특정 종목의 PENDING 주문을 시간 순으로 조회.
     * 시간 순으로 로드해야 가격·시간 우선순위가 올바르게 복원된다.
     */
    List<Order> findByStatusAndStockCodeOrderByOrderAtAsc(OrderStatus status, String stockCode);

    /**
     * 계좌 ID와 주문 일시 범위로 주문 조회
     */
    Page<Order> findByAccount_AccountIdAndOrderAtBetweenOrderByOrderAtDesc(
        Long accountId,
        Instant startAt,
        Instant endAt,
        Pageable pageable
    );

    Page<Order> findByAccount_AccountIdAndStatusAndOrderAtBetweenOrderByOrderAtDesc(
        Long accountId,
        OrderStatus status,
        Instant startAt,
        Instant endAt,
        Pageable pageable
    );
}
