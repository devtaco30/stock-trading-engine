package com.flab.stocktradingengine.trading.repository;

import java.time.Instant;
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
    Optional<Order> findByOrderId(String orderId);

    /**
     * 계좌 ID와 주문 일시 범위로 주문 조회
     * @param accountId
     * @param startAt
     * @param endAt
     * @param pageable
     * @return
     */
    Page<Order> findByAccount_IdAndOrderAtBetweenOrderByOrderAtDesc(
        Long accountId,
        Instant startAt,
        Instant endAt,
        Pageable pageable
    );

    Page<Order> findByAccount_IdAndStatusAndOrderAtBetweenOrderByOrderAtDesc(
        Long accountId,
        OrderStatus status,
        Instant startAt,
        Instant endAt,
        Pageable pageable
    );
}
