package com.flab.stocktradingengine.trading.service;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.flab.stocktradingengine.trading.entity.Order;
import com.flab.stocktradingengine.trading.entity.OrderStatus;
import com.flab.stocktradingengine.trading.repository.OrderRepository;

import lombok.RequiredArgsConstructor;

/**
 * 주문 조회 전용 서비스 (trading 도메인).
 */
@Service
@RequiredArgsConstructor
public class OrderQueryService {

    private final OrderRepository orderRepository;

    /**
     * 주문 단건 조회. 권한 검증·취소 이벤트 발행 등 복수 필드가 필요한 경우 사용.
     */
    public Order getOrder(Long orderId) {
        return findOrder(orderId);
    }

    /**
     * 주문 소유 계좌 ID 조회. 권한 검증용으로 api에서 사용.
     * account 가 Lazy 로딩이므로 트랜잭션 필요.
     */
    @Transactional(readOnly = true)
    public Long getAccountIdByOrderId(Long orderId) {
        return findOrder(orderId).getAccount().getAccountId();
    }

    /**
     * 주문 종목코드 조회. 취소 이벤트 Kafka 발행 시 토픽 결정용.
     */
    public String getStockCodeByOrderId(Long orderId) {
        return findOrder(orderId).getStockCode();
    }

    private Order findOrder(Long orderId) {
        return orderRepository.findByOrderId(orderId)
            .orElseThrow(() -> new IllegalArgumentException("Order not found: " + orderId));
    }

    /**
     * 특정 종목의 PENDING 주문을 접수 시각 오름차순으로 조회.
     * Kafka 파티션 할당 시 해당 종목 OrderBook 복원에 사용.
     */
    public List<Order> getPendingByStockCodeSortedByTime(String stockCode) {
        return orderRepository.findByStatusAndStockCodeOrderByOrderAtAsc(OrderStatus.PENDING, stockCode);
    }

    /**
     * 계좌별 주문 내역 페이징 조회. status가 null이면 전체 상태 조회.
     */
    public Page<Order> getOrdersPaged(long accountId, OrderStatus status, Instant start, Instant end, Pageable pageable) {
        if (status != null) {
            return orderRepository.findByAccount_AccountIdAndStatusAndOrderAtBetweenOrderByOrderAtDesc(
                accountId, status, start, end, pageable);
        }
        return orderRepository.findByAccount_AccountIdAndOrderAtBetweenOrderByOrderAtDesc(
            accountId, start, end, pageable);
    }

    /**
     * 계좌별 체결 내역 페이징 조회.
     */
    public Page<Order> getFilledOrdersPaged(long accountId, Instant start, Instant end) {
        return orderRepository.findByAccount_AccountIdAndStatusAndOrderAtBetweenOrderByOrderAtDesc(
            accountId, OrderStatus.FILLED, start, end, PageRequest.of(0, 20));
    }

    /**
     * 계좌별 매수 주문 증거금 홀딩 합계. 계좌 상세(출금가능 등) 계산용.
     */
    public BigDecimal getReservedMarginSumByAccountId(Long accountId) {
        return orderRepository.sumReservedMarginByAccountId(accountId);
    }
}
