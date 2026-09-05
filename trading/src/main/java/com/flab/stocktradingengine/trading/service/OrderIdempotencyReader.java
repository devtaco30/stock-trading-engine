package com.flab.stocktradingengine.trading.service;

import java.util.Optional;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.flab.stocktradingengine.trading.entity.Order;
import com.flab.stocktradingengine.trading.repository.OrderRepository;

import lombok.RequiredArgsConstructor;

/**
 * 멱등키(requestId)로 기존 주문을 조회하는 전용 빈.
 *
 * <p>반드시 별도 트랜잭션(REQUIRES_NEW)으로 커밋된 데이터만 읽는다.
 * 저장 실패로 rollback-only 로 오염된 트랜잭션을 재사용하면 Hibernate AssertionFailure 가 나므로,
 * 복구 조회는 이 빈을 통해 새 트랜잭션에서 수행한다.</p>
 */
@Component
@RequiredArgsConstructor
public class OrderIdempotencyReader {

    private final OrderRepository orderRepository;

    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public Optional<Order> findByRequestId(String requestId) {
        return orderRepository.findByRequestId(requestId);
    }
}
