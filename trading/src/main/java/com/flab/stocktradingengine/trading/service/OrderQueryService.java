package com.flab.stocktradingengine.trading.service;

import java.math.BigDecimal;

import org.springframework.lang.NonNull;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.flab.stocktradingengine.trading.entity.Order;
import com.flab.stocktradingengine.trading.entity.OrderSide;
import com.flab.stocktradingengine.trading.repository.OrderRepository;

import lombok.RequiredArgsConstructor;

/**
 * 주문 조회 전용 서비스 (trading 도메인).
 */
@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class OrderQueryService {

    private final OrderRepository orderRepository;

    /**
     * 계좌별 매수 주문 증거금 홀딩 합계. 계좌 상세(출금가능 등) 계산용.
     */
    public BigDecimal getReservedMarginSumByAccountId(@NonNull Long accountId) {
        return orderRepository.findByAccount_AccountIdAndReservedMarginIsNotNull(accountId)
            .stream()
            .filter(o -> o.getSide() == OrderSide.BUY)
            .map(Order::getReservedMargin)
            .reduce(BigDecimal.ZERO, BigDecimal::add);
    }
}
