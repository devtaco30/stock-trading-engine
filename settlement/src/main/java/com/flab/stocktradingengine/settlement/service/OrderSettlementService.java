package com.flab.stocktradingengine.settlement.service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.flab.stocktradingengine.account.service.AccountService;
import com.flab.stocktradingengine.support.SnowflakeIdGenerator;
import com.flab.stocktradingengine.settlement.entity.Unpaid;
import com.flab.stocktradingengine.settlement.repository.UnpaidRepository;
import com.flab.stocktradingengine.trading.entity.Order;
import com.flab.stocktradingengine.trading.entity.OrderSide;
import com.flab.stocktradingengine.trading.entity.OrderStatus;
import com.flab.stocktradingengine.trading.repository.OrderRepository;

import lombok.extern.slf4j.Slf4j;

import lombok.RequiredArgsConstructor;

/**
 * 체결 시 주문 상태 반영·보유 갱신·미결제 생성 (settlement 도메인 오케스트레이션)
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class OrderSettlementService {

    private final AccountService accountService;
    private final OrderRepository orderRepository;
    private final UnpaidRepository unpaidRepository;
    private final SnowflakeIdGenerator snowflakeIdGenerator;

    /**
     * 매수 주문 체결: 주문 FILLED 처리, 보유 추가, 미결제 생성.
     * PENDING·BUY가 아니면 무시.
     */
    @Transactional
    public void fillOrder(Long orderId) {
        Order order = orderRepository.findByOrderId(orderId)
            .orElseThrow(() -> new IllegalArgumentException("Order not found: " + orderId));
        
        // PENDING·BUY가 아니면 무시
        if (order.getStatus() != OrderStatus.PENDING || order.getSide() != OrderSide.BUY) {
            return;
        }

        Instant now = Instant.now();
        order.fill(now);
        orderRepository.save(order);

        accountService.addHoldingOrIncreaseQuantity(
            order.getAccount().getAccountId(),
            order.getStockCode(),
            order.getQuantity(),
            order.getPrice()
        );

        BigDecimal marginRate = order.getAccount().getMarginRate();
        BigDecimal unpaidAmount = order.getPrice().multiply(BigDecimal.valueOf(order.getQuantity()))
            .multiply(BigDecimal.ONE.subtract(marginRate)).setScale(0, RoundingMode.DOWN);
        LocalDate settlementDate = LocalDate.ofInstant(now, ZoneId.systemDefault()).plusDays(2);
        String unpaidId = String.valueOf(snowflakeIdGenerator.nextId());
        unpaidRepository.save(new Unpaid(unpaidId, order.getAccount(), order, unpaidAmount, settlementDate));
    }

    /**
     * 매수 주문 부분 체결 반영.
     * <ul>
     *   <li>보유 수량·평균가 합산</li>
     *   <li>미결제(Unpaid) 생성 (체결 금액 × (1 - 증거금률))</li>
     *   <li>전량 체결 시 주문 상태 FILLED 전환</li>
     * </ul>
     */
    @Transactional
    public void fillBuyOrderPartially(Long orderId, int fillQty, BigDecimal matchPrice) {
        Order order = orderRepository.findByOrderId(orderId)
            .orElseThrow(() -> new IllegalArgumentException("Order not found: " + orderId));

        if (order.getSide() != OrderSide.BUY || order.getStatus() == OrderStatus.CANCELLED) {
            log.warn("[매수 부분 체결 무시] orderId={} side={} status={}", orderId, order.getSide(), order.getStatus());
            return;
        }

        Instant now = Instant.now();
        order.fillPartially(fillQty, now);
        orderRepository.save(order);

        Long accountId = order.getAccount().getAccountId();
        accountService.addHoldingOrIncreaseQuantity(accountId, order.getStockCode(), fillQty, matchPrice);

        BigDecimal marginRate = order.getAccount().getMarginRate();
        BigDecimal unpaidAmount = matchPrice.multiply(BigDecimal.valueOf(fillQty))
            .multiply(BigDecimal.ONE.subtract(marginRate)).setScale(0, RoundingMode.DOWN);
        LocalDate settlementDate = LocalDate.ofInstant(now, ZoneId.systemDefault()).plusDays(2);
        unpaidRepository.save(new Unpaid(
            String.valueOf(snowflakeIdGenerator.nextId()),
            order.getAccount(), order, unpaidAmount, settlementDate
        ));
    }

    /**
     * 매도 주문 부분 체결 반영.
     * <ul>
     *   <li>보유 수량 차감</li>
     *   <li>전량 체결 시 주문 상태 FILLED 전환</li>
     * </ul>
     */
    @Transactional
    public void fillSellOrderPartially(Long orderId, int fillQty) {
        Order order = orderRepository.findByOrderId(orderId)
            .orElseThrow(() -> new IllegalArgumentException("Order not found: " + orderId));

        if (order.getSide() != OrderSide.SELL || order.getStatus() == OrderStatus.CANCELLED) {
            log.warn("[매도 부분 체결 무시] orderId={} side={} status={}", orderId, order.getSide(), order.getStatus());
            return;
        }

        Instant now = Instant.now();
        order.fillPartially(fillQty, now);
        orderRepository.save(order);

        accountService.decreaseHolding(order.getAccount().getAccountId(), order.getStockCode(), fillQty);
    }
}
