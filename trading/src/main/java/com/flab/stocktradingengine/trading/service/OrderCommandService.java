package com.flab.stocktradingengine.trading.service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;

import org.springframework.lang.NonNull;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.flab.stocktradingengine.account.entity.Account;
import com.flab.stocktradingengine.account.service.AccountService;
import com.flab.stocktradingengine.trading.command.BuyOrderCommand;
import com.flab.stocktradingengine.trading.command.SellOrderCommand;
import com.flab.stocktradingengine.trading.entity.Order;
import com.flab.stocktradingengine.trading.entity.OrderSide;
import com.flab.stocktradingengine.trading.entity.OrderStatus;
import com.flab.stocktradingengine.trading.entity.OrderType;
import com.flab.stocktradingengine.trading.repository.OrderRepository;
import com.flab.stocktradingengine.trading.view.CancelOrderResultView;
import com.flab.stocktradingengine.trading.view.PlaceOrderResultView;

import lombok.RequiredArgsConstructor;

/**
 * 주문 접수·취소 서비스 (trading 도메인)
 */
@Service
@RequiredArgsConstructor
public class OrderCommandService {

    private final OrderRepository orderRepository;
    private final AccountService accountService;

    /**
     * 매수 주문 접수 (증거금 예약). 계좌 행 락 후 잔고/증거금 검증.
     * @param pendingUnpaidSum 계좌의 미결제 미수금 합계 (호출부에서 조회해 전달).
     */
    @Transactional
    public PlaceOrderResultView placeBuyOrder(@NonNull BuyOrderCommand command, @NonNull Account account,
            @NonNull BigDecimal pendingUnpaidSum) {
        // BUY price * quantity 로 주문 금액 계산
        Account lockedAccount = accountService.getAccountByIdForUpdate(account.getId())
            .orElseThrow(() -> new IllegalArgumentException("Account not found: " + account.getId()));

        // user 가 설정해둔 증거금 마진율 가져오기
        BigDecimal orderAmount = command.price().multiply(BigDecimal.valueOf(command.quantity()));
        BigDecimal marginRate = lockedAccount.getMarginRate();

        // amount * marginRate = reservedMargin (예약 증거금) => 이번 주문에 소요되는 증거금 계산
        BigDecimal reservedMargin = orderAmount.multiply(marginRate).setScale(0, RoundingMode.DOWN);

        // PENDING 상태의 주문 중 매수 주문의 예약 증거금 합계
        BigDecimal currentReservedMarginSum = orderRepository.findByAccount_AccountIdAndStatus(lockedAccount.getAccountId(), OrderStatus.PENDING)
            .stream()
            .filter(o -> o.getSide() == OrderSide.BUY && o.getReservedMargin() != null)
            .map(Order::getReservedMargin)
            .reduce(BigDecimal.ZERO, BigDecimal::add);

        // 출금 가능 금액 = 잔고 - 예약 증거금 - 미결제 미수금
        BigDecimal withdrawableBalance = lockedAccount.getBalance()
            .subtract(currentReservedMarginSum)
            .subtract(pendingUnpaidSum);

    
        // required amount = orderAmount * marginRate == withdrawableBalance (같거나 작아야 함)
        // withdrawableBalance / marginRate = buyLimit 
        BigDecimal buyLimit = withdrawableBalance.divide(marginRate, 0, RoundingMode.DOWN);
        
        // 주문 금액이 매수 가능 금액보다 크면 예외 
        if (orderAmount.compareTo(buyLimit) > 0) {
            throw new IllegalStateException("매수 가능 금액 초과");
        }

        // 증거금이 충분하면 주문 접수
        Order order = Order.builder()
            .account(lockedAccount)
            .stockCode(command.stockCode())
            .side(OrderSide.BUY)
            .orderType(OrderType.valueOf(command.orderType()))
            .price(command.price())
            .quantity(command.quantity())
            .status(OrderStatus.PENDING)
            .orderAt(Instant.now())
            .reservedMargin(reservedMargin)
            .build();
            
        order = orderRepository.save(order);

        return new PlaceOrderResultView(
            order.getOrderId(),
            order.getStatus().name(),
            order.getOrderAt().toEpochMilli(),
            order.getReservedMargin()
        );
    }

    /**
     * 매도 주문 접수. 해당 종목 보유 행 락 후 보유 수량 검증.
     */
    @Transactional
    public PlaceOrderResultView placeSellOrder(@NonNull SellOrderCommand command, @NonNull Account account) {
        var holding = accountService.getHoldingForUpdate(account.getId(), command.stockCode())
            .orElseThrow(() -> new IllegalStateException("보유 종목이 아님: " + command.stockCode()));

        if (holding.getQuantity() < command.quantity()) {
            throw new IllegalStateException("매도 수량 초과 (보유: " + holding.getQuantity() + ", 요청: " + command.quantity() + ")");
        }

        Order order = Order.builder()
            .account(account)
            .stockCode(command.stockCode())
            .side(OrderSide.SELL)
            .orderType(OrderType.valueOf(command.orderType()))
            .price(command.price() != null ? command.price() : BigDecimal.ZERO)
            .quantity(command.quantity())
            .status(OrderStatus.PENDING)
            .orderAt(Instant.now())
            .reservedMargin(null)
            .build();
        order = orderRepository.save(order);

        return new PlaceOrderResultView(
            order.getOrderId(),
            order.getStatus().name(),
            order.getOrderAt().toEpochMilli(),
            null
        );
    }

    /**
     * 주문 취소 (PENDING만 가능, 매수 시 예약 증거금 반환)
     */
    @Transactional
    public CancelOrderResultView cancelOrder(@NonNull Long orderId) {
        Order order = orderRepository.findByOrderId(orderId).orElseThrow(
            () -> new IllegalArgumentException("Order not found: " + orderId));
        if (order.getStatus() != OrderStatus.PENDING) {
            throw new IllegalStateException("취소 가능한 상태가 아님: " + order.getStatus());
        }
        BigDecimal returnedMargin = order.getReservedMargin() != null ? order.getReservedMargin() : BigDecimal.ZERO;
        order.cancel();
        orderRepository.save(order);
        return new CancelOrderResultView(order.getOrderId(), returnedMargin);
    }
}
