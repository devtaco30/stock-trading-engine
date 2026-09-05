package com.flab.stocktradingengine.trading.service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.function.Supplier;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.flab.stocktradingengine.account.entity.Account;
import com.flab.stocktradingengine.account.entity.Holding;
import com.flab.stocktradingengine.account.exception.InsufficientResourceException;
import com.flab.stocktradingengine.account.service.AccountService;
import com.flab.stocktradingengine.exception.InvalidRequestException;
import com.flab.stocktradingengine.exception.ResourceNotFoundException;
import com.flab.stocktradingengine.trading.command.BuyOrderCommand;
import com.flab.stocktradingengine.trading.command.SellOrderCommand;
import com.flab.stocktradingengine.trading.entity.Order;
import com.flab.stocktradingengine.trading.entity.OrderSide;
import com.flab.stocktradingengine.trading.entity.OrderStatus;
import com.flab.stocktradingengine.trading.entity.OrderType;
import com.flab.stocktradingengine.trading.repository.OrderRepository;
import com.flab.stocktradingengine.trading.view.PlaceOrderResultView;

import lombok.RequiredArgsConstructor;

/**
 * 주문 저장(쓰기) 담당. 계좌·보유 행 락 후 잔고/보유 검증을 거쳐 주문을 저장한다.
 *
 * <p>멱등 판별·중복 복구는 {@link OrderCommandService} 가 담당한다.
 * 이 빈은 실제 락·검증·저장이라는 하나의 트랜잭션 단위만 책임진다.
 * {@code saveAndFlush} 로 UNIQUE 위반을 즉시 노출해 상위에서 경쟁 상태를 처리할 수 있게 한다.</p>
 */
@Service
@RequiredArgsConstructor
public class OrderWriter {

    private final OrderRepository orderRepository;
    private final AccountService accountService;

    /**
     * 매수 주문 저장 (증거금 예약). 계좌 행 락 후 잔고/증거금 검증.
     * @param unpaidSumSupplier 계좌의 미결제 미수금 합계 공급자. 락 획득 후 호출돼 TOCTOU를 방지한다.
     */
    @Transactional
    public PlaceOrderResultView writeBuyOrder(BuyOrderCommand command,
            Supplier<BigDecimal> unpaidSumSupplier) {
        // BUY price * quantity 로 주문 금액 계산 -> Pessimistic Lock 으로 인해 락
        Account lockedAccount = accountService.getAccountByAccountIdForUpdate(command.accountId())
            .orElseThrow(() -> new ResourceNotFoundException("Account not found: " + command.accountId()));

        // 락 획득 후 미결제 미수금 조회 — 락 전 조회 시 TOCTOU 발생
        BigDecimal pendingUnpaidSum = unpaidSumSupplier.get();

        // user 가 설정해둔 증거금 마진율 가져오기
        BigDecimal orderAmount = command.price().multiply(BigDecimal.valueOf(command.quantity()));
        BigDecimal marginRate = lockedAccount.getMarginRate();

        // amount * marginRate = reservedMargin (예약 증거금) => 이번 주문에 소요되는 증거금 계산
        BigDecimal reservedMargin = orderAmount.multiply(marginRate).setScale(0, RoundingMode.DOWN);

        // PENDING 매수 주문의 예약증거금 합계 — DB SUM으로 락 보유 시간 단축
        BigDecimal currentReservedMarginSum = orderRepository.sumReservedMarginByAccountId(lockedAccount.getAccountId());

        // 출금 가능 금액 = 잔고 - 예약 증거금 - 미결제 미수금
        BigDecimal withdrawableBalance = lockedAccount.getBalance()
            .subtract(currentReservedMarginSum)
            .subtract(pendingUnpaidSum);

        // required amount = orderAmount * marginRate == withdrawableBalance (같거나 작아야 함)
        // withdrawableBalance / marginRate = buyLimit
        BigDecimal buyLimit = withdrawableBalance.divide(marginRate, 0, RoundingMode.DOWN);

        // 주문 금액이 매수 가능 금액보다 크면 예외
        if (orderAmount.compareTo(buyLimit) > 0) {
            throw new InsufficientResourceException("매수 가능 금액 초과");
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
            .requestedAt(command.requestedAt())
            .requestId(command.requestId())
            .build();

        Order saved = orderRepository.saveAndFlush(order);
        return toView(saved);
    }

    /**
     * 매도 주문 저장. 해당 종목 보유 행 락 후 보유 수량 검증.
     */
    @Transactional
    public PlaceOrderResultView writeSellOrder(SellOrderCommand command) {
        // 보유 종목 검증
        Holding holding = accountService.getHoldingByAccountIdForUpdate(command.accountId(), command.stockCode())
            .orElseThrow(() -> new InvalidRequestException("보유 종목이 아님: " + command.stockCode()));

        // 보유 수량 검증
        if (holding.getQuantity() < command.quantity()) {
            throw new InsufficientResourceException("매도 수량 초과 (보유: " + holding.getQuantity() + ", 요청: " + command.quantity() + ")");
        }

        Account account = holding.getAccount();
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
            .requestedAt(command.requestedAt())
            .requestId(command.requestId())
            .build();

        Order saved = orderRepository.saveAndFlush(order);
        return toView(saved);
    }

    private PlaceOrderResultView toView(Order order) {
        return new PlaceOrderResultView(
            order.getOrderId(),
            order.getStatus().name(),
            order.getOrderAt().toEpochMilli(),
            order.getReservedMargin());
    }
}
