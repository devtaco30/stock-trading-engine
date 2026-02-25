package com.flab.stocktradingengine.service;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.flab.stocktradingengine.account.entity.Account;
import com.flab.stocktradingengine.dto.common.PagedResponse;
import com.flab.stocktradingengine.dto.order.BuyOrderRequest;
import com.flab.stocktradingengine.dto.order.CancelOrderResponse;
import com.flab.stocktradingengine.dto.order.OrderDto;
import com.flab.stocktradingengine.dto.order.OrderResponse;
import com.flab.stocktradingengine.dto.order.SellOrderRequest;
import com.flab.stocktradingengine.dto.trade.TradeDto;
import com.flab.stocktradingengine.facade.AccountReadFacade;
import com.flab.stocktradingengine.market.view.StockInfo;
import com.flab.stocktradingengine.resolver.AccountAccessResolver;
import com.flab.stocktradingengine.settlement.service.OrderSettlementService;
import com.flab.stocktradingengine.trading.command.BuyOrderCommand;
import com.flab.stocktradingengine.trading.command.SellOrderCommand;
import org.springframework.data.domain.Page;

import com.flab.stocktradingengine.trading.entity.Order;
import com.flab.stocktradingengine.trading.entity.OrderStatus;
import com.flab.stocktradingengine.trading.repository.OrderRepository;
import com.flab.stocktradingengine.trading.service.OrderCommandService;

import lombok.RequiredArgsConstructor;

/**
 * 주문 API 서비스.
 * <p>계좌 소유·ACTIVE 검증만 수행하고, 주문 접수/취소/체결은 각 도메인 모듈에 위임.</p>
 */
@Service
@RequiredArgsConstructor
public class OrderApiService {

    private final AccountAccessResolver accountAccessResolver;
    private final OrderCommandService orderCommandService;
    private final OrderSettlementService orderSettlementService;
    private final OrderRepository orderRepository;
    private final AccountReadFacade accountReadFacade;

    /**
     * 매수 주문 접수. 계좌 소유·ACTIVE 검증 후 trading 위임.
     */
    @Transactional
    public OrderResponse placeBuyOrder(Long userId, BuyOrderRequest request) {
        Account account = accountAccessResolver.resolveAccountOwnedAndActive(userId, request.accountId());
        BigDecimal pendingUnpaidSum = accountReadFacade.getAccountDetail(request.accountId()).unpaidSum();
        BuyOrderCommand command = new BuyOrderCommand(
            request.accountId(),
            request.stockCode(),
            request.orderType(),
            request.price(),
            request.quantity()
        );
        return OrderResponse.from(orderCommandService.placeBuyOrder(command, account, pendingUnpaidSum));
    }

    /**
     * 매도 주문 접수. 계좌 소유·ACTIVE 검증 후 trading 위임.
     */
    @Transactional
    public OrderResponse placeSellOrder(Long userId, SellOrderRequest request) {
        Account account = accountAccessResolver.resolveAccountOwnedAndActive(userId, request.accountId());
        SellOrderCommand command = new SellOrderCommand(
            request.accountId(),
            request.stockCode(),
            request.orderType(),
            request.price(),
            request.quantity()
        );
        return OrderResponse.from(orderCommandService.placeSellOrder(command, account));
    }

    /**
     * 주문 취소. 주문 소유 계좌 검증 후 trading에 위임.
     */
    @Transactional
    public CancelOrderResponse cancelOrder(Long userId, Long orderId) {
        Long accountId = orderCommandService.getAccountIdByOrderId(orderId);
        accountAccessResolver.resolveAccountOwnedAndActive(userId, accountId);
        return CancelOrderResponse.from(orderCommandService.cancelOrder(orderId));
    }

    /**
     * 매수 주문 체결. 주문 소유 계좌 검증 후 settlement에 위임.
     */
    @Transactional
    public void fillOrder(Long userId, Long orderId) {
        Long accountId = orderCommandService.getAccountIdByOrderId(orderId);
        accountAccessResolver.resolveAccountOwnedAndActive(userId, accountId);
        orderSettlementService.fillOrder(orderId);
    }

    private static final int DEFAULT_ORDER_PAGE_SIZE = 20;

    /**
     * 주문 내역 조회. accountId 기준, 기간·상태 필터.
     */
    @Transactional(readOnly = true)
    public PagedResponse<OrderDto> getOrdersPaged(Long userId, String accountId, String status, Long startAt, Long endAt) {
        long aid = Long.parseLong(accountId);
        accountAccessResolver.resolveAccountOwnedBy(userId, aid);
        var page = findOrderPage(aid, status, startAt, endAt);
        return PagedResponse.of(toOrderDtos(page), page.getNumber(), page.getSize(), page.getTotalElements());
    }

    /**
     * 체결 내역 조회. Trade = 체결(FILLED) 1건.
     */
    @Transactional(readOnly = true)
    public PagedResponse<TradeDto> getTradesPaged(Long userId, String accountId, Long startAt, Long endAt) {
        long aid = Long.parseLong(accountId);
        accountAccessResolver.resolveAccountOwnedBy(userId, aid);
        var page = findFilledOrderPage(aid, startAt, endAt);
        return PagedResponse.of(toTradeDtos(page), page.getNumber(), page.getSize(), page.getTotalElements());
    }

    private Page<Order> findOrderPage(long accountId, String status, Long startAt, Long endAt) {
        Instant start = startAt != null ? Instant.ofEpochMilli(startAt) : Instant.EPOCH;
        Instant end = endAt != null ? Instant.ofEpochMilli(endAt) : Instant.now().plusSeconds(86400);
        Pageable pageable = PageRequest.of(0, DEFAULT_ORDER_PAGE_SIZE);
        return Optional.ofNullable(status)
            .filter(s -> !s.isBlank())
            .map(s -> OrderStatus.valueOf(s.toUpperCase()))
            .map(st -> orderRepository.findByAccount_AccountIdAndStatusAndOrderAtBetweenOrderByOrderAtDesc(accountId, st, start, end, pageable))
            .orElseGet(() -> orderRepository.findByAccount_AccountIdAndOrderAtBetweenOrderByOrderAtDesc(accountId, start, end, pageable));
    }

    private Page<Order> findFilledOrderPage(long accountId, Long startAt, Long endAt) {
        Instant start = startAt != null ? Instant.ofEpochMilli(startAt) : Instant.EPOCH;
        Instant end = endAt != null ? Instant.ofEpochMilli(endAt) : Instant.now().plusSeconds(86400);
        return orderRepository.findByAccount_AccountIdAndStatusAndOrderAtBetweenOrderByOrderAtDesc(
            accountId, OrderStatus.FILLED, start, end, PageRequest.of(0, DEFAULT_ORDER_PAGE_SIZE));
    }

    private List<OrderDto> toOrderDtos(Page<Order> page) {
        List<Order> content = page.getContent();
        Map<String, StockInfo> stockInfoMap = content.isEmpty() ? Map.of()
            : accountReadFacade.getStockInfoBatch(content.stream().map(Order::getStockCode).distinct().toList());
        return content.stream()
            .map(o -> toOrderDto(o, stockInfoMap.getOrDefault(o.getStockCode(), new StockInfo("", BigDecimal.ZERO))))
            .toList();
    }

    private List<TradeDto> toTradeDtos(Page<Order> page) {
        List<Order> content = page.getContent();
        Map<String, StockInfo> stockInfoMap = content.isEmpty() ? Map.of()
            : accountReadFacade.getStockInfoBatch(content.stream().map(Order::getStockCode).distinct().toList());
        return content.stream()
            .map(o -> toTradeDto(o, stockInfoMap.getOrDefault(o.getStockCode(), new StockInfo("", BigDecimal.ZERO))))
            .toList();
    }

    private static OrderDto toOrderDto(Order o, StockInfo info) {
        return OrderDto.builder()
            .orderId(String.valueOf(o.getOrderId()))
            .stockCode(o.getStockCode())
            .stockName(info.stockName())
            .side(o.getSide().name())
            .orderType(o.getOrderType().name())
            .price(o.getPrice())
            .quantity(o.getQuantity())
            .status(o.getStatus().name())
            .orderAt(o.getOrderAt() != null ? o.getOrderAt().toEpochMilli() : null)
            .filledAt(o.getFilledAt() != null ? o.getFilledAt().toEpochMilli() : null)
            .build();
    }

    private static TradeDto toTradeDto(Order o, StockInfo info) {
        BigDecimal amount = o.getPrice().multiply(BigDecimal.valueOf(o.getQuantity()));
        return TradeDto.builder()
            .filledAt(o.getFilledAt() != null ? o.getFilledAt().toEpochMilli() : o.getOrderAt().toEpochMilli())
            .stockCode(o.getStockCode())
            .stockName(info.stockName())
            .side(o.getSide().name())
            .quantity(o.getQuantity())
            .executionPrice(o.getPrice())
            .amount(amount)
            .build();
    }
}
