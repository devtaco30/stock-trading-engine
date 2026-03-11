package com.flab.stocktradingengine.service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.redis.core.StringRedisTemplate;
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
import com.flab.stocktradingengine.market.service.QuoteService;
import com.flab.stocktradingengine.trading.kafka.event.OrderCancelledEvent;
import com.flab.stocktradingengine.trading.kafka.event.OrderPlacedEvent;
import com.flab.stocktradingengine.trading.entity.OrderSide;
import com.flab.stocktradingengine.market.view.QuoteView;
import com.flab.stocktradingengine.market.view.StockInfo;
import com.flab.stocktradingengine.resolver.AccountAccessResolver;
import com.flab.stocktradingengine.trading.command.BuyOrderCommand;
import com.flab.stocktradingengine.trading.command.SellOrderCommand;
import com.flab.stocktradingengine.trading.entity.Order;
import com.flab.stocktradingengine.trading.entity.OrderStatus;
import com.flab.stocktradingengine.trading.redis.RedisKeys;
import com.flab.stocktradingengine.trading.service.OrderCommandService;
import com.flab.stocktradingengine.trading.service.OrderQueryService;
import com.flab.stocktradingengine.trading.view.PlaceOrderResultView;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * 주문 API 서비스.
 *
 * <h3>가격 제한폭 검증</h3>
 * <p>
 * 주문 접수 시 LTP(최근 체결가) 기준 ±30% 범위를 벗어나면 거부한다.
 * LTP가 없으면 전일 종가(previousClose)를 fallback으로 사용한다.
 * 둘 다 없으면 존재하지 않는 종목으로 간주해 예외를 던진다.
 * </p>
 *
 * <h3>Kafka 발행 — ApplicationEvent 방식</h3>
 * <p>
 * 주문 접수/취소 후 ApplicationEventPublisher 로 이벤트를 발행한다.
 * OrderKafkaEventListener 가 @TransactionalEventListener(AFTER_COMMIT) 으로 수신해
 * DB 커밋 완료 후에만 Kafka 로 전달한다. OrderApiService 는 Kafka 존재를 모른다.
 * </p>
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class OrderApiService {

    private final AccountAccessResolver accountAccessResolver;
    private final OrderCommandService orderCommandService;
    private final OrderQueryService orderQueryService;
    private final AccountReadFacade accountReadFacade;
    private final StringRedisTemplate stringRedisTemplate;
    private final QuoteService quoteService;
    private final ApplicationEventPublisher eventPublisher;

    /**
     * 매수 주문 접수. 가격 제한폭 검증 후 DB 저장, 커밋 후 Kafka 발행.
     */
    @Transactional
    public OrderResponse placeBuyOrder(Long userId, BuyOrderRequest request) {

        log.info("[매수 접수] 종목={} 가격={} 수량={}", request.stockCode(), request.price(), request.quantity());

        // 계좌 검증
        Account account = accountAccessResolver.resolveAccountOwnedAndActive(userId, request.accountId());

        // 가격 제한폭 검증 ->
        validatePriceBandLimit(request.stockCode(), request.price());

        // request 를 command 로 변환
        BuyOrderCommand command = new BuyOrderCommand(
                request.accountId(), request.stockCode(),
                request.orderType(), request.price(), request.quantity());

        // 주문 접수
        PlaceOrderResultView result = orderCommandService.placeBuyOrder(command, account,
                // Supplier 로 account 의 미결제 미수금 합계를 계산하고 전달한다.
                () -> accountReadFacade.getAccountDetail(request.accountId()).unpaidSum());

        long orderId = result.orderId();
        log.info("[매수 DB 저장 완료] 종목={} 주문={}", request.stockCode(), orderId);

        eventPublisher.publishEvent(new OrderPlacedEvent(
                orderId, account.getAccountId(),
                request.stockCode(), OrderSide.BUY,
                request.price(), request.quantity(), Instant.now()));

        return OrderResponse.from(result);
    }

    /**
     * 매도 주문 접수. 가격 제한폭 검증 후 DB 저장, 커밋 후 Kafka 발행.
     */
    @Transactional
    public OrderResponse placeSellOrder(Long userId, SellOrderRequest request) {
        log.info("[매도 접수] 종목={} 가격={} 수량={}", request.stockCode(), request.price(), request.quantity());
        Account account = accountAccessResolver.resolveAccountOwnedAndActive(userId, request.accountId());
        validatePriceBandLimit(request.stockCode(), request.price());

        SellOrderCommand command = new SellOrderCommand(
                request.accountId(), request.stockCode(),
                request.orderType(), request.price(), request.quantity());
        PlaceOrderResultView result = orderCommandService.placeSellOrder(command, account);

        long orderId = result.orderId();
        log.info("[매도 DB 저장 완료] 종목={} 주문={}", request.stockCode(), orderId);

        eventPublisher.publishEvent(new OrderPlacedEvent(
                orderId, account.getAccountId(),
                request.stockCode(), OrderSide.SELL,
                request.price(), request.quantity(), Instant.now()));

        return OrderResponse.from(result);
    }

    /**
     * 주문 취소. DB 상태 CANCELLED 전환 후 커밋 완료 시 Kafka 발행.
     */
    @Transactional
    public CancelOrderResponse cancelOrder(Long userId, Long orderId) {
        var order = orderQueryService.getOrder(orderId);
        accountAccessResolver.resolveAccountOwnedAndActive(userId, order.getAccount().getAccountId());

        String stockCode = order.getStockCode();
        var result = orderCommandService.cancelOrder(orderId);

        eventPublisher.publishEvent(new OrderCancelledEvent(orderId, stockCode));

        return CancelOrderResponse.from(result);
    }

    private static final int DEFAULT_ORDER_PAGE_SIZE = 20;

    /**
     * 주문 내역 조회. accountId 기준, 기간·상태 필터.
     */
    @Transactional(readOnly = true)
    public PagedResponse<OrderDto> getOrdersPaged(Long userId, String accountId, String status, Long startAt,
            Long endAt) {
        long aid = Long.parseLong(accountId);
        accountAccessResolver.resolveAccountOwnedBy(userId, aid);
        Instant start = startAt != null ? Instant.ofEpochMilli(startAt) : Instant.EPOCH;
        Instant end = endAt != null ? Instant.ofEpochMilli(endAt) : Instant.now().plusSeconds(86400);
        OrderStatus orderStatus = (status != null && !status.isBlank()) ? OrderStatus.valueOf(status.toUpperCase())
                : null;
        var page = orderQueryService.getOrdersPaged(aid, orderStatus, start, end,
                PageRequest.of(0, DEFAULT_ORDER_PAGE_SIZE));
        return PagedResponse.of(toOrderDtos(page), page.getNumber(), page.getSize(), page.getTotalElements());
    }

    /**
     * 체결 내역 조회.
     */
    @Transactional(readOnly = true)
    public PagedResponse<TradeDto> getTradesPaged(Long userId, String accountId, Long startAt, Long endAt) {
        long aid = Long.parseLong(accountId);
        accountAccessResolver.resolveAccountOwnedBy(userId, aid);
        Instant start = startAt != null ? Instant.ofEpochMilli(startAt) : Instant.EPOCH;
        Instant end = endAt != null ? Instant.ofEpochMilli(endAt) : Instant.now().plusSeconds(86400);
        var page = orderQueryService.getFilledOrdersPaged(aid, start, end);
        return PagedResponse.of(toTradeDtos(page), page.getNumber(), page.getSize(), page.getTotalElements());
    }

    // ── private helpers ───────────────────────────────────────────────────────

    /**
     * 해당 종목의 가격 제한폭(Price Band)을 검증한다.
     * 기준가 대비 ±30% 범위를 벗어나면 예외를 던진다.
     * 
     * 기준가 결정 순서:
     * 1. LTP(최근 체결가, Redis) — 당일 체결이 한 번이라도 발생한 경우
     * 2. 전일 종가(previousClose) — 당일 첫 주문 등 LTP가 없는 경우
     */

    private void validatePriceBandLimit(String stockCode, BigDecimal price) {
        BigDecimal reference = getLtp(stockCode)
                .or(() -> quoteService.getQuote(stockCode).map(QuoteView::previousClose))
                .orElseThrow(() -> new IllegalArgumentException("기준가를 조회할 수 없는 종목: " + stockCode));

        BigDecimal upper = reference.multiply(new BigDecimal("1.3")).setScale(0, RoundingMode.DOWN);
        BigDecimal lower = reference.multiply(new BigDecimal("0.7")).setScale(0, RoundingMode.UP);

        if (price.compareTo(upper) > 0 || price.compareTo(lower) < 0) {
            throw new IllegalArgumentException(
                    "가격 제한폭 초과: " + price + " (기준가: " + reference + ", 범위: " + lower + "~" + upper + ")");
        }
    }

    private java.util.Optional<BigDecimal> getLtp(String stockCode) {
        String val = stringRedisTemplate.opsForValue().get(RedisKeys.ltp(stockCode));
        return val != null ? java.util.Optional.of(new BigDecimal(val)) : java.util.Optional.empty();
    }

    private List<OrderDto> toOrderDtos(Page<Order> page) {
        List<Order> content = page.getContent();
        Map<String, StockInfo> stockInfoMap = content.isEmpty() ? Map.of()
                : accountReadFacade.getStockInfoBatch(content.stream().map(Order::getStockCode).distinct().toList());
        return content.stream()
                .map(o -> toOrderDto(o,
                        stockInfoMap.getOrDefault(o.getStockCode(), new StockInfo("", BigDecimal.ZERO, BigDecimal.ZERO))))
                .toList();
    }

    private List<TradeDto> toTradeDtos(Page<Order> page) {
        List<Order> content = page.getContent();
        Map<String, StockInfo> stockInfoMap = content.isEmpty() ? Map.of()
                : accountReadFacade.getStockInfoBatch(content.stream().map(Order::getStockCode).distinct().toList());
        return content.stream()
                .map(o -> toTradeDto(o,
                        stockInfoMap.getOrDefault(o.getStockCode(), new StockInfo("", BigDecimal.ZERO, BigDecimal.ZERO))))
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
