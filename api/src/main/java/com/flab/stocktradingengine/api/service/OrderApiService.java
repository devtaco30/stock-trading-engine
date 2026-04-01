package com.flab.stocktradingengine.api.service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.flab.stocktradingengine.account.entity.Account;
import com.flab.stocktradingengine.api.dto.order.BuyOrderRequest;
import com.flab.stocktradingengine.api.dto.order.OrderDto;
import com.flab.stocktradingengine.api.dto.order.SellOrderRequest;
import com.flab.stocktradingengine.api.dto.trade.TradeDto;
import com.flab.stocktradingengine.api.redis.LtpRedisRepository;
import com.flab.stocktradingengine.api.resolver.AccountAccessResolver;
import com.flab.stocktradingengine.dto.common.PagedResponse;
import com.flab.stocktradingengine.exception.InvalidRequestException;
import com.flab.stocktradingengine.exception.ResourceNotFoundException;
import com.flab.stocktradingengine.market.service.QuoteService;
import com.flab.stocktradingengine.market.view.QuoteView;
import com.flab.stocktradingengine.market.view.StockInfo;
import com.flab.stocktradingengine.trading.entity.Order;
import com.flab.stocktradingengine.trading.entity.OrderSide;
import com.flab.stocktradingengine.trading.entity.OrderStatus;
import com.flab.stocktradingengine.trading.kafka.KafkaTopics;
import com.flab.stocktradingengine.trading.kafka.event.OrderCancelRequestEvent;
import com.flab.stocktradingengine.trading.kafka.event.OrderRequestEvent;
import com.flab.stocktradingengine.trading.service.OrderQueryService;

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
 * <h3>Phase 2 — Stateless API</h3>
 * <p>
 * API 서버는 입력 검증(계좌 소유·가격 제한폭)만 수행하고
 * order-requests 토픽(key=accountId)에 이벤트를 발행한 뒤 즉시 202 반환.
 * OrderRequestConsumer 가 수신해 잔고·보유 검증 후 DB 저장 및 orders.{stockCode} 발행.
 * accountId 파티셔닝으로 같은 계좌의 주문이 직렬 처리 → DB 비관적 락 경합 제거.
 * </p>
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class OrderApiService {

    private final AccountAccessResolver accountAccessResolver;
    private final OrderQueryService orderQueryService;
    private final LtpRedisRepository ltpRedisRepository;
    private final QuoteService quoteService;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    /**
     * 매수 주문 접수. 계좌·가격 제한폭 검증 후 order-requests(key=accountId) 발행.
     */
    public void placeBuyOrder(Long userId, BuyOrderRequest request) {
        
        Account account = accountAccessResolver.resolveAccountOwnedAndActive(userId, request.accountId());

        validatePriceBandLimit(request.stockCode(), request.price());

        kafkaTemplate.send(
                KafkaTopics.orderRequests(),
                String.valueOf(account.getAccountId()),
                new OrderRequestEvent(
                        account.getAccountId(), request.stockCode(),
                        OrderSide.BUY, request.orderType(),
                        request.price(), request.quantity(), Instant.now()));

        log.info("[매수 접수] 종목={} 계좌={}", request.stockCode(), account.getAccountId());
    }

    /**
     * 매도 주문 접수. 계좌·가격 제한폭 검증 후 order-requests(key=accountId) 발행.
     */
    public void placeSellOrder(Long userId, SellOrderRequest request) {
        Account account = accountAccessResolver.resolveAccountOwnedAndActive(userId, request.accountId());
        validatePriceBandLimit(request.stockCode(), request.price());

        kafkaTemplate.send(
                KafkaTopics.orderRequests(),
                String.valueOf(account.getAccountId()),
                new OrderRequestEvent(
                        account.getAccountId(), request.stockCode(),
                        OrderSide.SELL, request.orderType(),
                        request.price(), request.quantity(), Instant.now()));

        log.info("[매도 접수] 종목={} 계좌={}", request.stockCode(), account.getAccountId());
    }

    /**
     * 주문 취소 접수. 주문 조회·계좌 소유 검증 후 order-requests(key=accountId) 발행.
     * Order.account lazy loading을 위해 readOnly 트랜잭션 필요.
     */
    @Transactional(readOnly = true)
    public void cancelOrder(Long userId, Long orderId) {
        Order order = orderQueryService.getOrder(orderId);
        accountAccessResolver.resolveAccountOwnedAndActive(userId, order.getAccount().getAccountId());

        kafkaTemplate.send(
                KafkaTopics.orderRequests(),
                String.valueOf(order.getAccount().getAccountId()),
                new OrderCancelRequestEvent(
                        order.getAccount().getAccountId(), orderId, order.getStockCode()));

        log.info("[취소 접수] 종목={} 주문={}", order.getStockCode(), orderId);
    }

    private static final int DEFAULT_ORDER_PAGE_SIZE = 20;

    /**
     * 주문 내역 조회. accountId 기준, 기간·상태 필터.
     */
    @Transactional(readOnly = true)
    public PagedResponse<OrderDto> getOrdersPaged(Long userId, Long accountId, String status, Long startAt,
            Long endAt) {
        accountAccessResolver.resolveAccountOwnedBy(userId, accountId);
        Instant end = endAt != null ? Instant.ofEpochMilli(endAt) : Instant.now();
        Instant start = startAt != null ? Instant.ofEpochMilli(startAt) : end.minus(3, ChronoUnit.DAYS);
        OrderStatus orderStatus = (status != null && !status.isBlank()) ? OrderStatus.valueOf(status.toUpperCase())
                : null;
        Page<Order> page = orderQueryService.getOrdersPaged(accountId, orderStatus, start, end,
                PageRequest.of(0, DEFAULT_ORDER_PAGE_SIZE));
        return PagedResponse.of(toOrderDtos(page), page.getNumber(), page.getSize(), page.getTotalElements());
    }

    /**
     * 체결 내역 조회.
     */
    @Transactional(readOnly = true)
    public PagedResponse<TradeDto> getTradesPaged(Long userId, Long accountId, Long startAt, Long endAt) {
        accountAccessResolver.resolveAccountOwnedBy(userId, accountId);
        Instant end = endAt != null ? Instant.ofEpochMilli(endAt) : Instant.now();
        Instant start = startAt != null ? Instant.ofEpochMilli(startAt) : end.minus(3, ChronoUnit.DAYS);
        Page<Order> page = orderQueryService.getFilledOrdersPaged(accountId, start, end);
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
                .orElseThrow(() -> new ResourceNotFoundException("기준가를 조회할 수 없는 종목: " + stockCode));

        BigDecimal upper = reference.multiply(new BigDecimal("1.3")).setScale(0, RoundingMode.DOWN);
        BigDecimal lower = reference.multiply(new BigDecimal("0.7")).setScale(0, RoundingMode.UP);

        if (price.compareTo(upper) > 0 || price.compareTo(lower) < 0) {
            throw new InvalidRequestException(
                    "가격 제한폭 초과: " + price + " (기준가: " + reference + ", 범위: " + lower + "~" + upper + ")");
        }
    }

    private Optional<BigDecimal> getLtp(String stockCode) {
        return ltpRedisRepository.get(stockCode);
    }

    private List<OrderDto> toOrderDtos(Page<Order> page) {
        List<Order> content = page.getContent();
        Map<String, StockInfo> stockInfoMap = content.isEmpty() ? Map.of()
                : quoteService.getStockInfoBatch(content.stream().map(Order::getStockCode).distinct().toList());
        return content.stream()
                .map(o -> toOrderDto(o,
                        stockInfoMap.getOrDefault(o.getStockCode(), new StockInfo("", BigDecimal.ZERO, BigDecimal.ZERO))))
                .toList();
    }

    private List<TradeDto> toTradeDtos(Page<Order> page) {
        List<Order> content = page.getContent();
        Map<String, StockInfo> stockInfoMap = content.isEmpty() ? Map.of()
                : quoteService.getStockInfoBatch(content.stream().map(Order::getStockCode).distinct().toList());
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
