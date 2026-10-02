package com.flab.stocktradingengine.trading.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Optional;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.flab.stocktradingengine.account.entity.Account;
import com.flab.stocktradingengine.account.entity.Holding;
import com.flab.stocktradingengine.account.exception.InsufficientResourceException;
import com.flab.stocktradingengine.account.service.AccountService;
import com.flab.stocktradingengine.time.EpochNanos;
import com.flab.stocktradingengine.trading.command.BuyOrderCommand;
import com.flab.stocktradingengine.trading.command.SellOrderCommand;
import com.flab.stocktradingengine.trading.entity.Order;
import com.flab.stocktradingengine.trading.repository.OrderRepository;
import com.flab.stocktradingengine.trading.view.PlaceOrderResultView;

/**
 * 구간 분해 측정(V-P3~V-P6)용 {@link OrderStageTimes}가 접수 경로의 지점 순서대로 채워지는지 확인한다.
 * 시각 인자가 없는 기존 메서드는 그대로 동작해야 하므로 기존 {@code OrderWriterTest}가 그것을 맡는다.
 */
@DisplayName("OrderStageTimes - 접수 경로 지점 순서")
class OrderStageTimesFlowTest {

    private static final Long ACCOUNT_ID = 1001L;
    private static final String STOCK = "005930";

    private final OrderRepository orderRepository = mock(OrderRepository.class);
    private final AccountService accountService = mock(AccountService.class);
    private final OrderWriter orderWriter = new OrderWriter(orderRepository, accountService);

    private BuyOrderCommand buyCommand(String price) {
        return new BuyOrderCommand(ACCOUNT_ID, STOCK, "LIMIT", new BigDecimal(price), 1,
            Instant.parse("2026-01-01T00:00:00Z"), "req-buy");
    }

    private SellOrderCommand sellCommand(int quantity) {
        return new SellOrderCommand(ACCOUNT_ID, STOCK, "LIMIT", new BigDecimal("70000"), quantity,
            Instant.parse("2026-01-01T00:00:00Z"), "req-sell");
    }

    private Account richAccount() {
        Account account = mock(Account.class);
        when(account.getMarginRate()).thenReturn(new BigDecimal("0.40"));
        when(account.getBalance()).thenReturn(new BigDecimal("100000000"));
        when(account.getAccountId()).thenReturn(ACCOUNT_ID);
        return account;
    }

    private static void assertAscending(long... times) {
        for (int i = 0; i < times.length; i++) {
            assertThat(times[i]).as("index %d", i).isPositive();
            if (i > 0) {
                assertThat(times[i]).as("index %d >= %d", i, i - 1).isGreaterThanOrEqualTo(times[i - 1]);
            }
        }
    }

    @Test
    @DisplayName("매수 승인: 락, 합계 쿼리, 판정 도달, 저장 순서로 시각이 채워진다")
    void buyFillsTimesInOrder() {
        Account account = richAccount();
        when(accountService.getAccountByAccountIdForUpdate(ACCOUNT_ID)).thenReturn(Optional.of(account));
        when(orderRepository.sumReservedMarginByAccountId(ACCOUNT_ID)).thenReturn(BigDecimal.ZERO);
        when(orderRepository.saveAndFlush(any(Order.class))).thenAnswer(inv -> inv.getArgument(0));
        OrderStageTimes times = OrderStageTimes.startingAt(EpochNanos.now());

        orderWriter.writeBuyOrder(buyCommand("10000"), () -> BigDecimal.ZERO, times);

        assertAscending(times.receivedAtNanos(), times.lockedAtNanos(), times.summedAtNanos(),
            times.decidedAtNanos(), times.insertedAtNanos());
    }

    @Test
    @DisplayName("매수 거부(한도 초과): 판정 도달과 저장 시각은 비어 있다")
    void buyRejectedLeavesDecisionEmpty() {
        Account account = richAccount();
        when(accountService.getAccountByAccountIdForUpdate(ACCOUNT_ID)).thenReturn(Optional.of(account));
        when(orderRepository.sumReservedMarginByAccountId(ACCOUNT_ID)).thenReturn(BigDecimal.ZERO);
        OrderStageTimes times = OrderStageTimes.startingAt(EpochNanos.now());

        assertThatThrownBy(() -> orderWriter.writeBuyOrder(buyCommand("999999999"), () -> BigDecimal.ZERO, times))
            .isInstanceOf(InsufficientResourceException.class);

        assertThat(times.summedAtNanos()).isPositive();
        assertThat(times.decidedAtNanos()).isZero();
        assertThat(times.insertedAtNanos()).isZero();
    }

    @Test
    @DisplayName("매도 승인: 락, 합계 쿼리, 판정 도달, 저장 순서로 시각이 채워진다")
    void sellFillsTimesInOrder() {
        Account account = mock(Account.class);
        when(accountService.getHoldingByAccountIdForUpdate(ACCOUNT_ID, STOCK))
            .thenReturn(Optional.of(new Holding(account, STOCK, 100, new BigDecimal("70000"))));
        when(orderRepository.sumPendingSellQuantity(ACCOUNT_ID, STOCK)).thenReturn(0L);
        when(orderRepository.saveAndFlush(any(Order.class))).thenAnswer(inv -> inv.getArgument(0));
        OrderStageTimes times = OrderStageTimes.startingAt(EpochNanos.now());

        orderWriter.writeSellOrder(sellCommand(10), times);

        assertAscending(times.receivedAtNanos(), times.lockedAtNanos(), times.summedAtNanos(),
            times.decidedAtNanos(), times.insertedAtNanos());
    }

    @Test
    @DisplayName("서비스: 멱등 조회를 마친 시각을 남기고 시각을 실은 writer 메서드를 부른다")
    void serviceStampsIdempotencyCheck() {
        OrderWriter writer = mock(OrderWriter.class);
        OrderIdempotencyReader reader = mock(OrderIdempotencyReader.class);
        when(reader.findByRequestId("req-buy")).thenReturn(Optional.empty());
        PlaceOrderResultView view = new PlaceOrderResultView(1L, "PENDING", 0L, BigDecimal.ONE);
        OrderStageTimes times = OrderStageTimes.startingAt(EpochNanos.now());
        when(writer.writeBuyOrder(any(), any(), any(OrderStageTimes.class))).thenReturn(view);
        OrderCommandService service = new OrderCommandService(writer, reader, mock(OrderRepository.class));

        service.placeBuyOrder(buyCommand("10000"), () -> BigDecimal.ZERO, times);

        assertThat(times.idemCheckedAtNanos()).isGreaterThanOrEqualTo(times.receivedAtNanos()).isPositive();
        verify(writer).writeBuyOrder(any(), any(), any(OrderStageTimes.class));
    }

    @Test
    @DisplayName("서비스: 이미 접수된 requestId면 writer를 부르지 않아 락 이후 시각이 비어 있다")
    void duplicateLeavesWriterTimesEmpty() {
        OrderWriter writer = mock(OrderWriter.class);
        OrderIdempotencyReader reader = mock(OrderIdempotencyReader.class);
        Order existing = Order.builder().stockCode(STOCK).orderAt(Instant.now()).status(com.flab.stocktradingengine.trading.entity.OrderStatus.PENDING).build();
        when(reader.findByRequestId("req-buy")).thenReturn(Optional.of(existing));
        OrderStageTimes times = OrderStageTimes.startingAt(EpochNanos.now());
        OrderCommandService service = new OrderCommandService(writer, reader, mock(OrderRepository.class));

        service.placeBuyOrder(buyCommand("10000"), () -> BigDecimal.ZERO, times);

        assertThat(times.lockedAtNanos()).isZero();
        verify(writer, never()).writeBuyOrder(any(), any(), any(OrderStageTimes.class));
    }
}
