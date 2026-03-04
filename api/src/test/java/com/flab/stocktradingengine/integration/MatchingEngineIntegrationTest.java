package com.flab.stocktradingengine.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.kafka.support.Acknowledgment;

import com.flab.stocktradingengine.config.AsyncConfig;
import com.flab.stocktradingengine.trading.kafka.consumer.MatchingConsumer;
import com.flab.stocktradingengine.trading.kafka.event.OrderCancelledEvent;
import com.flab.stocktradingengine.trading.kafka.event.OrderPlacedEvent;
import com.flab.stocktradingengine.settlement.service.MatchingFillHandler;
import com.flab.stocktradingengine.settlement.service.OrderSettlementService;
import com.flab.stocktradingengine.trading.entity.OrderSide;
import com.flab.stocktradingengine.trading.matching.OrderBookRegistry;
import com.flab.stocktradingengine.trading.service.OrderQueryService;

/**
 * 매칭 컨슈머 통합 테스트.
 *
 * <h3>테스트 범위</h3>
 * <pre>
 * [실제 빈]
 * MatchingConsumer → OrderBookRegistry → OrderBook
 *   → ApplicationEventPublisher.publishEvent(OrderFilledEvent)
 *   → MatchingFillHandler (@Async @EventListener)
 *
 * [MockBean]
 * OrderSettlementService  ← DB 의존 차단
 * </pre>
 *
 * <h3>종목코드 격리</h3>
 * <p>테스트들이 같은 Spring 컨텍스트(OrderBookRegistry)를 공유하므로
 * 호가창 상태가 테스트 간에 누적된다.
 * 각 테스트가 고유한 종목코드를 사용해 서로의 호가창에 영향을 주지 않도록 격리한다.</p>
 *
 * <h3>동기/비동기 경계</h3>
 * <p>{@code consume()} 내의 매칭(OrderBook)은 호출 스레드에서 동기로 실행된다.
 * DB 반영({@code MatchingFillHandler})만 @Async 풀에서 비동기로 실행되므로
 * {@code CountDownLatch} 로 핸들러 완료를 기다린 뒤 assert 한다.</p>
 */
@SpringBootTest(classes = {
    OrderBookRegistry.class,
    MatchingConsumer.class,
    MatchingFillHandler.class,
    AsyncConfig.class,
    OrderQueryService.class
})
@DisplayName("매칭 컨슈머 통합 테스트")
class MatchingEngineIntegrationTest {

    @Autowired MatchingConsumer matchingConsumer;
    @Autowired OrderBookRegistry orderBookRegistry;

    @MockBean
    OrderSettlementService orderSettlementService;

    @MockBean
    com.flab.stocktradingengine.trading.repository.OrderRepository orderRepository;

    // ── 헬퍼 ─────────────────────────────────────────────────────────────────

    private static final Acknowledgment NO_OP_ACK = () -> {};

    private void submitBuy(long orderId, long accountId, String stock, String price, int qty) {
        OrderPlacedEvent event = new OrderPlacedEvent(
            orderId, accountId, stock, OrderSide.BUY, new BigDecimal(price), qty, Instant.now());
        matchingConsumer.consume(record(stock, event), NO_OP_ACK);
    }

    private void submitSell(long orderId, long accountId, String stock, String price, int qty) {
        OrderPlacedEvent event = new OrderPlacedEvent(
            orderId, accountId, stock, OrderSide.SELL, new BigDecimal(price), qty, Instant.now());
        matchingConsumer.consume(record(stock, event), NO_OP_ACK);
    }

    private void cancelOrder(String stock, long orderId) {
        matchingConsumer.consume(record(stock, new OrderCancelledEvent(orderId, stock)), NO_OP_ACK);
    }

    private ConsumerRecord<String, Object> record(String stockCode, Object value) {
        return new ConsumerRecord<>("orders." + stockCode, 0, 0L, stockCode, value);
    }

    // ── 전량 체결 ─────────────────────────────────────────────────────────────

    @Nested
    @DisplayName("전량 체결")
    class FullFill {

        private static final String STOCK = "FULL_001";

        @Test
        @DisplayName("매수·매도 동일 수량 → fillBuy/fillSell 올바른 인자로 각 1회 호출")
        void 전량체결_핸들러_정확한_인자() throws InterruptedException {
            CountDownLatch latch = new CountDownLatch(2);
            doAnswer(inv -> { latch.countDown(); return null; })
                .when(orderSettlementService).fillBuyOrderPartially(anyLong(), anyInt(), any(BigDecimal.class));
            doAnswer(inv -> { latch.countDown(); return null; })
                .when(orderSettlementService).fillSellOrderPartially(anyLong(), anyInt());

            submitBuy(1L, 100L, STOCK, "70000", 10);
            submitSell(2L, 200L, STOCK, "70000", 10);

            assertThat(latch.await(2, TimeUnit.SECONDS)).isTrue();
            verify(orderSettlementService).fillBuyOrderPartially(eq(1L), eq(10), eq(new BigDecimal("70000")));
            verify(orderSettlementService).fillSellOrderPartially(eq(2L), eq(10));
        }

        @Test
        @DisplayName("체결가는 매도 호가(ask) 기준으로 핸들러에 전달")
        void 전량체결_체결가_ask_기준() throws InterruptedException {
            CountDownLatch latch = new CountDownLatch(2);
            doAnswer(inv -> { latch.countDown(); return null; })
                .when(orderSettlementService).fillBuyOrderPartially(anyLong(), anyInt(), any(BigDecimal.class));
            doAnswer(inv -> { latch.countDown(); return null; })
                .when(orderSettlementService).fillSellOrderPartially(anyLong(), anyInt());

            // 매수 71000, 매도 69000 → 체결가는 ask=69000
            submitBuy(3L, 100L, STOCK, "71000", 5);
            submitSell(4L, 200L, STOCK, "69000", 5);

            assertThat(latch.await(2, TimeUnit.SECONDS)).isTrue();
            verify(orderSettlementService).fillBuyOrderPartially(eq(3L), eq(5), eq(new BigDecimal("69000")));
        }
    }

    // ── 부분 체결 ─────────────────────────────────────────────────────────────

    @Nested
    @DisplayName("부분 체결")
    class PartialFill {

        private static final String STOCK = "PART_001";

        @Test
        @DisplayName("매수 100주 vs 매도 30주 → fillQty=30으로 호출")
        void 부분체결_체결수량() throws InterruptedException {
            CountDownLatch latch = new CountDownLatch(2);
            doAnswer(inv -> { latch.countDown(); return null; })
                .when(orderSettlementService).fillBuyOrderPartially(anyLong(), anyInt(), any(BigDecimal.class));
            doAnswer(inv -> { latch.countDown(); return null; })
                .when(orderSettlementService).fillSellOrderPartially(anyLong(), anyInt());

            submitBuy(5L, 100L, STOCK, "70000", 100);
            submitSell(6L, 200L, STOCK, "70000", 30);

            assertThat(latch.await(2, TimeUnit.SECONDS)).isTrue();
            verify(orderSettlementService).fillBuyOrderPartially(eq(5L), eq(30), any(BigDecimal.class));
            verify(orderSettlementService).fillSellOrderPartially(eq(6L), eq(30));
        }

        @Test
        @DisplayName("매수 1건 vs 매도 2건 순차 접수 → 핸들러 각 2회 호출")
        void 부분체결_연속_매칭() throws InterruptedException {
            CountDownLatch latch = new CountDownLatch(4); // fillBuy×2 + fillSell×2
            doAnswer(inv -> { latch.countDown(); return null; })
                .when(orderSettlementService).fillBuyOrderPartially(anyLong(), anyInt(), any(BigDecimal.class));
            doAnswer(inv -> { latch.countDown(); return null; })
                .when(orderSettlementService).fillSellOrderPartially(anyLong(), anyInt());

            submitBuy(7L, 100L, STOCK, "70000", 100);
            submitSell(8L, 200L, STOCK, "70000", 40);
            submitSell(9L, 200L, STOCK, "70000", 60);

            assertThat(latch.await(2, TimeUnit.SECONDS)).isTrue();
            verify(orderSettlementService).fillBuyOrderPartially(eq(7L), eq(40), any(BigDecimal.class));
            verify(orderSettlementService).fillBuyOrderPartially(eq(7L), eq(60), any(BigDecimal.class));
            verify(orderSettlementService).fillSellOrderPartially(eq(8L), eq(40));
            verify(orderSettlementService).fillSellOrderPartially(eq(9L), eq(60));
        }
    }

    // ── 체결 불가 ─────────────────────────────────────────────────────────────

    @Nested
    @DisplayName("체결 불가 (스프레드)")
    class NoMatch {

        private static final String STOCK = "NONE_001";

        @Test
        @DisplayName("매수가 < 매도가 → 핸들러 미호출")
        void 스프레드_핸들러_미호출() throws InterruptedException {
            submitBuy(10L, 100L, STOCK, "69000", 10);
            submitSell(11L, 200L, STOCK, "70000", 10);

            // consume() 은 동기이므로 두 주문이 이미 처리됨. 체결 이벤트가 없으면 핸들러 미호출.
            Thread.sleep(200);

            verify(orderSettlementService, never()).fillBuyOrderPartially(anyLong(), anyInt(), any(BigDecimal.class));
            verify(orderSettlementService, never()).fillSellOrderPartially(anyLong(), anyInt());
        }
    }

    // ── 주문 취소 ─────────────────────────────────────────────────────────────

    @Nested
    @DisplayName("주문 취소 후 매칭 (Lazy Removal)")
    class Cancel {

        private static final String STOCK_CANCEL_BUY  = "CANCEL_BUY_001";
        private static final String STOCK_CANCEL_SELL = "CANCEL_SELL_001";

        @Test
        @DisplayName("매수 취소 후 매도 접수 → 체결 없음")
        void 매수_취소_후_매도_접수_체결없음() throws InterruptedException {
            // consume() 이 동기이므로 호출 순서대로 처리 보장
            submitBuy(20L, 100L, STOCK_CANCEL_BUY, "70000", 10);
            cancelOrder(STOCK_CANCEL_BUY, 20L);
            submitSell(21L, 200L, STOCK_CANCEL_BUY, "70000", 10);

            Thread.sleep(200);

            verify(orderSettlementService, never()).fillBuyOrderPartially(anyLong(), anyInt(), any(BigDecimal.class));
            verify(orderSettlementService, never()).fillSellOrderPartially(anyLong(), anyInt());
        }

        @Test
        @DisplayName("매도 취소 후 매수 접수 → 체결 없음")
        void 매도_취소_후_매수_접수_체결없음() throws InterruptedException {
            submitSell(22L, 200L, STOCK_CANCEL_SELL, "70000", 10);
            cancelOrder(STOCK_CANCEL_SELL, 22L);
            submitBuy(23L, 100L, STOCK_CANCEL_SELL, "70000", 10);

            Thread.sleep(200);

            verify(orderSettlementService, never()).fillBuyOrderPartially(anyLong(), anyInt(), any(BigDecimal.class));
            verify(orderSettlementService, never()).fillSellOrderPartially(anyLong(), anyInt());
        }
    }

    // ── 최근 체결가 갱신 ──────────────────────────────────────────────────────

    @Nested
    @DisplayName("최근 체결가 (lastTradedPrice) 갱신")
    class LastTradedPrice {

        private static final String STOCK = "LTP_001";

        @Test
        @DisplayName("체결 전 empty, 체결 후 ask 가격으로 갱신")
        void 체결_후_lastTradedPrice_ask_가격으로_갱신() throws InterruptedException {
            CountDownLatch latch = new CountDownLatch(2);
            doAnswer(inv -> { latch.countDown(); return null; })
                .when(orderSettlementService).fillBuyOrderPartially(anyLong(), anyInt(), any(BigDecimal.class));
            doAnswer(inv -> { latch.countDown(); return null; })
                .when(orderSettlementService).fillSellOrderPartially(anyLong(), anyInt());

            assertThat(orderBookRegistry.getLastTradedPrice(STOCK)).isEmpty();

            // 매수 71000, 매도 69500 → 체결가는 ask = 69500
            submitBuy(24L, 100L, STOCK, "71000", 10);
            submitSell(25L, 200L, STOCK, "69500", 10);

            assertThat(latch.await(2, TimeUnit.SECONDS)).isTrue();
            assertThat(orderBookRegistry.getLastTradedPrice(STOCK))
                .hasValue(new BigDecimal("69500"));
        }
    }

    // ── FIFO 우선순위 ─────────────────────────────────────────────────────────

    @Nested
    @DisplayName("동일 가격 FIFO 우선순위")
    class FifoPriority {

        private static final String STOCK = "FIFO_001";

        @Test
        @DisplayName("동일 가격 매수 2건 중 먼저 접수된 주문이 우선 체결")
        void 동일가격_먼저_접수된_매수_우선_체결() throws InterruptedException {
            CountDownLatch latch = new CountDownLatch(2);
            doAnswer(inv -> { latch.countDown(); return null; })
                .when(orderSettlementService).fillBuyOrderPartially(anyLong(), anyInt(), any(BigDecimal.class));
            doAnswer(inv -> { latch.countDown(); return null; })
                .when(orderSettlementService).fillSellOrderPartially(anyLong(), anyInt());

            submitBuy(26L, 100L, STOCK, "70000", 10); // 먼저 접수
            submitBuy(27L, 101L, STOCK, "70000", 10); // 나중 접수
            submitSell(28L, 200L, STOCK, "70000", 10); // 매도 10주

            assertThat(latch.await(2, TimeUnit.SECONDS)).isTrue();
            verify(orderSettlementService).fillBuyOrderPartially(eq(26L), eq(10), any(BigDecimal.class));
            verify(orderSettlementService, never()).fillBuyOrderPartially(eq(27L), anyInt(), any(BigDecimal.class));
            verify(orderSettlementService).fillSellOrderPartially(eq(28L), eq(10));
        }
    }

    // ── 부분 체결 (매도 기준) ─────────────────────────────────────────────────

    @Nested
    @DisplayName("부분 체결 (매도 기준)")
    class PartialFillSell {

        private static final String STOCK = "PART_SELL_001";

        @Test
        @DisplayName("매도 1건 vs 매수 2건 순차 접수 → 핸들러 각 2회 호출")
        void 부분체결_매도1_매수2_연속_매칭() throws InterruptedException {
            CountDownLatch latch = new CountDownLatch(4); // fillBuy×2 + fillSell×2
            doAnswer(inv -> { latch.countDown(); return null; })
                .when(orderSettlementService).fillBuyOrderPartially(anyLong(), anyInt(), any(BigDecimal.class));
            doAnswer(inv -> { latch.countDown(); return null; })
                .when(orderSettlementService).fillSellOrderPartially(anyLong(), anyInt());

            submitSell(29L, 200L, STOCK, "70000", 100);
            submitBuy(30L, 100L, STOCK, "70000", 40);
            submitBuy(31L, 101L, STOCK, "70000", 60);

            assertThat(latch.await(2, TimeUnit.SECONDS)).isTrue();
            verify(orderSettlementService).fillSellOrderPartially(eq(29L), eq(40));
            verify(orderSettlementService).fillSellOrderPartially(eq(29L), eq(60));
            verify(orderSettlementService).fillBuyOrderPartially(eq(30L), eq(40), any(BigDecimal.class));
            verify(orderSettlementService).fillBuyOrderPartially(eq(31L), eq(60), any(BigDecimal.class));
        }
    }
}
