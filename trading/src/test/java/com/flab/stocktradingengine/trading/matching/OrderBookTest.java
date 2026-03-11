package com.flab.stocktradingengine.trading.matching;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import com.flab.stocktradingengine.trading.entity.OrderSide;

@DisplayName("OrderBook 단위 테스트")
class OrderBookTest {

    private OrderBook book;

    @BeforeEach
    void setUp() {
        book = new OrderBook();
    }

    // ── 헬퍼 ────────────────────────────────────────────────────────────────

    private OrderEntry buy(long orderId, BigDecimal price, int quantity) {
        return new OrderEntry(orderId, 100L, "005930", OrderSide.BUY, price, quantity, Instant.now());
    }

    private OrderEntry sell(long orderId, BigDecimal price, int quantity) {
        return new OrderEntry(orderId, 200L, "005930", OrderSide.SELL, price, quantity, Instant.now());
    }

    // ── 체결 조건 ────────────────────────────────────────────────────────────

    @Nested
    @DisplayName("체결 조건")
    class MatchCondition {

        @Test
        @DisplayName("매수가 > 매도가 이면 체결 발생")
        void 매수가_매도가_초과시_체결() {
            book.addOrder(buy(1L, new BigDecimal("70000"), 10));
            book.addOrder(sell(2L, new BigDecimal("69000"), 10));

            Optional<FillResult> result = book.match();

            assertThat(result).isPresent();
        }

        @Test
        @DisplayName("매수가 == 매도가 이면 체결 발생")
        void 매수가_매도가_같으면_체결() {
            book.addOrder(buy(1L, new BigDecimal("70000"), 10));
            book.addOrder(sell(2L, new BigDecimal("70000"), 10));

            Optional<FillResult> result = book.match();

            assertThat(result).isPresent();
        }

        @Test
        @DisplayName("매수가 < 매도가 이면 체결 없음 (스프레드)")
        void 매수가_매도가_미만시_체결없음() {
            book.addOrder(buy(1L, new BigDecimal("69000"), 10));
            book.addOrder(sell(2L, new BigDecimal("70000"), 10));

            Optional<FillResult> result = book.match();

            assertThat(result).isEmpty();
        }

        @Test
        @DisplayName("매수 주문만 있으면 체결 없음")
        void 매수만_있으면_체결없음() {
            book.addOrder(buy(1L, new BigDecimal("70000"), 10));

            assertThat(book.match()).isEmpty();
        }

        @Test
        @DisplayName("매도 주문만 있으면 체결 없음")
        void 매도만_있으면_체결없음() {
            book.addOrder(sell(1L, new BigDecimal("70000"), 10));

            assertThat(book.match()).isEmpty();
        }
    }

    // ── 체결가 ───────────────────────────────────────────────────────────────

    @Nested
    @DisplayName("체결가")
    class MatchPrice {

        @Test
        @DisplayName("체결가는 매도 호가(ask) 기준")
        void 체결가는_매도호가_기준() {
            book.addOrder(buy(1L, new BigDecimal("71000"), 10));
            book.addOrder(sell(2L, new BigDecimal("69000"), 10));

            FillResult result = book.match().orElseThrow();

            assertThat(result.matchPrice()).isEqualByComparingTo("69000");
        }
    }

    // ── 부분 체결 ────────────────────────────────────────────────────────────

    @Nested
    @DisplayName("부분 체결")
    class PartialFill {

        @Test
        @DisplayName("매수 수량 > 매도 수량: 매도 수량만큼만 체결, 매수 잔량 남음")
        void 매수_수량이_많으면_매도_수량만큼_체결() {
            OrderEntry buyOrder = buy(1L, new BigDecimal("70000"), 100);
            book.addOrder(buyOrder);
            book.addOrder(sell(2L, new BigDecimal("70000"), 30));

            FillResult result = book.match().orElseThrow();

            assertThat(result.filledQuantity()).isEqualTo(30);
            assertThat(buyOrder.getRemainingQuantity()).isEqualTo(70);
        }

        @Test
        @DisplayName("매도 수량 > 매수 수량: 매수 수량만큼만 체결, 매도 잔량 남음")
        void 매도_수량이_많으면_매수_수량만큼_체결() {
            book.addOrder(buy(1L, new BigDecimal("70000"), 30));
            OrderEntry sellOrder = sell(2L, new BigDecimal("70000"), 100);
            book.addOrder(sellOrder);

            FillResult result = book.match().orElseThrow();

            assertThat(result.filledQuantity()).isEqualTo(30);
            assertThat(sellOrder.getRemainingQuantity()).isEqualTo(70);
        }

        @Test
        @DisplayName("부분 체결 후 잔량이 남은 주문은 다음 체결에 참여")
        void 부분_체결_후_잔량_주문_재체결() {
            book.addOrder(buy(1L, new BigDecimal("70000"), 100));
            book.addOrder(sell(2L, new BigDecimal("70000"), 30));
            book.addOrder(sell(3L, new BigDecimal("70000"), 70));

            book.match(); // 1차: 매수100 vs 매도30 → 30 체결
            FillResult second = book.match().orElseThrow(); // 2차: 매수70 vs 매도70 → 70 체결

            assertThat(second.filledQuantity()).isEqualTo(70);
        }
    }

    // ── 전량 체결 후 제거 ────────────────────────────────────────────────────

    @Nested
    @DisplayName("전량 체결 후 호가창 제거")
    class FullFillRemoval {

        @Test
        @DisplayName("전량 체결된 주문은 cancelOrder가 false 반환 (orderIndex에서 제거됨)")
        void 전량_체결_후_cancelOrder_false() {
            book.addOrder(buy(1L, new BigDecimal("70000"), 10));
            book.addOrder(sell(2L, new BigDecimal("70000"), 10));

            book.match();

            assertThat(book.cancelOrder(1L)).isFalse();
            assertThat(book.cancelOrder(2L)).isFalse();
        }

        @Test
        @DisplayName("전량 체결 후 추가 match()는 empty")
        void 전량_체결_후_추가_match_empty() {
            book.addOrder(buy(1L, new BigDecimal("70000"), 10));
            book.addOrder(sell(2L, new BigDecimal("70000"), 10));

            book.match();

            assertThat(book.match()).isEmpty();
        }
    }

    // ── 주문 취소 ─────────────────────────────────────────────────────────────

    @Nested
    @DisplayName("주문 취소")
    class CancelOrder {

        @Test
        @DisplayName("존재하는 주문 취소 시 true 반환")
        void 존재하는_주문_취소_성공() {
            book.addOrder(buy(1L, new BigDecimal("70000"), 10));

            assertThat(book.cancelOrder(1L)).isTrue();
        }

        @Test
        @DisplayName("존재하지 않는 주문 취소 시 false 반환")
        void 존재하지_않는_주문_취소_false() {
            assertThat(book.cancelOrder(999L)).isFalse();
        }

        @Test
        @DisplayName("이중 취소 시 두 번째는 false 반환")
        void 이중_취소_false() {
            book.addOrder(buy(1L, new BigDecimal("70000"), 10));

            book.cancelOrder(1L);

            assertThat(book.cancelOrder(1L)).isFalse();
        }
    }

    // ── Lazy Removal ─────────────────────────────────────────────────────────

    @Nested
    @DisplayName("Lazy Removal")
    class LazyRemoval {

        @Test
        @DisplayName("취소된 매수 주문은 match() 시 건너뜀 → 체결 없음")
        void 취소된_매수_주문은_체결_안됨() {
            book.addOrder(buy(1L, new BigDecimal("70000"), 10));
            book.addOrder(sell(2L, new BigDecimal("70000"), 10));
            book.cancelOrder(1L);

            assertThat(book.match()).isEmpty();
        }

        @Test
        @DisplayName("취소된 매도 주문은 match() 시 건너뜀 → 체결 없음")
        void 취소된_매도_주문은_체결_안됨() {
            book.addOrder(buy(1L, new BigDecimal("70000"), 10));
            book.addOrder(sell(2L, new BigDecimal("70000"), 10));
            book.cancelOrder(2L);

            assertThat(book.match()).isEmpty();
        }

        @Test
        @DisplayName("취소된 주문 뒤의 유효 주문은 정상 체결")
        void 취소된_주문_뒤_유효_주문_체결() {
            // 같은 가격에 두 매수 주문 등록. 첫 번째를 취소.
            book.addOrder(buy(1L, new BigDecimal("70000"), 10));
            book.addOrder(buy(2L, new BigDecimal("70000"), 10));
            book.addOrder(sell(3L, new BigDecimal("70000"), 10));
            book.cancelOrder(1L); // 첫 번째 매수 취소

            FillResult result = book.match().orElseThrow();

            // 취소된 1번이 아닌 2번 매수와 체결
            assertThat(result.buyOrderId()).isEqualTo(2L);
        }
    }

    // ── FIFO (가격-시간 우선순위) ─────────────────────────────────────────────

    @Nested
    @DisplayName("FIFO - 가격-시간 우선순위")
    class FifoPriority {

        @Test
        @DisplayName("같은 가격의 매수 주문은 먼저 등록된 순서로 체결")
        void 같은_가격_매수_시간_우선() {
            book.addOrder(buy(1L, new BigDecimal("70000"), 10)); // 먼저
            book.addOrder(buy(2L, new BigDecimal("70000"), 10)); // 나중
            book.addOrder(sell(3L, new BigDecimal("70000"), 10));

            FillResult result = book.match().orElseThrow();

            assertThat(result.buyOrderId()).isEqualTo(1L);
        }

        @Test
        @DisplayName("같은 가격의 매도 주문은 먼저 등록된 순서로 체결")
        void 같은_가격_매도_시간_우선() {
            book.addOrder(sell(1L, new BigDecimal("70000"), 10)); // 먼저
            book.addOrder(sell(2L, new BigDecimal("70000"), 10)); // 나중
            book.addOrder(buy(3L, new BigDecimal("70000"), 10));

            FillResult result = book.match().orElseThrow();

            assertThat(result.sellOrderId()).isEqualTo(1L);
        }

        @Test
        @DisplayName("더 높은 매수가가 더 낮은 매수가보다 먼저 체결")
        void 높은_매수가_우선_체결() {
            book.addOrder(buy(1L, new BigDecimal("69000"), 10)); // 낮은 매수가
            book.addOrder(buy(2L, new BigDecimal("70000"), 10)); // 높은 매수가
            book.addOrder(sell(3L, new BigDecimal("69000"), 10));

            FillResult result = book.match().orElseThrow();

            assertThat(result.buyOrderId()).isEqualTo(2L); // 70000짜리가 먼저 체결
        }
    }

    // ── containsOrder ────────────────────────────────────────────────────────

    @Nested
    @DisplayName("containsOrder — 멱등성 체크")
    class ContainsOrder {

        @Test
        @DisplayName("addOrder 후 containsOrder true")
        void addOrder_후_containsOrder_true() {
            book.addOrder(buy(1L, new BigDecimal("70000"), 10));

            assertThat(book.containsOrder(1L)).isTrue();
        }

        @Test
        @DisplayName("등록하지 않은 주문은 containsOrder false")
        void 미등록_주문_containsOrder_false() {
            assertThat(book.containsOrder(999L)).isFalse();
        }

        @Test
        @DisplayName("cancelOrder 후 containsOrder false — orderIndex에서 제거됨")
        void cancelOrder_후_containsOrder_false() {
            book.addOrder(buy(1L, new BigDecimal("70000"), 10));
            book.cancelOrder(1L);

            assertThat(book.containsOrder(1L)).isFalse();
        }

        @Test
        @DisplayName("전량 체결 후 containsOrder true — filledOrderIds로 멱등성 유지")
        void 전량체결_후_containsOrder_true() {
            book.addOrder(buy(1L, new BigDecimal("70000"), 10));
            book.addOrder(sell(2L, new BigDecimal("70000"), 10));
            book.match();

            assertThat(book.containsOrder(1L)).isTrue();
            assertThat(book.containsOrder(2L)).isTrue();
        }
    }

    // ── FillResult 검증 ──────────────────────────────────────────────────────

    @Nested
    @DisplayName("FillResult 필드 검증")
    class FillResultFields {

        @Test
        @DisplayName("FillResult에 매수·매도 주문 ID와 계좌 ID가 올바르게 담김")
        void fillResult_필드_정확성() {
            OrderEntry buyOrder  = new OrderEntry(1L, 100L, "005930", OrderSide.BUY,  new BigDecimal("70000"), 10, Instant.now());
            OrderEntry sellOrder = new OrderEntry(2L, 200L, "005930", OrderSide.SELL, new BigDecimal("70000"), 10, Instant.now());
            book.addOrder(buyOrder);
            book.addOrder(sellOrder);

            FillResult result = book.match().orElseThrow();

            assertThat(result.buyOrderId()).isEqualTo(1L);
            assertThat(result.buyAccountId()).isEqualTo(100L);
            assertThat(result.sellOrderId()).isEqualTo(2L);
            assertThat(result.sellAccountId()).isEqualTo(200L);
            assertThat(result.filledQuantity()).isEqualTo(10);
            assertThat(result.matchPrice()).isEqualByComparingTo("70000");
        }
    }
}
