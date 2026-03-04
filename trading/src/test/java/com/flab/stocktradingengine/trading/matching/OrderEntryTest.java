package com.flab.stocktradingengine.trading.matching;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.time.Instant;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import com.flab.stocktradingengine.trading.entity.OrderSide;

@DisplayName("OrderEntry 단위 테스트")
class OrderEntryTest {

    private static final int QUANTITY = 100;
    private OrderEntry entry;

    @BeforeEach
    void setUp() {
        entry = new OrderEntry(1L, 100L, "005930", OrderSide.BUY,
            new BigDecimal("70000"), QUANTITY, Instant.now());
    }

    @Nested
    @DisplayName("getRemainingQuantity")
    class GetRemainingQuantity {

        @Test
        @DisplayName("체결 전 잔량은 전체 수량")
        void 체결전_잔량은_전체수량() {
            assertThat(entry.getRemainingQuantity()).isEqualTo(QUANTITY);
        }

        @Test
        @DisplayName("부분 체결 후 잔량 = 전체 - 체결")
        void 부분체결_후_잔량() {
            entry.addFilled(30);

            assertThat(entry.getRemainingQuantity()).isEqualTo(70);
        }

        @Test
        @DisplayName("전량 체결 후 잔량은 0")
        void 전량체결_후_잔량은_0() {
            entry.addFilled(QUANTITY);

            assertThat(entry.getRemainingQuantity()).isEqualTo(0);
        }
    }

    @Nested
    @DisplayName("addFilled")
    class AddFilled {

        @Test
        @DisplayName("addFilled 누적 호출 시 합산")
        void addFilled_누적() {
            entry.addFilled(30);
            entry.addFilled(40);

            assertThat(entry.getFilledQuantity()).isEqualTo(70);
            assertThat(entry.getRemainingQuantity()).isEqualTo(30);
        }
    }

    @Nested
    @DisplayName("isFullyFilled")
    class IsFullyFilled {

        @Test
        @DisplayName("체결 전에는 false")
        void 체결전_false() {
            assertThat(entry.isFullyFilled()).isFalse();
        }

        @Test
        @DisplayName("부분 체결 후에도 false")
        void 부분체결_후_false() {
            entry.addFilled(QUANTITY - 1);

            assertThat(entry.isFullyFilled()).isFalse();
        }

        @Test
        @DisplayName("전량 체결 후 true")
        void 전량체결_후_true() {
            entry.addFilled(QUANTITY);

            assertThat(entry.isFullyFilled()).isTrue();
        }
    }

    @Nested
    @DisplayName("cancel")
    class Cancel {

        @Test
        @DisplayName("취소 전에는 cancelled = false")
        void 취소전_false() {
            assertThat(entry.isCancelled()).isFalse();
        }

        @Test
        @DisplayName("cancel() 호출 후 cancelled = true")
        void 취소후_true() {
            entry.cancel();

            assertThat(entry.isCancelled()).isTrue();
        }

        @Test
        @DisplayName("cancel() 은 멱등 - 두 번 호출해도 true 유지")
        void 이중취소_멱등() {
            entry.cancel();
            entry.cancel();

            assertThat(entry.isCancelled()).isTrue();
        }
    }
}
