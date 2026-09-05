package com.flab.stocktradingengine.account.entity;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("Holding - 보유 수량 차감 가드")
class HoldingTest {

    private Holding holding(int quantity) {
        return new Holding(null, "005930", quantity, new BigDecimal("70000"));
    }

    @Test
    @DisplayName("보유 범위 내 차감은 정상 반영")
    void subtractsWithinQuantity() {
        Holding holding = holding(100);

        holding.subtractQuantity(30);

        assertThat(holding.getQuantity()).isEqualTo(70);
    }

    @Test
    @DisplayName("전량 차감(경계)은 0으로 반영")
    void subtractsAll() {
        Holding holding = holding(100);

        holding.subtractQuantity(100);

        assertThat(holding.getQuantity()).isZero();
    }

    @Test
    @DisplayName("보유보다 많이 차감하면 예외 — 음수 보유 저장 차단")
    void rejectsOverSubtraction() {
        Holding holding = holding(100);

        assertThatThrownBy(() -> holding.subtractQuantity(101))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("보유 수량 초과 차감");
        assertThat(holding.getQuantity()).isEqualTo(100);
    }
}
