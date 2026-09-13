package com.flab.stocktradingengine.settlement.worker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;

import org.junit.jupiter.api.Test;

import com.flab.stocktradingengine.settlement.worker.entity.PendingSettlement;
import com.flab.stocktradingengine.settlement.worker.entity.SettlementStatus;

class PendingSettlementTest {

    @Test
    void markSettled_호출하면_SETTLED로_바뀐다() {
        PendingSettlement pendingSettlement = new PendingSettlement(9001L, 1L, new BigDecimal("60000"), 123L);

        pendingSettlement.markSettled();

        assertThat(pendingSettlement.getStatus()).isEqualTo(SettlementStatus.SETTLED);
    }

    @Test
    void 이미_SETTLED인_건에_markSettled_호출하면_예외를_던진다() {
        PendingSettlement pendingSettlement = new PendingSettlement(9001L, 1L, new BigDecimal("60000"), 123L);
        pendingSettlement.markSettled();

        assertThatThrownBy(pendingSettlement::markSettled).isInstanceOf(IllegalStateException.class);
    }
}
