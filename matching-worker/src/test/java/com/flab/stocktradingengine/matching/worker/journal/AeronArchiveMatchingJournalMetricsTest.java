package com.flab.stocktradingengine.matching.worker.journal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.time.Instant;

import org.agrona.DirectBuffer;
import org.junit.jupiter.api.Test;

import com.flab.stocktradingengine.codec.EventType;
import com.flab.stocktradingengine.codec.JournaledOrder;
import com.flab.stocktradingengine.matching.disruptor.journal.AeronArchiveMatchingJournal;
import com.flab.stocktradingengine.trading.entity.OrderSide;

import io.aeron.ExclusivePublication;
import io.aeron.Publication;

/** 12-3절 2번 표의 8단계: 매칭 저널 offer가 되돌아와 다시 시도한 횟수를 읽을 수 있다. */
class AeronArchiveMatchingJournalMetricsTest {

    @Test
    void offer가_밀린_횟수는_종류별로_센다() {
        // BACK_PRESSURED는 소비가 늦다는 신호다. ADMIN_ACTION(term 교체)과 NOT_CONNECTED는 소비가 밀리지 않아도 나올 수 있어 따로 센다.
        ExclusivePublication publication = mock(ExclusivePublication.class);
        when(publication.offer(any(DirectBuffer.class), anyInt(), anyInt()))
            .thenReturn(Publication.BACK_PRESSURED, Publication.BACK_PRESSURED, Publication.ADMIN_ACTION,
                Publication.NOT_CONNECTED, 100L, 200L);
        AeronArchiveMatchingJournal journal = new AeronArchiveMatchingJournal(publication);
        assertThat(journal.offerBackPressureCount()).isZero();
        assertThat(journal.offerOtherRetryCount()).isZero();

        journal.append(order(1L));
        assertThat(journal.offerBackPressureCount()).as("BACK_PRESSURED가 두 번").isEqualTo(2);
        assertThat(journal.offerOtherRetryCount()).as("ADMIN_ACTION과 NOT_CONNECTED").isEqualTo(2);

        journal.append(order(2L));
        journal.append(order(3L));
        assertThat(journal.offerBackPressureCount()).as("다시 시도하지 않은 주문은 횟수를 늘리지 않는다").isEqualTo(2);
        assertThat(journal.offerOtherRetryCount()).isEqualTo(2);
    }

    private JournaledOrder order(long orderId) {
        return new JournaledOrder(EventType.PLACE, orderId, 1_000L + orderId, "005930", OrderSide.SELL,
            new BigDecimal("10000"), 1, Instant.now());
    }
}
