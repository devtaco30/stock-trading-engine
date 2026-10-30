package com.flab.stocktradingengine.account.worker.journal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;

import org.agrona.DirectBuffer;
import org.junit.jupiter.api.Test;

import com.flab.stocktradingengine.account.disruptor.journal.AeronArchiveAccountJournal;
import com.flab.stocktradingengine.codec.AccountEventType;
import com.flab.stocktradingengine.codec.AccountJournalEntry;

import io.aeron.ExclusivePublication;
import io.aeron.Publication;

/** 12-3절 2번 표의 4단계: 계좌 저널 offer가 되돌아와 다시 시도한 횟수를 읽을 수 있다. */
class AeronArchiveAccountJournalMetricsTest {

    @Test
    void offer가_밀려_다시_시도한_횟수를_BACK_PRESSURED와_그_밖으로_나눠_센다() {
        ExclusivePublication publication = mock(ExclusivePublication.class);
        when(publication.offer(any(DirectBuffer.class), anyInt(), anyInt()))
            .thenReturn(Publication.BACK_PRESSURED, Publication.BACK_PRESSURED, Publication.ADMIN_ACTION, 100L,
                Publication.NOT_CONNECTED, 200L, 300L);
        AeronArchiveAccountJournal journal = new AeronArchiveAccountJournal(publication);
        assertThat(journal.offerBackPressureCount()).isZero();
        assertThat(journal.offerOtherRetryCount()).isZero();

        journal.append(entry(1L));
        assertThat(journal.offerBackPressureCount()).as("첫 엔트리: BACK_PRESSURED 두 번").isEqualTo(2);
        assertThat(journal.offerOtherRetryCount()).as("첫 엔트리: ADMIN_ACTION 한 번").isEqualTo(1);

        journal.append(entry(2L));
        assertThat(journal.offerBackPressureCount()).isEqualTo(2);
        assertThat(journal.offerOtherRetryCount()).as("둘째 엔트리: NOT_CONNECTED 한 번이 그 밖에 더해진다").isEqualTo(2);

        journal.append(entry(3L));
        assertThat(journal.offerBackPressureCount()).as("다시 시도하지 않은 엔트리는 어느 쪽도 늘리지 않는다").isEqualTo(2);
        assertThat(journal.offerOtherRetryCount()).isEqualTo(2);
    }

    @Test
    void 복구_불가_결과로_예외를_던지기_전까지의_재시도만_센다() {
        ExclusivePublication publication = mock(ExclusivePublication.class);
        when(publication.offer(any(DirectBuffer.class), anyInt(), anyInt()))
            .thenReturn(Publication.BACK_PRESSURED, Publication.CLOSED);
        AeronArchiveAccountJournal journal = new AeronArchiveAccountJournal(publication);

        assertThatThrownBy(() -> journal.append(entry(1L))).isInstanceOf(IllegalStateException.class);
        assertThat(journal.offerBackPressureCount()).isEqualTo(1);
        assertThat(journal.offerOtherRetryCount()).isZero();
    }

    private AccountJournalEntry entry(long orderId) {
        return new AccountJournalEntry(AccountEventType.SELL, orderId, 90002L, "005930", new BigDecimal("10000"), 1, "r-" + orderId, 0L);
    }
}
