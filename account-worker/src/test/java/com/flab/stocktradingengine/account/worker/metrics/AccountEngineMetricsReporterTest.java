package com.flab.stocktradingengine.account.worker.metrics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;

import org.agrona.DirectBuffer;
import org.junit.jupiter.api.Test;

import com.flab.stocktradingengine.account.disruptor.domain.NoOpAccountResultListener;
import com.flab.stocktradingengine.account.disruptor.engine.AccountEngine;
import com.flab.stocktradingengine.account.disruptor.journal.AeronArchiveAccountJournal;
import com.flab.stocktradingengine.codec.AccountEventType;
import com.flab.stocktradingengine.codec.AccountJournalEntry;

import io.aeron.ExclusivePublication;
import io.aeron.Publication;

/** 12-3절 2번 표의 3단계(계좌 ring buffer)와 4단계(계좌 저널 offer)를 1초마다 로그 한 줄로 내는 보고기. */
class AccountEngineMetricsReporterTest {

    @Test
    void 이전_값과의_차이와_현재_남은_칸을_한_줄로_만든다() {
        AccountEngineMetrics previous = new AccountEngineMetrics(1024L, 2L, 10L, 4L);
        AccountEngineMetrics current = new AccountEngineMetrics(0L, 5L, 25L, 4L);

        String line = AccountEngineMetricsReporter.formatLine(previous, current);

        assertThat(line).isEqualTo("account-engine ringRemaining=0 ringFullWaits=3 journalBackPressure=15 journalOtherRetries=0");
    }

    @Test
    void 엔진이_시작되기_전이면_남은_칸이_알_수_없음_음수로_나온다() {
        AccountEngine engine = new AccountEngine(1024, 1L, (orderId, accountId, stockCode, side, price, quantity) -> { },
            NoOpAccountResultListener.INSTANCE);

        AccountEngineMetrics metrics = AccountEngineMetricsReporter.read(engine);

        assertThat(metrics.ringRemaining()).isEqualTo(-1L);
        assertThat(metrics.ringFullWaits()).isZero();
        assertThat(metrics.journalBackPressure()).as("Aeron 저널이 아니면 저널 재시도는 0이다").isZero();
        assertThat(metrics.journalOtherRetries()).isZero();
    }

    @Test
    void Aeron_저널을_쓰면_저널_재시도를_종류별로_읽는다() {
        ExclusivePublication publication = mock(ExclusivePublication.class);
        when(publication.offer(any(DirectBuffer.class), anyInt(), anyInt()))
            .thenReturn(Publication.BACK_PRESSURED, Publication.ADMIN_ACTION, 100L);
        AeronArchiveAccountJournal journal = new AeronArchiveAccountJournal(publication);
        journal.append(new AccountJournalEntry(AccountEventType.SELL, 1L, 90002L, "005930", new BigDecimal("10000"), 1, "r1", 0L));
        AccountEngine engine = new AccountEngine(1024, new com.lmax.disruptor.BlockingWaitStrategy(),
            com.lmax.disruptor.dsl.ProducerType.SINGLE, 1L, (orderId, accountId, stockCode, side, price, quantity) -> { },
            NoOpAccountResultListener.INSTANCE, journal);

        AccountEngineMetrics metrics = AccountEngineMetricsReporter.read(engine);

        assertThat(metrics.journalBackPressure()).isEqualTo(1L);
        assertThat(metrics.journalOtherRetries()).isEqualTo(1L);
    }
}
