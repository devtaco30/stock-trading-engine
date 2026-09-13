package com.flab.stocktradingengine.account.worker;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.util.List;

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

import com.flab.stocktradingengine.account.disruptor.AccountEngine;
import com.flab.stocktradingengine.codec.AccountJournalEntry;

import io.aeron.ExclusivePublication;
import io.aeron.archive.client.AeronArchive;

/**
 * U2 — 저널에 손상 바이트(잘못된 type ordinal)가 섞여 있어도 리플레이가 startup을 abort시키지
 * 않고 정상 엔트리만 복구하는지 확인한다. {@link AccountJournalReplayerPositionIntegrationTest}와
 * 같은 결(실 Archive 녹화·replay) — 정상 API로는 poison을 만들 수 없어(핸들러가 유효한 값만
 * 인코딩) 저널 발행 스트림에 {@link ExclusivePublication}으로 손상 프레임을 직접 offer한다.
 */
class AccountJournalReplayerPoisonIntegrationTest {

    private static final String STOCK = "005930";
    private static final long TIMEOUT_NANOS = 5_000_000_000L;

    @TempDir
    private Path archiveDir;

    @Test
    void 저널에_손상_type_바이트가_섞여도_replay가_예외없이_끝나고_정상엔트리만_복구한다() {
        ConfigurableApplicationContext run1 = launch();
        try {
            AccountEngine engine = run1.getBean(AccountEngine.class);
            ExclusivePublication publication = run1.getBean(ExclusivePublication.class);

            engine.publishBuy(1L, STOCK, new BigDecimal("10000"), 10, "r1");
            awaitOrderId(engine, "r1");

            offerPoisonFrame(publication);

            engine.publishBuy(1L, STOCK, new BigDecimal("9000"), 5, "r2");
            awaitOrderId(engine, "r2");
        } finally {
            run1.close(); // 녹화를 멈춰 stopPosition을 확정 짓는다
        }

        ConfigurableApplicationContext run2 = launch();
        try {
            AeronArchive archive = run2.getBean(AeronArchive.class);
            AccountJournalReplayer replayer = new AccountJournalReplayer(archive);

            // 손상 엔트리 하나가 여기서 예외를 던지면(red) 테스트 자체가 실패한다 — try-catch로
            // 감싸지 않는다. 이 호출이 예외 없이 끝나는 것 자체가 U2의 검증 대상이다.
            List<AccountJournalEntry> entries = replayer.readAll(
                AccountJournalArchiveConfig.JOURNAL_CHANNEL, AccountJournalArchiveConfig.JOURNAL_STREAM_ID);

            assertThat(entries)
                .as("손상 엔트리는 skip되고 정상 엔트리(r1·r2)만 남아야 한다")
                .extracting(AccountJournalEntry::requestId)
                .containsExactly("r1", "r2");
        } finally {
            run2.close();
        }
    }

    /** type ordinal이 AccountEventType 범위(0~4)를 벗어난 손상 프레임 하나를 저널 발행 스트림에 직접 심는다. */
    private void offerPoisonFrame(ExclusivePublication publication) {
        UnsafeBuffer buffer = new UnsafeBuffer(new byte[16]);
        buffer.putByte(0, (byte) 99);
        long deadline = System.nanoTime() + TIMEOUT_NANOS;
        while (publication.offer(buffer, 0, buffer.capacity()) < 0) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("5초 안에 손상 프레임을 발행하지 못함");
            }
            Thread.yield();
        }
    }

    private ConfigurableApplicationContext launch() {
        return new SpringApplicationBuilder(AccountWorkerApplication.class)
            .web(WebApplicationType.NONE)
            .properties(
                "account-worker.seed-accounts[0].account-id=1",
                "account-worker.seed-accounts[0].balance=1000000",
                "account-worker.seed-accounts[0].margin-rate=0.40",
                "account.worker.archive-dir=" + archiveDir.toAbsolutePath())
            .run();
    }

    private void awaitOrderId(AccountEngine engine, String requestId) {
        long deadline = System.nanoTime() + TIMEOUT_NANOS;
        while (engine.accountState(1L).orderIdFor(requestId) == null) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("5초 안에 " + requestId + "의 orderId가 발급되지 않음");
            }
            Thread.yield();
        }
    }
}
