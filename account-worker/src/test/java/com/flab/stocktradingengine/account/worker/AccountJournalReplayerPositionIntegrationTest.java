package com.flab.stocktradingengine.account.worker;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

import com.flab.stocktradingengine.account.disruptor.engine.AccountEngine;
import com.flab.stocktradingengine.account.worker.config.AccountJournalArchiveConfig;
import com.flab.stocktradingengine.account.worker.recovery.AccountJournalReplayer;
import com.flab.stocktradingengine.codec.AccountJournalEntry;

import io.aeron.archive.client.AeronArchive;

/**
 * 2d-2b 하드파트 — {@link AccountJournalReplayer#readFrom}이 실제로 지정한 position부터만 읽는지
 * (그 앞 엔트리는 재생하지 않는지)를 반환된 엔트리 자체를 세어 증명한다. matching
 * {@code MatchingJournalReplayerPositionIntegrationTest}와 같은 결.
 *
 * <p>{@link AccountSnapshotRecoveryIntegrationTest}는 "재시작 뒤 상태가 맞다"까지만 블랙박스로
 * 본다 — 계좌 반영도 멱등(같은 tradeId·requestId 재도착 무시)이라 델타 리플레이가 실제로 position
 * 부터 시작하는지, 처음부터 다시 읽고도 우연히 맞는 건지 최종 상태만으로는 구분할 수 없다. 여기서는
 * {@link AccountJournalReplayer}를 직접 불러 반환된 엔트리 자체(requestId)로 구분한다.</p>
 *
 * <h3>왜 run1/run2 두 단계인가</h3>
 * <p>Archive는 아직 멈추지 않은(같은 프로세스가 계속 쓰고 있는) 녹화의 {@code stopPosition}을
 * NULL_POSITION으로 둔다 — 그래서 같은 프로세스 안에서 자기 자신의 녹화를 바로 리플레이할 수
 * 없다. run1을 정상 종료해 녹화를 확정 짓고, 그 카탈로그를 공유하는 run2에서 리플레이한다.</p>
 */
class AccountJournalReplayerPositionIntegrationTest {

    private static final String STOCK = "005930";
    private static final long TIMEOUT_NANOS = 5_000_000_000L;

    @TempDir
    private Path archiveDir;

    @Test
    void readFrom은_지정한_position_이전_엔트리를_재생하지_않는다() {
        long recordingId;
        long positionAfterFirst;

        ConfigurableApplicationContext run1 = launch();
        try {
            AccountEngine engine = run1.getBean(AccountEngine.class);
            AeronArchive archive = run1.getBean(AeronArchive.class);

            recordingId = awaitRecordingId(archive);
            engine.publishBuy(1L, STOCK, new BigDecimal("10000"), 10, "r1");
            positionAfterFirst = awaitPositionAdvance(archive, recordingId, 0L);

            engine.publishBuy(1L, STOCK, new BigDecimal("9000"), 5, "r2");
            engine.publishBuy(1L, STOCK, new BigDecimal("8000"), 3, "r3");
            awaitOrderId(engine, "r3");
        } finally {
            run1.close(); // 녹화를 멈춰 stopPosition을 확정 짓는다
        }

        ConfigurableApplicationContext run2 = launch();
        try {
            AeronArchive archive = run2.getBean(AeronArchive.class);
            AccountJournalReplayer replayer = new AccountJournalReplayer(archive);

            List<AccountJournalEntry> all = replayer.readAll(
                AccountJournalArchiveConfig.JOURNAL_CHANNEL, AccountJournalArchiveConfig.JOURNAL_STREAM_ID);
            assertThat(all).extracting(AccountJournalEntry::requestId).containsExactly("r1", "r2", "r3");

            List<AccountJournalEntry> fromFirst = replayer.readFrom(
                AccountJournalArchiveConfig.JOURNAL_CHANNEL, AccountJournalArchiveConfig.JOURNAL_STREAM_ID,
                recordingId, positionAfterFirst);
            assertThat(fromFirst)
                .as("첫 엔트리 이후 position부터 읽으면 r1은 빠지고 r2·r3만 나와야 한다")
                .extracting(AccountJournalEntry::requestId)
                .containsExactly("r2", "r3");
        } finally {
            run2.close();
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

    private long awaitRecordingId(AeronArchive archive) {
        long deadline = System.nanoTime() + TIMEOUT_NANOS;
        long recordingId;
        while ((recordingId = findRecordingId(archive)) == -1L) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("5초 안에 저널 스트림 녹화가 시작되지 않음");
            }
            Thread.yield();
        }
        return recordingId;
    }

    private long findRecordingId(AeronArchive archive) {
        long[] found = {-1L};
        archive.listRecordingsForUri(0, 10,
            AccountJournalArchiveConfig.JOURNAL_CHANNEL, AccountJournalArchiveConfig.JOURNAL_STREAM_ID,
            (controlSessionId, correlationId, recordingId, startTimestamp, stopTimestamp,
             startPosition, stopPosition, initialTermId, segmentFileLength, termBufferLength,
             mtuLength, sessionId, streamId, strippedChannel, originalChannel, sourceIdentity) ->
                found[0] = recordingId);
        return found[0];
    }

    /** 녹화 위치가 기준값보다 늘어날 때까지 기다린 뒤, 늘어난 위치를 돌려준다. */
    private long awaitPositionAdvance(AeronArchive archive, long recordingId, long positionBefore) {
        long deadline = System.nanoTime() + TIMEOUT_NANOS;
        long position;
        while ((position = archive.getRecordingPosition(recordingId)) <= positionBefore) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("5초 안에 녹화 위치가 늘지 않음");
            }
            Thread.yield();
        }
        return position;
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
