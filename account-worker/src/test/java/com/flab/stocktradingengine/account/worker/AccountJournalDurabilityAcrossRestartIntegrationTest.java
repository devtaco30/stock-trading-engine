package com.flab.stocktradingengine.account.worker;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

import com.flab.stocktradingengine.account.disruptor.engine.AccountEngine;
import com.flab.stocktradingengine.account.worker.config.AccountJournalArchiveConfig;
import com.flab.stocktradingengine.account.worker.config.AccountOrderIntakeConfig;

import io.aeron.archive.client.AeronArchive;

/**
 * 2b-1b 핵심 — 계좌 워커가 재시작해도 이전 저널 녹화가 Archive 카탈로그에 남아있는지 확인한다.
 *
 * <p>{@code account.worker.archive-dir}를 고정 경로(이 테스트 메서드 동안만 사는 {@code @TempDir})로
 * 줘서, 서로 다른 두 Spring 컨텍스트(재시작을 흉내)가 같은 Archive 카탈로그를 공유하게 한다.
 * {@code deleteArchiveOnStart(false)}(항상 이 값 — {@link AccountOrderIntakeConfig}) 덕에 둘째
 * 컨텍스트가 카탈로그를 지우지 않고 그대로 이어받아야 한다.</p>
 *
 * <p>다른 테스트는 이 프로퍼티를 안 줘서 매번 새 임시 디렉터리를 쓰므로(격리) 이 테스트와 겹치지
 * 않는다.</p>
 */
class AccountJournalDurabilityAcrossRestartIntegrationTest {

    private static final String STOCK = "005930";
    private static final long TIMEOUT_NANOS = 5_000_000_000L;
    private static final long NOT_FOUND = -1L;

    @TempDir
    private Path archiveDir;

    @Test
    void 재시작해도_이전_저널_녹화가_카탈로그에_남는다() {
        long firstRunRecordingId;
        ConfigurableApplicationContext firstRun = launch();
        try {
            AeronArchive firstArchive = firstRun.getBean(AeronArchive.class);
            AccountEngine firstEngine = firstRun.getBean(AccountEngine.class);

            firstRunRecordingId = awaitRecordingId(firstArchive);
            firstEngine.publishBuy(1L, STOCK, new BigDecimal("10000"), 10, "durability-r1");
            awaitPositionAdvance(firstArchive, firstRunRecordingId, 0L);
        } finally {
            firstRun.close();
        }

        ConfigurableApplicationContext secondRun = launch();
        try {
            AeronArchive secondArchive = secondRun.getBean(AeronArchive.class);

            RecordingDescriptor descriptor = awaitDescriptor(secondArchive, firstRunRecordingId);
            assertThat(descriptor)
                .as("첫 컨텍스트가 남긴 recordingId(%d)가 둘째 컨텍스트의 카탈로그에도 있어야 한다", firstRunRecordingId)
                .isNotNull();
            assertThat(descriptor.stopPosition())
                .as("이전 녹화에 실제로 쓰인 바이트(stopPosition)가 재시작 뒤에도 남아있어야 한다")
                .isGreaterThan(0L);
        } finally {
            secondRun.close();
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
        while ((recordingId = findLatestRecordingId(archive)) == NOT_FOUND) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("5초 안에 저널 스트림 녹화가 시작되지 않음");
            }
            Thread.yield();
        }
        return recordingId;
    }

    private long findLatestRecordingId(AeronArchive archive) {
        long[] found = {NOT_FOUND};
        archive.listRecordingsForUri(0, 10,
            AccountJournalArchiveConfig.JOURNAL_CHANNEL, AccountJournalArchiveConfig.JOURNAL_STREAM_ID,
            (controlSessionId, correlationId, recordingId, startTimestamp, stopTimestamp,
             startPosition, stopPosition, initialTermId, segmentFileLength, termBufferLength,
             mtuLength, sessionId, streamId, strippedChannel, originalChannel, sourceIdentity) ->
                found[0] = recordingId);
        return found[0];
    }

    private void awaitPositionAdvance(AeronArchive archive, long recordingId, long positionBefore) {
        long deadline = System.nanoTime() + TIMEOUT_NANOS;
        while (archive.getRecordingPosition(recordingId) <= positionBefore) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("5초 안에 녹화 위치가 늘지 않음");
            }
            Thread.yield();
        }
    }

    /** recordingId로 지정한 녹화의 stopPosition을 카탈로그에서 조회한다. 재시작 뒤 나타날 때까지 기다린다. */
    private RecordingDescriptor awaitDescriptor(AeronArchive archive, long recordingId) {
        long deadline = System.nanoTime() + TIMEOUT_NANOS;
        RecordingDescriptor descriptor;
        while ((descriptor = findDescriptor(archive, recordingId)) == null) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("5초 안에 재시작 뒤 카탈로그에서 이전 녹화를 못 찾음: recordingId=" + recordingId);
            }
            Thread.yield();
        }
        return descriptor;
    }

    private RecordingDescriptor findDescriptor(AeronArchive archive, long recordingId) {
        RecordingDescriptor[] found = {null};
        archive.listRecording(recordingId,
            (controlSessionId, correlationId, foundRecordingId, startTimestamp, stopTimestamp,
             startPosition, stopPosition, initialTermId, segmentFileLength, termBufferLength,
             mtuLength, sessionId, streamId, strippedChannel, originalChannel, sourceIdentity) ->
                found[0] = new RecordingDescriptor(stopPosition));
        return found[0];
    }

    private record RecordingDescriptor(long stopPosition) {
    }
}
