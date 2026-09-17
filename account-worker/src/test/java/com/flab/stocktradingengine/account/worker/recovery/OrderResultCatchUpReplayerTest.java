package com.flab.stocktradingengine.account.worker.recovery;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.net.DatagramSocket;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.List;

import org.agrona.CloseHelper;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.flab.stocktradingengine.aeron.AeronStreamIds;
import com.flab.stocktradingengine.codec.OrderResultCodec;
import com.flab.stocktradingengine.codec.OrderResultEntry;
import com.flab.stocktradingengine.codec.OrderVerdict;

import io.aeron.Aeron;
import io.aeron.CommonContext;
import io.aeron.ExclusivePublication;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchivingMediaDriver;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.driver.MediaDriver;

/**
 * U3-b — {@link OrderResultCatchUpReplayer}가 과거(이미 멈춘) recording만 읽고, 저장된 position
 * 이전 엔트리는 건너뛰며, 아직 stop되지 않은(진행 중인) recording은 자동으로 제외하는지 검증한다.
 * {@code AccountJournalReplayerPositionIntegrationTest}와 같은 결(run을 나눠 recording을
 * 확정 짓는다) — 다만 계좌 엔진 전체를 띄우지 않고 Aeron Archive만 직접 연다.
 */
class OrderResultCatchUpReplayerTest {

    private static final String CHANNEL = "aeron:ipc";
    private static final int STREAM_ID = AeronStreamIds.ORDER_RESULT;
    private static final long TIMEOUT_NANOS = 5_000_000_000L;
    private static final OrderResultCodec CODEC = new OrderResultCodec();

    private File archiveDir;
    private Run run;

    @AfterEach
    void tearDown() {
        if (run != null) {
            run.closeAll();
        }
    }

    @Test
    void readAll은_멈춘_recording만_시작시각_순서로_읽고_진행중인_recording은_제외한다() throws IOException {
        archiveDir = Files.createTempDirectory("order-result-catchup-test-").toFile();

        run = launch(true); // runA — 새 archiveDir
        long recordingIdA = run.recordingId;
        offer(run.publication, entry(1L, "r1"));
        long positionAfterFirst = awaitPositionAdvance(run.archive, recordingIdA, 0L);
        offer(run.publication, entry(2L, "r2"));
        awaitPositionAdvance(run.archive, recordingIdA, positionAfterFirst);
        run.closeAll(); // recording A를 멈춰 stopPosition을 확정 짓는다

        run = launch(false); // runB — 같은 archiveDir, 새 recording B(아직 stop 안 함)
        offer(run.publication, entry(3L, "r3"));
        awaitPositionAdvance(run.archive, run.recordingId, 0L);

        OrderResultCatchUpReplayer replayer = new OrderResultCatchUpReplayer(run.archive);
        List<OrderResultEntry> entries = replayer.readAll(CHANNEL, STREAM_ID);

        assertThat(entries)
            .as("진행 중인 recording B(r3)는 제외되고 멈춘 recording A(r1·r2)만 나와야 한다")
            .extracting(OrderResultEntry::requestId)
            .containsExactly("r1", "r2");
    }

    @Test
    void readFrom은_저장된_position_이전_엔트리는_건너뛰고_그_뒤와_다음_recording을_읽는다() throws IOException {
        archiveDir = Files.createTempDirectory("order-result-catchup-test-").toFile();

        run = launch(true); // runA
        long recordingIdA = run.recordingId;
        offer(run.publication, entry(1L, "r1"));
        long positionAfterFirst = awaitPositionAdvance(run.archive, recordingIdA, 0L);
        offer(run.publication, entry(2L, "r2"));
        awaitPositionAdvance(run.archive, recordingIdA, positionAfterFirst);
        run.closeAll();

        run = launch(false); // runB
        offer(run.publication, entry(3L, "r3"));
        awaitPositionAdvance(run.archive, run.recordingId, 0L);
        run.closeAll(); // recording B도 멈춰 stopPosition을 확정 짓는다

        run = launch(false); // runC — 읽기 전용(새 recording 시작 안 함)
        OrderResultCatchUpReplayer replayer = new OrderResultCatchUpReplayer(run.archive);

        List<OrderResultEntry> fromFirst = replayer.readFrom(CHANNEL, STREAM_ID, recordingIdA, positionAfterFirst);

        assertThat(fromFirst)
            .as("recording A의 첫 엔트리 이후 position부터 읽으면 r1은 빠지고, 그 뒤 recording B의 r3까지 나와야 한다")
            .extracting(OrderResultEntry::requestId)
            .containsExactly("r2", "r3");
    }

    private OrderResultEntry entry(long accountId, String requestId) {
        return new OrderResultEntry(accountId, 0L, requestId, OrderVerdict.ACCEPTED, null, 1_000L);
    }

    private void offer(ExclusivePublication publication, OrderResultEntry entry) {
        UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(256));
        int length = CODEC.encode(buffer, 0, entry);
        long deadline = System.nanoTime() + TIMEOUT_NANOS;
        long result;
        do {
            result = publication.offer(buffer, 0, length);
            if (result > 0) {
                return;
            }
            if (System.nanoTime() > deadline) {
                throw new AssertionError("5초 안에 엔트리 발행 실패 — offer 반환=" + result);
            }
            Thread.yield();
        } while (true);
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

    private Run launch(boolean firstLaunch) throws IOException {
        String aeronDirectoryName = CommonContext.generateRandomDirName();
        String controlRequestChannel;
        try (DatagramSocket socket = new DatagramSocket(0)) {
            controlRequestChannel = "aeron:udp?endpoint=localhost:" + socket.getLocalPort();
        }

        ArchivingMediaDriver archivingMediaDriver = ArchivingMediaDriver.launch(
            new MediaDriver.Context().aeronDirectoryName(aeronDirectoryName)
                .dirDeleteOnStart(true)
                .dirDeleteOnShutdown(true),
            new Archive.Context()
                .controlChannel(controlRequestChannel)
                .replicationChannel("aeron:udp?endpoint=localhost:0")
                .archiveDir(archiveDir)
                .deleteArchiveOnStart(firstLaunch));

        Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDirectoryName));
        AeronArchive archive = AeronArchive.connect(new AeronArchive.Context()
            .aeron(aeron)
            .ownsAeronClient(false)
            .controlRequestChannel(controlRequestChannel)
            .controlResponseChannel("aeron:udp?endpoint=localhost:0"));

        archive.startRecording(CHANNEL, STREAM_ID, SourceLocation.LOCAL);
        ExclusivePublication publication = aeron.addExclusivePublication(CHANNEL, STREAM_ID);
        awaitConnected(publication);
        long recordingId = awaitRecordingId(archive);

        return new Run(archivingMediaDriver, aeron, archive, publication, recordingId);
    }

    private void awaitConnected(ExclusivePublication publication) {
        long deadline = System.nanoTime() + TIMEOUT_NANOS;
        while (!publication.isConnected()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("5초 안에 Publication이 연결되지 않음");
            }
            Thread.yield();
        }
    }

    private long awaitRecordingId(AeronArchive archive) {
        long deadline = System.nanoTime() + TIMEOUT_NANOS;
        long[] found = {-1L};
        long[] latestStartTimestamp = {Long.MIN_VALUE};
        while (found[0] < 0) {
            found[0] = -1L;
            latestStartTimestamp[0] = Long.MIN_VALUE;
            archive.listRecordingsForUri(0, 100, CHANNEL, STREAM_ID,
                (controlSessionId, correlationId, recordingId, startTimestamp, stopTimestamp,
                 startPosition, stopPosition, initialTermId, segmentFileLength, termBufferLength,
                 mtuLength, sessionId, streamId, strippedChannel, originalChannel, sourceIdentity) -> {
                    if (startTimestamp > latestStartTimestamp[0]) {
                        latestStartTimestamp[0] = startTimestamp;
                        found[0] = recordingId;
                    }
                });
            if (found[0] < 0 && System.nanoTime() > deadline) {
                throw new AssertionError("5초 안에 recordingId를 카탈로그에서 찾지 못함");
            }
        }
        return found[0];
    }

    /** 한 번의 launch로 만들어진 리소스 묶음. */
    private static final class Run {
        private final ArchivingMediaDriver archivingMediaDriver;
        private final Aeron aeron;
        private final AeronArchive archive;
        private final ExclusivePublication publication;
        private final long recordingId;

        Run(ArchivingMediaDriver archivingMediaDriver, Aeron aeron, AeronArchive archive,
                ExclusivePublication publication, long recordingId) {
            this.archivingMediaDriver = archivingMediaDriver;
            this.aeron = aeron;
            this.archive = archive;
            this.publication = publication;
            this.recordingId = recordingId;
        }

        void closeAll() {
            CloseHelper.quietClose(publication);
            CloseHelper.quietClose(archive);
            CloseHelper.quietClose(aeron);
            CloseHelper.quietClose(archivingMediaDriver);
        }
    }
}
