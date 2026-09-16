package com.flab.stocktradingengine.account.worker.recovery;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.net.DatagramSocket;
import java.nio.ByteBuffer;
import java.nio.file.Files;

import org.agrona.CloseHelper;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.aeron.Aeron;
import io.aeron.CommonContext;
import io.aeron.ExclusivePublication;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchivingMediaDriver;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.driver.MediaDriver;

/**
 * ADR-032 I5 U1 — 저널 recording의 오래된 Archive 세그먼트를 실제로 회수하는지 검증한다(LLD
 * `_i5_lld.md` 5절). 기본 세그먼트 길이(128MB)로는 테스트 데이터로 세그먼트 경계를 못 넘어
 * "지울 게 없어 통과"하는 거짓 green이 나므로(handoff 6-2), 세그먼트·term 길이를 Aeron 최소값
 * (64KB, {@code LogBufferDescriptor.TERM_MIN_LENGTH})으로 줄여 세그먼트 여러 개를 실제로 만든다.
 */
class AccountArchiveSegmentPurgerTest {

    private static final int TERM_BUFFER_LENGTH = 64 * 1024; // Aeron 허용 최소 term 길이
    private static final int SEGMENT_FILE_LENGTH = TERM_BUFFER_LENGTH; // 세그먼트 1개 = term 1개
    private static final String CHANNEL = "aeron:ipc?term-length=" + TERM_BUFFER_LENGTH;
    private static final int STREAM_ID = 9101;
    private static final long TIMEOUT_NANOS = 5_000_000_000L;

    private String controlRequestChannel;
    private File archiveDir;
    private ArchivingMediaDriver archivingMediaDriver;
    private Aeron aeron;
    private AeronArchive testArchive; // 테스트가 recording 시작·카탈로그 조회에 쓰는 별도 연결
    private ExclusivePublication publication;
    private long recordingId;

    @BeforeEach
    void setUp() throws IOException {
        archiveDir = Files.createTempDirectory("account-archive-purger-test-").toFile();
        String aeronDirectoryName = CommonContext.generateRandomDirName();
        int controlPort;
        try (DatagramSocket socket = new DatagramSocket(0)) {
            controlPort = socket.getLocalPort();
        }
        controlRequestChannel = "aeron:udp?endpoint=localhost:" + controlPort;

        archivingMediaDriver = ArchivingMediaDriver.launch(
            new MediaDriver.Context().aeronDirectoryName(aeronDirectoryName),
            new Archive.Context()
                .controlChannel(controlRequestChannel)
                .replicationChannel("aeron:udp?endpoint=localhost:0")
                .archiveDir(archiveDir)
                .segmentFileLength(SEGMENT_FILE_LENGTH)
                .deleteArchiveOnStart(true));

        aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDirectoryName));
        testArchive = AeronArchive.connect(new AeronArchive.Context()
            .aeron(aeron)
            .ownsAeronClient(false)
            .controlRequestChannel(controlRequestChannel)
            .controlResponseChannel("aeron:udp?endpoint=localhost:0"));

        testArchive.startRecording(CHANNEL, STREAM_ID, SourceLocation.LOCAL);
        publication = aeron.addExclusivePublication(CHANNEL, STREAM_ID);
        awaitConnected(publication);
        recordingId = awaitRecordingId();
    }

    @AfterEach
    void tearDown() {
        CloseHelper.quietClose(publication);
        CloseHelper.quietClose(testArchive);
        CloseHelper.quietClose(aeron);
        CloseHelper.quietClose(archivingMediaDriver);
    }

    @Test
    void 첫_회차는_아무것도_지우지_않고_두번째_회차부터_직전_경계까지_지운다() {
        long positionAfterBatch1 = publishUntilAdvanced(4); // 세그먼트 4개 분량

        AccountArchiveSegmentPurger purger = new AccountArchiveSegmentPurger(aeron, controlRequestChannel, recordingId);
        purger.start();
        try {
            int filesBeforeAnyPurge = countSegmentFiles();
            assertThat(filesBeforeAnyPurge).as("세그먼트 4개 분량을 발행했으니 최소 3개 이상은 파일로 남아야 한다").isGreaterThanOrEqualTo(3);

            purger.purgeUpTo(positionAfterBatch1);
            assertThat(countSegmentFiles()).as("첫 회차는 직전 경계가 없어 아무것도 지우지 않는다").isEqualTo(filesBeforeAnyPurge);

            publishUntilAdvanced(4);
            int filesBeforeSecondPurge = countSegmentFiles();
            long positionAfterBatch2 = testArchive.getRecordingPosition(recordingId);
            purger.purgeUpTo(positionAfterBatch2);

            int filesAfterSecondPurge = countSegmentFiles();
            assertThat(filesAfterSecondPurge)
                .as("두 번째 회차는 직전 경계(positionAfterBatch1)까지 지워야 한다 — 두 번째 발행 직후(회수 전) 파일 수보다 줄어야 한다")
                .isLessThan(filesBeforeSecondPurge);
        } finally {
            purger.close();
        }
    }

    @Test
    void purgeUpTo를_부르지_않으면_세그먼트_파일이_줄지_않는다() {
        publishUntilAdvanced(4);
        int filesAfterBatch1 = countSegmentFiles();

        publishUntilAdvanced(4);
        int filesAfterBatch2 = countSegmentFiles();

        assertThat(filesAfterBatch2).as("회수를 부르지 않으면 세그먼트는 계속 쌓여야 한다").isGreaterThan(filesAfterBatch1);
    }

    /** 최소 {@code segments}개의 세그먼트 경계를 넘길 만큼 발행하고, 발행 후 recording 위치를 돌려준다. */
    private long publishUntilAdvanced(int segments) {
        byte[] payload = new byte[512];
        UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(payload.length));
        long targetAdvance = (long) SEGMENT_FILE_LENGTH * segments;
        long deadline = System.nanoTime() + TIMEOUT_NANOS;
        long startPosition = testArchive.getRecordingPosition(recordingId);

        while (testArchive.getRecordingPosition(recordingId) - startPosition < targetAdvance) {
            long result = publication.offer(buffer, 0, payload.length);
            if (result < 0) {
                Thread.yield();
            }
            if (System.nanoTime() > deadline) {
                throw new AssertionError("5초 안에 목표 위치까지 발행하지 못함");
            }
        }
        return testArchive.getRecordingPosition(recordingId);
    }

    private int countSegmentFiles() {
        String[] files = archiveDir.list((dir, name) -> name.startsWith(recordingId + "-") && name.endsWith(".rec"));
        return files == null ? 0 : files.length;
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

    private long awaitRecordingId() {
        long deadline = System.nanoTime() + TIMEOUT_NANOS;
        long[] found = {-1L};
        while (found[0] < 0) {
            testArchive.listRecordingsForUri(0, 10, CHANNEL, STREAM_ID,
                (controlSessionId, correlationId, recordingId, startTimestamp, stopTimestamp,
                 startPosition, stopPosition, initialTermId, segmentFileLength, termBufferLength,
                 mtuLength, sessionId, streamId, strippedChannel, originalChannel, sourceIdentity) ->
                    found[0] = recordingId);
            if (found[0] < 0) {
                if (System.nanoTime() > deadline) {
                    throw new AssertionError("5초 안에 recording이 카탈로그에 나타나지 않음");
                }
                Thread.yield();
            }
        }
        return found[0];
    }
}
