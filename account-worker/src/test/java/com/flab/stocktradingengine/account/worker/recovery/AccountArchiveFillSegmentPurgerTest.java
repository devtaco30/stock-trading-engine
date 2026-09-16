package com.flab.stocktradingengine.account.worker.recovery;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.net.DatagramSocket;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.Map;

import org.agrona.CloseHelper;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.aeron.Aeron;
import io.aeron.CommonContext;
import io.aeron.Publication;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchivingMediaDriver;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.driver.MediaDriver;

/**
 * ADR-032 I5 U2 — 체결 recording들(발행자마다 따로, sessionId로 구분)의 오래된 Archive 세그먼트를
 * recording별로 각자 경계까지 회수하는지 검증한다(LLD {@code _i5_lld.md} 5절 U2). 저널(U1)은
 * {@link AccountArchiveSegmentPurgerTest} 참고 — 이 파일은 체결 경로만 본다.
 *
 * <h3>왜 발행자를 udp+별도 MediaDriver 두 개로 만드나</h3>
 * <p>{@code AccountFillMultiSessionRecordingIntegrationTest}(I1)가 이미 실측한 대로, 같은 드라이버
 * 안에서 같은 IPC 채널·스트림에 발행을 두 번 하면 세션이 공유돼(참조 카운트) "발행자가 다르다"는
 * 전제가 깨진다. udp+별도 드라이버는 실제 크로스프로세스 상황(매칭 프로세스 둘)을 흉내내면서
 * 세션이 확실히 분리된다.</p>
 */
class AccountArchiveFillSegmentPurgerTest {

    private static final int TERM_BUFFER_LENGTH = 64 * 1024; // Aeron 허용 최소 term 길이
    private static final int SEGMENT_FILE_LENGTH = TERM_BUFFER_LENGTH; // 세그먼트 1개 = term 1개
    private static final int FILL_STREAM_ID = 9102;
    private static final long TIMEOUT_NANOS = 5_000_000_000L;

    private String controlRequestChannel;
    private String fillChannel;
    private File archiveDir;
    private ArchivingMediaDriver archivingMediaDriver;
    private Aeron accountAeron; // 계좌 쪽(테스트가 회수 대상 카탈로그를 조회하는 연결)
    private AeronArchive testArchive;
    private MediaDriver matchingDriverA;
    private MediaDriver matchingDriverB;
    private Aeron matchingAeronA;
    private Aeron matchingAeronB;
    private Publication publicationA;
    private Publication publicationB;
    private long recordingIdA;
    private long recordingIdB;

    @BeforeEach
    void setUp() throws IOException {
        archiveDir = Files.createTempDirectory("account-archive-fill-purger-test-").toFile();
        String accountAeronDirectoryName = CommonContext.generateRandomDirName();
        int controlPort;
        try (DatagramSocket socket = new DatagramSocket(0)) {
            controlPort = socket.getLocalPort();
        }
        controlRequestChannel = "aeron:udp?endpoint=localhost:" + controlPort;
        int fillPort;
        try (DatagramSocket socket = new DatagramSocket(0)) {
            fillPort = socket.getLocalPort();
        }
        fillChannel = "aeron:udp?endpoint=localhost:" + fillPort;

        archivingMediaDriver = ArchivingMediaDriver.launch(
            new MediaDriver.Context().aeronDirectoryName(accountAeronDirectoryName)
                .dirDeleteOnStart(true)
                .dirDeleteOnShutdown(true),
            new Archive.Context()
                .controlChannel(controlRequestChannel)
                .replicationChannel("aeron:udp?endpoint=localhost:0")
                .archiveDir(archiveDir)
                .segmentFileLength(SEGMENT_FILE_LENGTH)
                .deleteArchiveOnStart(true));

        accountAeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(accountAeronDirectoryName));
        testArchive = AeronArchive.connect(new AeronArchive.Context()
            .aeron(accountAeron)
            .ownsAeronClient(false)
            .controlRequestChannel(controlRequestChannel)
            .controlResponseChannel("aeron:udp?endpoint=localhost:0"));
        testArchive.startRecording(fillChannel, FILL_STREAM_ID, SourceLocation.REMOTE);

        matchingDriverA = MediaDriver.launchEmbedded(cleanEmbeddedMediaDriverContext());
        matchingDriverB = MediaDriver.launchEmbedded(cleanEmbeddedMediaDriverContext());
        matchingAeronA = Aeron.connect(new Aeron.Context().aeronDirectoryName(matchingDriverA.aeronDirectoryName()));
        matchingAeronB = Aeron.connect(new Aeron.Context().aeronDirectoryName(matchingDriverB.aeronDirectoryName()));
        publicationA = matchingAeronA.addPublication(
            fillChannel + "|term-length=" + TERM_BUFFER_LENGTH, FILL_STREAM_ID);
        publicationB = matchingAeronB.addPublication(
            fillChannel + "|term-length=" + TERM_BUFFER_LENGTH, FILL_STREAM_ID);
        awaitConnected(publicationA);
        awaitConnected(publicationB);
        assertThat(publicationA.sessionId()).as("별도 프로세스(드라이버)의 발행이므로 세션이 달라야 실측 전제가 성립한다")
            .isNotEqualTo(publicationB.sessionId());
        recordingIdA = awaitRecordingId(publicationA.sessionId());
        recordingIdB = awaitRecordingId(publicationB.sessionId());
    }

    @AfterEach
    void tearDown() {
        CloseHelper.quietClose(publicationA);
        CloseHelper.quietClose(publicationB);
        CloseHelper.quietClose(matchingAeronA);
        CloseHelper.quietClose(matchingAeronB);
        CloseHelper.quietClose(matchingDriverA);
        CloseHelper.quietClose(matchingDriverB);
        CloseHelper.quietClose(testArchive);
        CloseHelper.quietClose(accountAeron);
        CloseHelper.quietClose(archivingMediaDriver);
    }

    @Test
    void 발행자_한쪽만_반영이_진행되면_그쪽_recording만_줄고_다른쪽은_그대로다() {
        long positionA1 = publishUntilAdvanced(publicationA, recordingIdA, 4);
        publishUntilAdvanced(publicationB, recordingIdB, 4); // B도 같이 진행시켜 두지만 이번엔 회수 대상에 안 넣는다

        int filesA_before = countSegmentFiles(recordingIdA);
        int filesB_before = countSegmentFiles(recordingIdB);
        assertThat(filesA_before).as("A도 세그먼트 4개 분량을 발행했으니 최소 3개 이상 남아야 한다").isGreaterThanOrEqualTo(3);

        AccountArchiveSegmentPurger purger = new AccountArchiveSegmentPurger(
            accountAeron, controlRequestChannel, -1L /* 이 테스트는 저널을 안 씀 */, fillChannel, FILL_STREAM_ID);
        purger.start();
        try {
            // 1회차 — A만 맵에 담아 보고한다. 첫 회차라 직전 경계가 없어 아직 아무것도 안 지워진다.
            purger.purgeFillsUpTo(Map.of(publicationA.sessionId(), positionA1));
            assertThat(countSegmentFiles(recordingIdA)).as("A 첫 회차는 직전 경계가 없어 안 지운다").isEqualTo(filesA_before);
            assertThat(countSegmentFiles(recordingIdB)).as("B는 맵에 없으니 이번에도 안 건드린다").isEqualTo(filesB_before);

            long positionA2 = publishUntilAdvanced(publicationA, recordingIdA, 4);
            publishUntilAdvanced(publicationB, recordingIdB, 4);
            int filesA_beforeSecondPurge = countSegmentFiles(recordingIdA);
            int filesB_beforeSecondPurge = countSegmentFiles(recordingIdB);

            // 2회차 — 여전히 A만 맵에 담는다(B는 반영 안 된 것으로 취급).
            purger.purgeFillsUpTo(Map.of(publicationA.sessionId(), positionA2));

            assertThat(countSegmentFiles(recordingIdA))
                .as("A는 직전 경계(positionA1)까지 회수돼 두 번째 발행 직후보다 줄어야 한다")
                .isLessThan(filesA_beforeSecondPurge);
            assertThat(countSegmentFiles(recordingIdB))
                .as("B는 fillPositions 맵에 한 번도 안 담겼으니 전혀 줄지 않아야 한다")
                .isEqualTo(filesB_beforeSecondPurge);
        } finally {
            purger.close();
        }
    }

    /**
     * 목표량만큼 발행하고, 실제로 Archive에 기록된 위치({@code getRecordingPosition})가 그만큼
     * 늘 때까지 기다려 그 값을 돌려준다. REMOTE 녹화는 네트워크를 거쳐 비동기로 기록되므로
     * {@code publication.position()}만 보고 "발행 끝"이라 판단하면 그 시점에 디스크엔 아직 안
     * 쓰였을 수 있다 — 세그먼트 파일 수를 재는 이 테스트에서는 recordingPosition 자체를 기준으로
     * 삼아야 레이스가 없다.
     */
    private long publishUntilAdvanced(Publication publication, long recordingId, int segments) {
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
                throw new AssertionError("5초 안에 목표 위치까지 기록되지 않음");
            }
        }
        return testArchive.getRecordingPosition(recordingId);
    }

    private int countSegmentFiles(long recordingId) {
        String[] files = archiveDir.list((dir, name) -> name.startsWith(recordingId + "-") && name.endsWith(".rec"));
        return files == null ? 0 : files.length;
    }

    private void awaitConnected(Publication publication) {
        long deadline = System.nanoTime() + TIMEOUT_NANOS;
        while (!publication.isConnected()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("5초 안에 Publication이 연결되지 않음");
            }
            Thread.yield();
        }
    }

    private long awaitRecordingId(int sessionId) {
        long deadline = System.nanoTime() + TIMEOUT_NANOS;
        long[] found = {-1L};
        while (found[0] < 0) {
            testArchive.listRecordingsForUri(0, 10, fillChannel, FILL_STREAM_ID,
                (controlSessionId, correlationId, recordingId, startTimestamp, stopTimestamp,
                 startPosition, stopPosition, initialTermId, segmentFileLength, termBufferLength,
                 mtuLength, foundSessionId, streamId, strippedChannel, originalChannel, sourceIdentity) -> {
                    if (foundSessionId == sessionId) {
                        found[0] = recordingId;
                    }
                });
            if (found[0] < 0) {
                if (System.nanoTime() > deadline) {
                    throw new AssertionError("5초 안에 sessionId=" + sessionId + " recording이 카탈로그에 나타나지 않음");
                }
                Thread.yield();
            }
        }
        return found[0];
    }

    /** Aeron 드라이버 통신용 임시 디렉터리(aeron-*)를 시작·종료 시 지운다(I10) — 기본값은 안 지운다. */
    private static MediaDriver.Context cleanEmbeddedMediaDriverContext() {
        return new MediaDriver.Context().dirDeleteOnStart(true).dirDeleteOnShutdown(true);
    }
}
