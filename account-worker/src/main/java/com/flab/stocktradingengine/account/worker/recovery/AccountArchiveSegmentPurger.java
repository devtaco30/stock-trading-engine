package com.flab.stocktradingengine.account.worker.recovery;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;

import io.aeron.Aeron;
import io.aeron.archive.client.AeronArchive;

/**
 * 저널 recording의 오래된 Archive 세그먼트를 회수(purge)한다(ADR-032 I5 U1, LLD {@code _i5_lld.md}).
 * {@link AccountSnapshotWriter}의 쓰기 스레드가 스냅샷을 디스크에 확정(fsync)한 직후 호출한다
 * (D2) — 이 클래스는 "언제 부를지"는 모르고 "부르면 무엇을 지울지"만 안다(SRP, writer의 책임인
 * "스냅샷을 쓰고 durable을 보고하는 것"과 분리).
 *
 * <h3>한 세대 여유 (D1)</h3>
 * <p>{@link #purgeUpTo}가 받은 위치를 그 자리에서 바로 지우지 않는다 — 직전 호출 때 받은 위치까지만
 * 지우고, 이번 위치는 다음 호출을 위해 기억해 둔다. 스냅샷 파일이 하나뿐이라(rename으로 덮어씀)
 * 최신 스냅샷 경계까지 지웠는데 그 파일이 손상되면 그 앞 저널도 이미 없어 되살릴 방법이 없다 —
 * {@code AccountState.pruneOlderThan}이 메모리에서 tradeId 세대를 현재+직전 남기는 것과 같은 규칙을
 * 디스크에 적용한 것이다.</p>
 *
 * <h3>전용 Archive 연결 (D3)</h3>
 * <p>기동 배선이 쓰는 {@code AeronArchive} 빈을 런타임에 같이 쓰지 않는다 — 같은 제어 세션을 여러
 * 스레드가 동시에 쓰면 안 되므로, 이 클래스가 {@link #start()}에서 전용 연결을 열고 {@link #close()}
 * 에서 닫는다({@code AccountOrderIntakeConfig.aeronArchive}와 같은 연결 방식, 같은 {@link Aeron}
 * 인스턴스를 공유하되 제어 세션만 별도로 연다).</p>
 *
 * <h3>실패해도 멈추지 않는다 (D4)</h3>
 * <p>{@link #purgeUpTo}는 예외를 밖으로 던지지 않는다 — 회수 실패가 계좌 엔진을 멈추면 안 된다.
 * 실패는 조용히 넘기지 않고 대상 recordingId·목표 위치·이유를 경고 로그로 남긴다. 다음 스냅샷
 * 주기가 다시 시도한다.</p>
 */
public class AccountArchiveSegmentPurger implements AutoCloseable {

    private static final Logger log = System.getLogger(AccountArchiveSegmentPurger.class.getName());
    private static final String CONTROL_RESPONSE_CHANNEL = "aeron:udp?endpoint=localhost:0";

    private final Aeron aeron;
    private final String controlRequestChannel;
    private final long recordingId;

    private AeronArchive archive;
    private Long previousPosition;

    public AccountArchiveSegmentPurger(Aeron aeron, String controlRequestChannel, long recordingId) {
        this.aeron = aeron;
        this.controlRequestChannel = controlRequestChannel;
        this.recordingId = recordingId;
    }

    /** 이 스레드 전용 Archive 제어 연결을 연다. {@link #purgeUpTo}보다 먼저 불러야 한다. */
    public void start() {
        archive = AeronArchive.connect(new AeronArchive.Context()
            .aeron(aeron)
            .ownsAeronClient(false)
            .controlRequestChannel(controlRequestChannel)
            .controlResponseChannel(CONTROL_RESPONSE_CHANNEL));
    }

    /**
     * 직전 호출에서 받은 위치까지 회수하고, 이번 위치를 다음 호출을 위해 기억한다. 첫 호출(기억해
     * 둔 직전 위치가 없음)은 아무것도 지우지 않는다.
     */
    public void purgeUpTo(long position) {
        if (previousPosition != null) {
            purgeSafely(previousPosition);
        }
        previousPosition = position;
    }

    private void purgeSafely(long targetPosition) {
        try {
            RecordingDimensions dimensions = queryDimensions();
            long segmentBase = AeronArchive.segmentFileBasePosition(
                dimensions.startPosition(), targetPosition, dimensions.termBufferLength(), dimensions.segmentFileLength());
            long lowerBound = AeronArchive.segmentFileBasePosition(
                    dimensions.startPosition(), dimensions.startPosition(), dimensions.termBufferLength(), dimensions.segmentFileLength())
                + dimensions.segmentFileLength();
            if (segmentBase < lowerBound) {
                return; // 아직 세그먼트 하나를 다 채우지 못해 지울 게 없다 — 실패가 아니라 정상 상태.
            }
            long deletedSegments = archive.purgeSegments(recordingId, segmentBase);
            log.log(Level.DEBUG, "[계좌] Archive 세그먼트 회수: recordingId=" + recordingId
                + " newStartPosition=" + segmentBase + " deletedSegments=" + deletedSegments);
        } catch (RuntimeException e) {
            log.log(Level.WARNING, "[계좌] Archive 세그먼트 회수 실패: recordingId=" + recordingId
                + " targetPosition=" + targetPosition + " 이유=" + e.getMessage());
        }
    }

    private RecordingDimensions queryDimensions() {
        RecordingDimensions[] holder = new RecordingDimensions[1];
        archive.listRecording(recordingId, (controlSessionId, correlationId, recId, startTimestamp, stopTimestamp,
                startPosition, stopPosition, initialTermId, segmentFileLength, termBufferLength,
                mtuLength, sessionId, streamId, strippedChannel, originalChannel, sourceIdentity) ->
            holder[0] = new RecordingDimensions(startPosition, termBufferLength, segmentFileLength));
        RecordingDimensions dimensions = holder[0];
        if (dimensions == null) {
            throw new IllegalStateException("recording을 카탈로그에서 찾지 못했습니다: recordingId=" + recordingId);
        }
        return dimensions;
    }

    /** 전용 Archive 연결을 닫는다. */
    @Override
    public void close() {
        if (archive != null) {
            archive.close();
        }
    }

    private record RecordingDimensions(long startPosition, int termBufferLength, int segmentFileLength) {
    }
}
