package com.flab.stocktradingengine.account.worker.recovery;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import io.aeron.Aeron;
import io.aeron.archive.client.AeronArchive;

/**
 * 저널·체결 recording의 오래된 Archive 세그먼트를 회수(purge)한다(ADR-032 I5 U1·U2, LLD
 * {@code _i5_lld.md}). {@link AccountSnapshotWriter}의 쓰기 스레드가 스냅샷을 디스크에 확정(fsync)한
 * 직후 {@link #purgeJournalUpTo}·{@link #purgeFillsUpTo}를 순서대로 호출한다(D2) — 이 클래스는
 * "언제 부를지"는 모르고 "부르면 무엇을 지울지"만 안다(SRP, writer의 책임인 "스냅샷을 쓰고 durable을
 * 보고하는 것"과 분리).
 *
 * <p>저널 회수와 체결 회수를 한 클래스에 둔 이유 — 둘 다 "Archive 세그먼트 회수"라는 같은 정책이
 * 바뀔 때 같이 바뀌고, 같은 전용 연결과 같은 스레드 순차 실행(동시 삭제 작업 금지, 5절) 제약을
 * 공유한다. "무엇을 회수할지"(체결의 sessionId→recordingId 매핑)를 {@link AccountSnapshotWriter}가
 * 갖게 하면 writer가 다시 정책을 갖게 돼 SRP 분리 취지가 무너진다.</p>
 *
 * <h3>한 세대 여유 (D1)</h3>
 * <p>받은 위치를 그 자리에서 바로 지우지 않는다 — recording별로 직전 호출 때 받은 위치까지만
 * 지우고, 이번 위치는 다음 호출을 위해 기억해 둔다. 스냅샷 파일이 하나뿐이라(rename으로 덮어씀)
 * 최신 스냅샷 경계까지 지웠는데 그 파일이 손상되면 그 앞 기록도 이미 없어 되살릴 방법이 없다 —
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
 * <p>회수는 예외를 밖으로 던지지 않는다 — 회수 실패가 계좌 엔진을 멈추면 안 된다. 실패는 조용히
 * 넘기지 않고 대상 recordingId·목표 위치·이유를 경고 로그로 남긴다. 다음 스냅샷 주기가 다시
 * 시도한다. 체결 쪽에서 sessionId에 대응하는 recording을 카탈로그에서 못 찾아도(U2) 같은 방식으로
 * 경고만 남기고 그 sessionId는 건너뛴다.</p>
 *
 * <h3>⚠️알려진 한계(이번 범위 밖, 확인만 하고 고치지 않음)</h3>
 * <p>{@code previousPositionByRecording}은 recordingId별 엔트리가 지워지는 코드가 없다 — 매칭
 * 프로세스가 재시작해 새 recording(새 세션)이 생길 때마다 항목이 계속 늘어난다. 같은 문제가
 * {@code AccountEventHandler.lastAppliedFillPositions}(스냅샷 파일에 직렬화되는 맵)에도 있다 — 조정
 * 세션에 확인·보고 완료(tradeId 무상한 HashSet 릭과 같은 모양), 새 이슈로 트래킹 예정.</p>
 */
public class AccountArchiveSegmentPurger implements AutoCloseable {

    private static final Logger log = System.getLogger(AccountArchiveSegmentPurger.class.getName());
    private static final String CONTROL_RESPONSE_CHANNEL = "aeron:udp?endpoint=localhost:0";
    // listRecordingsForUri 페이지 크기 — AccountFillReplayer와 같은 상한.
    private static final int LIST_RECORDINGS_LIMIT = 100;

    private final Aeron aeron;
    private final String controlRequestChannel;
    private final long journalRecordingId;
    private final String fillChannel;
    private final int fillStreamId;
    private final Map<Long, Long> previousPositionByRecording = new HashMap<>();

    private AeronArchive archive;

    public AccountArchiveSegmentPurger(Aeron aeron, String controlRequestChannel, long journalRecordingId,
            String fillChannel, int fillStreamId) {
        this.aeron = aeron;
        this.controlRequestChannel = controlRequestChannel;
        this.journalRecordingId = journalRecordingId;
        this.fillChannel = fillChannel;
        this.fillStreamId = fillStreamId;
    }

    /** 이 스레드 전용 Archive 제어 연결을 연다. {@link #purgeJournalUpTo}·{@link #purgeFillsUpTo}보다 먼저 불러야 한다. */
    public void start() {
        archive = AeronArchive.connect(new AeronArchive.Context()
            .aeron(aeron)
            .ownsAeronClient(false)
            .controlRequestChannel(controlRequestChannel)
            .controlResponseChannel(CONTROL_RESPONSE_CHANNEL));
    }

    /** 저널 recording(생성자에서 고정)에서 직전 회차 경계까지 회수한다(U1). */
    public void purgeJournalUpTo(long position) {
        purgeUpTo(journalRecordingId, position);
    }

    /**
     * 체결 recording들(발행자·Aeron sessionId별로 따로 존재, U2)에서 직전 회차 경계까지 회수한다.
     * {@code fillPositions}의 각 sessionId를 카탈로그에서 recordingId로 변환한 뒤 그 recording에
     * 대해 회수한다 — 맵에 없는(=아직 한 번도 반영 안 한) recording은 건드리지 않는다. sessionId에
     * 대응하는 recording을 카탈로그에서 못 찾으면(이례적) 경고만 남기고 그 항목은 건너뛴다.
     */
    public void purgeFillsUpTo(Map<Integer, Long> fillPositions) {
        fillPositions.forEach((sessionId, position) ->
            findRecordingId(sessionId).ifPresentOrElse(
                recordingId -> purgeUpTo(recordingId, position),
                () -> log.log(Level.WARNING, "[계좌] 체결 recording을 카탈로그에서 찾지 못함: sessionId=" + sessionId)));
    }

    private Optional<Long> findRecordingId(int sessionId) {
        long[] found = {-1L};
        archive.listRecordingsForUri(0, LIST_RECORDINGS_LIMIT, fillChannel, fillStreamId,
            (controlSessionId, correlationId, recordingId, startTimestamp, stopTimestamp,
             startPosition, stopPosition, initialTermId, segmentFileLength, termBufferLength,
             mtuLength, foundSessionId, foundStreamId, strippedChannel, originalChannel, sourceIdentity) -> {
                if (foundSessionId == sessionId) {
                    found[0] = recordingId;
                }
            });
        return found[0] < 0 ? Optional.empty() : Optional.of(found[0]);
    }

    /**
     * 이 recording에서 직전 호출 때 받은 위치까지 회수하고, 이번 위치를 다음 호출을 위해 기억한다.
     * 이 recording에 대한 첫 호출(기억해 둔 직전 위치가 없음)은 아무것도 지우지 않는다.
     */
    private void purgeUpTo(long recordingId, long position) {
        Long previousPosition = previousPositionByRecording.get(recordingId);
        if (previousPosition != null) {
            purgeSafely(recordingId, previousPosition);
        }
        previousPositionByRecording.put(recordingId, position);
    }

    private void purgeSafely(long recordingId, long targetPosition) {
        try {
            RecordingDimensions dimensions = queryDimensions(recordingId);
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

    private RecordingDimensions queryDimensions(long recordingId) {
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
