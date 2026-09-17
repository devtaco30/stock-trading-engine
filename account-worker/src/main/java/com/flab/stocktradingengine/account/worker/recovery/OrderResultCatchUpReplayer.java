package com.flab.stocktradingengine.account.worker.recovery;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

import com.flab.stocktradingengine.codec.OrderResultCodec;
import com.flab.stocktradingengine.codec.OrderResultEntry;

import io.aeron.Image;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.logbuffer.FragmentHandler;

/**
 * U3-b — 주문 결과 스트림의 과거(이미 멈춘) recording을 Aeron Archive에서 읽어 그동안 Kafka로
 * 못 보낸 판정을 모은다. {@code AccountJournalReplayer}(계좌 저널의 과거 recording을 읽는 클래스)와
 * 같은 구조를 그대로 따른다 — 다만 이 클래스는 계좌 상태 복구가 아니라 "아직 Kafka로 안 보낸
 * 판정을 캐치업"하는 용도다.
 *
 * <h3>왜 "이번 실행에서 시작한 recording"은 여기서 안 읽히나</h3>
 * <p>{@link #replayOne}은 {@code recording.stopPosition() - fromPosition <= 0}이면 건너뛴다.
 * 아직 stop되지 않은(진행 중인) recording은 {@code stopPosition()}이 확정 전이라 이 조건에
 * 걸려 자동으로 제외된다 — {@code AccountJournalReplayer}와 같은 성질이다. 이번 실행의 새
 * recording(라이브)은 {@code OrderResultForwarder}가 {@code archive.replay(..., Long.MAX_VALUE, ...)}로
 * 따로 읽는다.</p>
 */
public class OrderResultCatchUpReplayer {

    private static final Logger log = System.getLogger(OrderResultCatchUpReplayer.class.getName());

    // 결과 발행 스트림(4007)·라이브 replay 스트림(4008)과 구분되는, 캐치업 전용 replay 스트림.
    private static final String REPLAY_CHANNEL = "aeron:ipc";
    private static final int REPLAY_STREAM_ID = 4009;
    private static final int FRAGMENT_LIMIT = 10;
    private static final long CONNECT_TIMEOUT_NANOS = 5_000_000_000L;
    // listRecordingsForUri 페이지 크기 — AccountJournalReplayer와 같은 값.
    private static final int LIST_RECORDINGS_LIMIT = 100;

    private final AeronArchive aeronArchive;
    private final OrderResultCodec codec = new OrderResultCodec();

    public OrderResultCatchUpReplayer(AeronArchive aeronArchive) {
        this.aeronArchive = aeronArchive;
    }

    /** 지정한 채널·스트림의 이전 recording을 전부 시작 시각 순서로 읽어 디코딩한다(첫 기동용). */
    public List<OrderResultEntry> readAll(String channel, int streamId) {
        List<RecordingSummary> recordings = listRecordings(channel, streamId);
        recordings.sort(Comparator.comparingLong(RecordingSummary::startTimestamp));

        List<OrderResultEntry> entries = new ArrayList<>();
        for (RecordingSummary recording : recordings) {
            replayOne(recording, recording.startPosition(), entries);
        }
        return entries;
    }

    /**
     * {@code fromRecordingId}보다 먼저 시작 시각 순서로 실행된 recording은 이미 예전에 캐치업된
     * 것으로 보고 건너뛴다. {@code fromRecordingId}인 recording은 {@code fromPosition}부터(처음부터가
     * 아니라) 읽는다. 그보다 나중에 시작한 recording은 전부 처음부터 읽는다.
     *
     * @throws IllegalStateException fromRecordingId가 카탈로그에 없으면 — position 파일과 Archive
     *                                카탈로그가 서로 어긋난 상태라 캐치업을 계속할 수 없다.
     */
    public List<OrderResultEntry> readFrom(String channel, int streamId, long fromRecordingId, long fromPosition) {
        List<RecordingSummary> recordings = listRecordings(channel, streamId);
        recordings.sort(Comparator.comparingLong(RecordingSummary::startTimestamp));

        List<OrderResultEntry> entries = new ArrayList<>();
        boolean foundMatchingRecording = false;
        for (RecordingSummary recording : recordings) {
            if (!foundMatchingRecording) {
                if (recording.recordingId() != fromRecordingId) {
                    continue; // 이미 저장된 position보다 먼저 끝난 recording — 이미 캐치업된 상태라 건너뛴다
                }
                foundMatchingRecording = true;
                replayOne(recording, fromPosition, entries);
                continue;
            }
            replayOne(recording, recording.startPosition(), entries);
        }
        if (!foundMatchingRecording) {
            throw new IllegalStateException(
                "저장된 recordingId(" + fromRecordingId + ")를 주문 결과 Archive 카탈로그에서 찾지 못했습니다");
        }
        return entries;
    }

    private List<RecordingSummary> listRecordings(String channel, int streamId) {
        List<RecordingSummary> found = new ArrayList<>();
        aeronArchive.listRecordingsForUri(0, LIST_RECORDINGS_LIMIT, channel, streamId,
            (controlSessionId, correlationId, recordingId, startTimestamp, stopTimestamp,
             startPosition, stopPosition, initialTermId, segmentFileLength, termBufferLength,
             mtuLength, sessionId, foundStreamId, strippedChannel, originalChannel, sourceIdentity) ->
                found.add(new RecordingSummary(recordingId, startTimestamp, startPosition, stopPosition)));
        return found;
    }

    private void replayOne(RecordingSummary recording, long fromPosition, List<OrderResultEntry> out) {
        long length = recording.stopPosition() - fromPosition;
        if (length <= 0) {
            return; // 이 recording엔 fromPosition 이후로 새로 쓴 게 없다(빈 recording, 이미 다 캐치업됐거나, 아직 stop 안 된 라이브 recording) — 건너뛴다
        }
        try (Subscription subscription = aeronArchive.replay(
                recording.recordingId(), fromPosition, length, REPLAY_CHANNEL, REPLAY_STREAM_ID)) {
            awaitConnected(subscription);
            Image image = subscription.imageAtIndex(0);
            FragmentHandler handler = (buffer, offset, fragmentLength, header) -> {
                Optional<OrderResultEntry> entry = codec.tryDecode(buffer, offset, fragmentLength);
                if (entry.isEmpty()) {
                    log.log(Level.ERROR, "[계좌] 손상된 주문 결과 캐치업 엔트리 skip: recordingId=" + recording.recordingId());
                    return;
                }
                out.add(entry.get());
            };
            while (!image.isEndOfStream() && !image.isClosed()) {
                image.poll(handler, FRAGMENT_LIMIT);
            }
        }
    }

    private void awaitConnected(Subscription subscription) {
        long deadline = System.nanoTime() + CONNECT_TIMEOUT_NANOS;
        while (!subscription.isConnected()) {
            if (System.nanoTime() > deadline) {
                throw new IllegalStateException("주문 결과 캐치업 구독이 5초 안에 연결되지 않았습니다");
            }
            Thread.yield();
        }
    }

    /** listRecordingsForUri 콜백에서 뽑아낸, 재생에 필요한 최소 정보. */
    private record RecordingSummary(long recordingId, long startTimestamp, long startPosition, long stopPosition) {
    }
}
