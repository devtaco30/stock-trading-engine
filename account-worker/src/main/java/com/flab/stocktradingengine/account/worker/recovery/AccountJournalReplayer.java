package com.flab.stocktradingengine.account.worker.recovery;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

import com.flab.stocktradingengine.codec.AccountJournalEntry;
import com.flab.stocktradingengine.codec.AccountJournalEntryCodec;

import io.aeron.Image;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.logbuffer.FragmentHandler;

/**
 * 저널 스트림(4005)의 이전 녹화를 Aeron Archive에서 읽어 {@link AccountJournalEntry} 리스트로
 * 돌려주는 리더(2b-2b). {@link com.flab.stocktradingengine.account.disruptor.engine.AccountEngine#recover}에
 * 넘길 입력을 만드는 자리 — 재적용 로직 자체는 코어(2b-2a)가 갖고 있다.
 *
 * <h3>여러 녹화 = 여러 번의 재시작</h3>
 * <p>이 스트림에 이전 녹화가 여러 개면(재시작을 여러 번 거쳤으면) 시작 시각(startTimestamp) 순서로
 * 전부 읽는다 — 각 녹화가 그 프로세스 생애 동안의 저널 전체이므로, 시각 순서로 이어 붙이면 전체
 * 이력이 재구성된다. 녹화가 없으면(첫 기동) 빈 리스트를 돌려준다.</p>
 *
 * <h3>스냅샷 위치부터만 읽기(2d-2b)</h3>
 * <p>{@link #readFrom}은 스냅샷이 가리키는 recordingId 이전 녹화(이미 스냅샷에 담긴 상태)는 건너뛰고,
 * 그 recordingId 자체는 스냅샷이 찍힌 position부터(처음부터가 아니라)만 읽는다. 그 뒤에 생긴 녹화(그
 * 스냅샷 이후 재시작)는 전부 처음부터 읽는다 — matching {@code MatchingJournalReplayer.readFrom}과
 * 같은 이유(ADR-019, 저널을 0부터 전부 재생하지 않고 스냅샷 이후 변화만 읽어 복구 시간을 줄인다).</p>
 */
public class AccountJournalReplayer {

    private static final Logger log = System.getLogger(AccountJournalReplayer.class.getName());

    // 저널 발행 스트림(4005)과 구분되는, 리플레이 전용 스트림 — replay Subscription이 이 위에서만 연다.
    private static final String REPLAY_CHANNEL = "aeron:ipc";
    private static final int REPLAY_STREAM_ID = 4006;
    private static final int FRAGMENT_LIMIT = 10;
    private static final long CONNECT_TIMEOUT_NANOS = 5_000_000_000L;
    // listRecordingsForUri 페이지 크기 — 이 프로젝트 규모에서 한 스트림에 녹화가 100개를 넘길 일은 없다.
    private static final int LIST_RECORDINGS_LIMIT = 100;

    private final AeronArchive aeronArchive;
    private final AccountJournalEntryCodec codec = new AccountJournalEntryCodec();

    public AccountJournalReplayer(AeronArchive aeronArchive) {
        this.aeronArchive = aeronArchive;
    }

    /** 지정한 채널·스트림의 이전 녹화를 전부 시작 시각 순서로 읽어 디코딩한다. */
    public List<AccountJournalEntry> readAll(String journalChannel, int journalStreamId) {
        List<RecordingSummary> recordings = listRecordings(journalChannel, journalStreamId);
        recordings.sort(Comparator.comparingLong(RecordingSummary::startTimestamp));

        List<AccountJournalEntry> entries = new ArrayList<>();
        for (RecordingSummary recording : recordings) {
            replayOne(recording, recording.startPosition(), entries);
        }
        return entries;
    }

    /**
     * {@code fromRecordingId}보다 먼저 시작 시각 순서로 실행된 녹화는 이미 스냅샷에 담긴 것으로
     * 보고 건너뛴다. {@code fromRecordingId}인 녹화는 {@code fromPosition}부터(처음부터가 아니라)
     * 읽는다. 그보다 나중에 시작한 녹화는 전부 처음부터 읽는다.
     *
     * @throws IllegalStateException fromRecordingId가 카탈로그에 없으면 — 스냅샷 파일과 Archive
     *                                카탈로그가 서로 어긋난 상태라 복구를 계속할 수 없다.
     */
    public List<AccountJournalEntry> readFrom(String journalChannel, int journalStreamId, long fromRecordingId, long fromPosition) {
        List<RecordingSummary> recordings = listRecordings(journalChannel, journalStreamId);
        recordings.sort(Comparator.comparingLong(RecordingSummary::startTimestamp));

        List<AccountJournalEntry> entries = new ArrayList<>();
        boolean foundSnapshotRecording = false;
        for (RecordingSummary recording : recordings) {
            if (!foundSnapshotRecording) {
                if (recording.recordingId() != fromRecordingId) {
                    continue; // 스냅샷보다 먼저 끝난 녹화 — 이미 스냅샷에 담긴 상태라 건너뛴다
                }
                foundSnapshotRecording = true;
                replayOne(recording, fromPosition, entries);
                continue;
            }
            replayOne(recording, recording.startPosition(), entries);
        }
        if (!foundSnapshotRecording) {
            throw new IllegalStateException(
                "스냅샷이 가리키는 recordingId(" + fromRecordingId + ")를 Archive 카탈로그에서 찾지 못했습니다");
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

    private void replayOne(RecordingSummary recording, long fromPosition, List<AccountJournalEntry> out) {
        long length = recording.stopPosition() - fromPosition;
        if (length <= 0) {
            return; // 이 녹화엔 fromPosition 이후로 새로 쓴 게 없다(빈 녹화이거나, 스냅샷이 이미 끝까지 담았거나) — 건너뛴다
        }
        try (Subscription subscription = aeronArchive.replay(
                recording.recordingId(), fromPosition, length, REPLAY_CHANNEL, REPLAY_STREAM_ID)) {
            awaitConnected(subscription);
            Image image = subscription.imageAtIndex(0);
            FragmentHandler handler = (buffer, offset, fragmentLength, header) -> {
                Optional<AccountJournalEntry> entry = codec.tryDecode(buffer, offset);
                if (entry.isEmpty()) {
                    log.log(Level.ERROR, "[계좌] 손상 저널 엔트리 skip: recordingId=" + recording.recordingId());
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
                throw new IllegalStateException("저널 리플레이 구독이 5초 안에 연결되지 않았습니다");
            }
            Thread.yield();
        }
    }

    /** listRecordingsForUri 콜백에서 뽑아낸, 재생에 필요한 최소 정보. */
    private record RecordingSummary(long recordingId, long startTimestamp, long startPosition, long stopPosition) {
    }
}
