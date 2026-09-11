package com.flab.stocktradingengine.matching.worker;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import com.flab.stocktradingengine.codec.JournaledOrder;
import com.flab.stocktradingengine.codec.OrderCodec;

import io.aeron.Image;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.logbuffer.FragmentHandler;

/**
 * 저널 스트림(2005)의 이전 녹화를 Aeron Archive에서 읽어 {@link JournaledOrder} 리스트로 돌려주는
 * 리더(2c-2). account-worker의 {@code AccountJournalReplayer}와 같은 결 — 새 코덱 없이
 * {@link OrderCodec}을 그대로 재사용한다(매칭 인테이크가 이미 같은 코덱을 쓴다).
 * {@link com.flab.stocktradingengine.matching.disruptor.MatchingEngine#recover}에 넘길 입력을
 * 만드는 자리 — 재적용 로직 자체는 코어(matching-disruptor)가 갖고 있다.
 *
 * <h3>여러 녹화 = 여러 번의 재시작</h3>
 * <p>이 스트림에 이전 녹화가 여러 개면(재시작을 여러 번 거쳤으면) 시작 시각(startTimestamp) 순서로
 * 전부 읽는다 — 각 녹화가 그 프로세스 생애 동안의 저널 전체이므로, 시각 순서로 이어 붙이면 전체
 * 이력이 재구성된다. 녹화가 없으면(첫 기동) 빈 리스트를 돌려준다.</p>
 */
class MatchingJournalReplayer {

    // 저널 발행 스트림(2005)과 구분되는, 리플레이 전용 스트림 — replay Subscription이 이 위에서만 연다.
    private static final String REPLAY_CHANNEL = "aeron:ipc";
    private static final int REPLAY_STREAM_ID = 2006;
    private static final int FRAGMENT_LIMIT = 10;
    private static final long CONNECT_TIMEOUT_NANOS = 5_000_000_000L;
    // listRecordingsForUri 페이지 크기 — 이 프로젝트 규모에서 한 스트림에 녹화가 100개를 넘길 일은 없다.
    private static final int LIST_RECORDINGS_LIMIT = 100;

    private final AeronArchive aeronArchive;
    private final OrderCodec codec = new OrderCodec();

    MatchingJournalReplayer(AeronArchive aeronArchive) {
        this.aeronArchive = aeronArchive;
    }

    /** 지정한 채널·스트림의 이전 녹화를 전부 시작 시각 순서로 읽어 디코딩한다. */
    List<JournaledOrder> readAll(String journalChannel, int journalStreamId) {
        List<RecordingSummary> recordings = listRecordings(journalChannel, journalStreamId);
        recordings.sort(Comparator.comparingLong(RecordingSummary::startTimestamp));

        List<JournaledOrder> entries = new ArrayList<>();
        for (RecordingSummary recording : recordings) {
            replayOne(recording, entries);
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

    private void replayOne(RecordingSummary recording, List<JournaledOrder> out) {
        long length = recording.stopPosition() - recording.startPosition();
        if (length <= 0) {
            return; // 연결만 되고 아무것도 안 쓴 빈 녹화 — 건너뛴다
        }
        try (Subscription subscription = aeronArchive.replay(
                recording.recordingId(), recording.startPosition(), length, REPLAY_CHANNEL, REPLAY_STREAM_ID)) {
            awaitConnected(subscription);
            Image image = subscription.imageAtIndex(0);
            FragmentHandler handler = (buffer, offset, fragmentLength, header) -> out.add(codec.decode(buffer, offset));
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
