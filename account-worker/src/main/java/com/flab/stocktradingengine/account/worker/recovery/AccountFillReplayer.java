package com.flab.stocktradingengine.account.worker.recovery;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import com.flab.stocktradingengine.codec.FillCodec;
import com.flab.stocktradingengine.codec.FilledTrade;

import io.aeron.Image;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.logbuffer.FragmentHandler;

/**
 * 체결 스트림(6001)을 Aeron Archive에서 읽어 {@link FilledTrade} 리스트로 돌려주는 리더(ADR-032,
 * U4b). {@link AccountJournalReplayer}(2b-2b)를 미러한다 — replay+FragmentHandler+tryDecode·
 * awaitConnected 구조가 같다. 매칭이 "재기동 전에 계좌가 못 받은 체결"을 이 스트림에 이미 durable
 * 하게 남겨뒀으므로, 여기서 읽어 {@code AccountEngine#recover}에 넘길 입력을 만든다.
 *
 * <h3>단순화 — 단일 recording 가정 (C6로 미룸)</h3>
 * <p>{@link AccountJournalReplayer}는 저널 스트림에 recording이 여러 개(자기 프로세스가 여러 번
 * 재시작)일 수 있어 시작 시각순으로 이어 붙인다. 이 클래스는 그 복잡도를 지금 들이지 않는다 —
 * fork3 U3부터 체결 스트림(6001)의 녹화 주체는 이 프로세스(계좌) 자신이라(REMOTE, {@code
 * AccountFillIntakeConfig}), recordingId도 계좌가 직접 안다. 단순화는 여전히 남는다: run(프로세스
 * 수명) 하나당 이 채널·스트림에 recording이 정확히 하나(또는 아직 없음, 0개)라고 가정한다 — 발행자인
 * 매칭이 한 run 안에서 여러 번 재시작하면 같은 계좌 run이 매칭 recording을 여러 개 relay로 이어
 * 받게 되는 경우까지는 다루지 않는다(그때는 recordingId를 시작 시각순으로 이어 붙이는
 * {@link AccountJournalReplayer} 방식으로 확장 — 복구 하드닝 C6에서 다룬다).</p>
 */
public class AccountFillReplayer {

    private static final Logger log = System.getLogger(AccountFillReplayer.class.getName());

    // 체결 발행 스트림(6001)과 구분되는, 리플레이 전용 스트림 — replay Subscription이 이 위에서만 연다.
    private static final String REPLAY_CHANNEL = "aeron:ipc";
    private static final int REPLAY_STREAM_ID = 6002;
    private static final int FRAGMENT_LIMIT = 10;
    private static final long CONNECT_TIMEOUT_NANOS = 5_000_000_000L;
    // listRecordingsForUri 페이지 크기 — 이 채널·스트림은 단일 recording을 가정하므로 여유 있게 잡는다.
    private static final int LIST_RECORDINGS_LIMIT = 10;

    private final AeronArchive aeronArchive;
    private final FillCodec codec = new FillCodec();

    public AccountFillReplayer(AeronArchive aeronArchive) {
        this.aeronArchive = aeronArchive;
    }

    /**
     * {@code fromPosition}부터 체결 스트림의 recording을 읽어 디코딩한다. recording이 카탈로그에
     * 없으면(매칭이 아직 한 번도 뜬 적이 없거나, 이 채널을 전혀 안 씀) 빈 리스트를 돌려준다 —
     * 예외가 아니다(복구할 gap 자체가 없는 정상 상태).
     */
    public List<FilledTrade> readFrom(String fillChannel, int fillStreamId, long fromPosition) {
        Optional<RecordingSummary> recording = findRecording(fillChannel, fillStreamId);
        if (recording.isEmpty()) {
            return List.of();
        }
        List<FilledTrade> entries = new ArrayList<>();
        replayOne(recording.get(), fromPosition, entries);
        return entries;
    }

    /** 단일 recording을 가정하고 카탈로그에서 하나만 찾는다(클래스 javadoc "단순화" 참고). */
    private Optional<RecordingSummary> findRecording(String channel, int streamId) {
        List<RecordingSummary> found = new ArrayList<>();
        aeronArchive.listRecordingsForUri(0, LIST_RECORDINGS_LIMIT, channel, streamId,
            (controlSessionId, correlationId, recordingId, startTimestamp, stopTimestamp,
             startPosition, stopPosition, initialTermId, segmentFileLength, termBufferLength,
             mtuLength, sessionId, foundStreamId, strippedChannel, originalChannel, sourceIdentity) ->
                found.add(new RecordingSummary(recordingId, stopPosition)));
        if (found.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(found.get(found.size() - 1));
    }

    private void replayOne(RecordingSummary recording, long fromPosition, List<FilledTrade> out) {
        long length = recording.stopPosition() - fromPosition;
        if (length <= 0) {
            return; // fromPosition 이후로 새로 기록된 게 없다 — 복구할 gap이 없다
        }
        try (Subscription subscription = aeronArchive.replay(
                recording.recordingId(), fromPosition, length, REPLAY_CHANNEL, REPLAY_STREAM_ID)) {
            awaitConnected(subscription);
            Image image = subscription.imageAtIndex(0);
            FragmentHandler handler = (buffer, offset, fragmentLength, header) -> {
                Optional<FilledTrade> trade = codec.tryDecode(buffer, offset);
                if (trade.isEmpty()) {
                    log.log(Level.ERROR, "[계좌] 손상 체결 리플레이 엔트리 skip: recordingId=" + recording.recordingId());
                    return;
                }
                out.add(trade.get());
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
                throw new IllegalStateException("체결 리플레이 구독이 5초 안에 연결되지 않았습니다");
            }
            Thread.yield();
        }
    }

    /** listRecordingsForUri 콜백에서 뽑아낸, 재생에 필요한 최소 정보. */
    private record RecordingSummary(long recordingId, long stopPosition) {
    }
}
