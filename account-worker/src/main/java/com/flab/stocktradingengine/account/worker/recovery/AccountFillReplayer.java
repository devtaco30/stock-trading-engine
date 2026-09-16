package com.flab.stocktradingengine.account.worker.recovery;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.flab.stocktradingengine.codec.FillCodec;
import com.flab.stocktradingengine.codec.FilledTrade;

import io.aeron.Image;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.logbuffer.FragmentHandler;

/**
 * 체결 스트림(6001)을 Aeron Archive에서 읽어 {@link FilledTrade} 리스트로 돌려주는 리더(ADR-032,
 * U4b·I1 U3). {@link AccountJournalReplayer}(2b-2b)를 미러한다 — replay+FragmentHandler+tryDecode·
 * awaitConnected 구조가 같다. 매칭이 "재기동 전에 계좌가 못 받은 체결"을 이 스트림에 이미 durable
 * 하게 남겨뒀으므로, 여기서 읽어 {@code AccountEngine#recover}에 넘길 입력을 만든다.
 *
 * <h3>recording이 여럿이면 발행자(sessionId)별로 각자 읽는다 (ADR-032 I1)</h3>
 * <p>매칭 프로세스가 둘 이상이면 이 채널·스트림에 recording이 여럿 생긴다(각 Aeron 연결=세션마다
 * 하나) — {@link AccountJournalReplayer}처럼 시작 시각순으로 "이어 붙일" 수 없다, 스냅샷 시점에
 * recording 여러 개가 동시에 "현재"이기 때문이다(같은 값을 "먼저 것" "나중 것"으로 나눌 기준이
 * 없다). 대신 recording마다 자기 sessionId로 {@code fillPositions} 맵을 찾아 그 위치부터 읽는다 —
 * 맵에 없으면(스냅샷 이후 새로 연결된 발행자) 그 recording의 시작 위치부터 읽는다. 어느 순서로
 * 읽어도 결과가 같다(D3 — 체결 반영은 tradeId 멱등 + 잔량 차감이라 순서 무관, {@code
 * decision_records/account-fill-order-independence.md}).</p>
 */
public class AccountFillReplayer {

    private static final Logger log = System.getLogger(AccountFillReplayer.class.getName());

    // 체결 발행 스트림(6001)과 구분되는, 리플레이 전용 스트림 — replay Subscription이 이 위에서만 연다.
    private static final String REPLAY_CHANNEL = "aeron:ipc";
    private static final int REPLAY_STREAM_ID = 6002;
    private static final int FRAGMENT_LIMIT = 10;
    private static final long CONNECT_TIMEOUT_NANOS = 5_000_000_000L;
    // listRecordingsForUri 페이지 크기 — AccountJournalReplayer와 같은 상한(이 프로젝트 규모에서
    // 한 스트림에 recording이 100개를 넘길 일은 없다).
    private static final int LIST_RECORDINGS_LIMIT = 100;

    private final AeronArchive aeronArchive;
    private final FillCodec codec = new FillCodec();

    public AccountFillReplayer(AeronArchive aeronArchive) {
        this.aeronArchive = aeronArchive;
    }

    /**
     * 체결 스트림의 recording을 전부 읽어 디코딩한다. recording마다 자기 sessionId로
     * {@code fillPositions}(스냅샷 시점 발행자별 위치, ADR-032 I1 D1)를 찾아 그 위치부터 읽고,
     * 맵에 없으면 그 recording의 시작 위치부터 읽는다. recording이 카탈로그에 하나도 없으면(매칭이
     * 아직 한 번도 뜬 적이 없거나, 이 채널을 전혀 안 씀) 빈 리스트를 돌려준다 — 예외가 아니다
     * (복구할 gap 자체가 없는 정상 상태).
     */
    public List<FilledTrade> readFrom(String fillChannel, int fillStreamId, Map<Integer, Long> fillPositions) {
        List<RecordingSummary> recordings = listRecordings(fillChannel, fillStreamId);
        List<FilledTrade> entries = new ArrayList<>();
        for (RecordingSummary recording : recordings) {
            long fromPosition = fillPositions.getOrDefault(recording.sessionId(), recording.startPosition());
            replayOne(recording, fromPosition, entries);
        }
        return entries;
    }

    private List<RecordingSummary> listRecordings(String channel, int streamId) {
        List<RecordingSummary> found = new ArrayList<>();
        aeronArchive.listRecordingsForUri(0, LIST_RECORDINGS_LIMIT, channel, streamId,
            (controlSessionId, correlationId, recordingId, startTimestamp, stopTimestamp,
             startPosition, stopPosition, initialTermId, segmentFileLength, termBufferLength,
             mtuLength, sessionId, foundStreamId, strippedChannel, originalChannel, sourceIdentity) ->
                found.add(new RecordingSummary(recordingId, sessionId, startPosition, stopPosition)));
        return found;
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
    private record RecordingSummary(long recordingId, int sessionId, long startPosition, long stopPosition) {
    }
}
