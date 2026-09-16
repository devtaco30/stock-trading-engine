package com.flab.stocktradingengine.matching.worker.recovery;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.flab.stocktradingengine.codec.JournaledOrder;
import com.flab.stocktradingengine.codec.OrderCodec;

import io.aeron.Image;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.logbuffer.FragmentHandler;

/**
 * 주문 인테이크 스트림(2002)을 Aeron Archive에서 읽어 {@link JournaledOrder} 리스트로 돌려주는
 * 리더(I2 U2b). account-worker {@code AccountFillReplayer}를 그대로 미러한다 — replay+
 * FragmentHandler+tryDecode·awaitConnected 구조가 같다. 매칭이 "주문을 받긴 받았는데 아직
 * 반영 전에" 죽어 놓친 주문을 이 스트림에서 다시 읽어
 * {@link com.flab.stocktradingengine.matching.disruptor.engine.MatchingEngine#recover}에 넘길
 * 입력을 만드는 자리다({@code MatchingOrderIntakeConfig} 클래스 javadoc "주문 인테이크도
 * REMOTE 녹화한다" 참고).
 *
 * <h3>recording이 여럿이면 발행자(계좌 샤드)별로 각자 읽는다</h3>
 * <p>계좌 프로세스가 여럿이거나 계좌가 재시작할 때마다 이 채널·스트림에 recording이 여럿
 * 생긴다(Aeron 연결=세션마다 하나) — {@code MatchingJournalReplayer}처럼 시작 시각순으로
 * "이어 붙일" 수 없다(스냅샷 시점에 recording 여러 개가 동시에 "현재"이기 때문). 대신
 * recording마다 자기 sessionId로 {@code orderIntakePositions} 맵을 찾아 그 위치부터 읽는다 —
 * 맵에 없으면(스냅샷 이후 새로 연결된 발행자) 그 recording의 시작 위치부터 읽는다.</p>
 *
 * <p>{@code listRecordingsForUri} 등 내부 구조가 {@code AccountFillReplayer}와 같지만, 그
 * 클래스의 {@code listRecordings}가 private이라 재사용할 수 없어 이 클래스에 다시 둔다(I5
 * 인수인계가 미리 짚어둔 지점).</p>
 */
public class MatchingOrderIntakeReplayer {

    private static final Logger log = System.getLogger(MatchingOrderIntakeReplayer.class.getName());

    // 주문 인테이크 발행 스트림(2002)과 구분되는, 리플레이 전용 스트림 — replay Subscription이
    // 이 위에서만 연다. account 체결 리플레이(6002)와 겹치면 안 되고, 매칭 저널 리플레이(2006)와도
    // 구분되는 새 값이다.
    private static final String REPLAY_CHANNEL = "aeron:ipc";
    private static final int REPLAY_STREAM_ID = 2007;
    private static final int FRAGMENT_LIMIT = 10;
    private static final long CONNECT_TIMEOUT_NANOS = 5_000_000_000L;
    // listRecordingsForUri 페이지 크기 — 이 프로젝트 규모에서 한 스트림에 recording이 100개를
    // 넘길 일은 없다(AccountFillReplayer·MatchingJournalReplayer와 같은 상한).
    private static final int LIST_RECORDINGS_LIMIT = 100;

    private final AeronArchive aeronArchive;
    private final OrderCodec codec = new OrderCodec();

    public MatchingOrderIntakeReplayer(AeronArchive aeronArchive) {
        this.aeronArchive = aeronArchive;
    }

    /**
     * 주문 인테이크 스트림의 recording을 전부 읽어 디코딩한다. recording마다 자기 sessionId로
     * {@code orderIntakePositions}(스냅샷 시점 발행자별 위치, I2 U1)를 찾아 그 위치부터 읽고,
     * 맵에 없으면 그 recording의 시작 위치부터 읽는다. recording이 카탈로그에 하나도 없으면
     * (매칭이 아직 한 번도 뜬 적이 없거나, 이 채널을 전혀 안 씀) 빈 리스트를 돌려준다 — 예외가
     * 아니다(복구할 gap 자체가 없는 정상 상태).
     */
    public List<JournaledOrder> readFrom(String intakeChannel, int intakeStreamId, Map<Integer, Long> orderIntakePositions) {
        List<RecordingSummary> recordings = listRecordings(intakeChannel, intakeStreamId);
        List<JournaledOrder> entries = new ArrayList<>();
        for (RecordingSummary recording : recordings) {
            long fromPosition = orderIntakePositions.getOrDefault(recording.sessionId(), recording.startPosition());
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

    private void replayOne(RecordingSummary recording, long fromPosition, List<JournaledOrder> out) {
        long length = recording.stopPosition() - fromPosition;
        if (length <= 0) {
            return; // fromPosition 이후로 새로 기록된 게 없다 — 복구할 gap이 없다
        }
        try (Subscription subscription = aeronArchive.replay(
                recording.recordingId(), fromPosition, length, REPLAY_CHANNEL, REPLAY_STREAM_ID)) {
            awaitConnected(subscription);
            Image image = subscription.imageAtIndex(0);
            FragmentHandler handler = (buffer, offset, fragmentLength, header) -> {
                Optional<JournaledOrder> order = codec.tryDecode(buffer, offset);
                if (order.isEmpty()) {
                    log.log(Level.ERROR, "[매칭] 손상 주문 인테이크 리플레이 엔트리 skip: recordingId=" + recording.recordingId());
                    return;
                }
                out.add(order.get());
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
                throw new IllegalStateException("주문 인테이크 리플레이 구독이 5초 안에 연결되지 않았습니다");
            }
            Thread.yield();
        }
    }

    /** listRecordingsForUri 콜백에서 뽑아낸, 재생에 필요한 최소 정보. */
    private record RecordingSummary(long recordingId, int sessionId, long startPosition, long stopPosition) {
    }
}
