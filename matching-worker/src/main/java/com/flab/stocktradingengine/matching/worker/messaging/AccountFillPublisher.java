package com.flab.stocktradingengine.matching.worker.messaging;

import java.nio.ByteBuffer;
import java.util.Map;

import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;

import com.flab.stocktradingengine.aeron.ShardRoutingTable;
import com.flab.stocktradingengine.codec.FillCodec;
import com.flab.stocktradingengine.codec.FilledTrade;
import com.flab.stocktradingengine.matching.disruptor.io.MatchListener;
import com.flab.stocktradingengine.support.SnowflakeIdGenerator;
import com.flab.stocktradingengine.trading.matching.FillResult;

import io.aeron.ExclusivePublication;
import io.aeron.Publication;

/**
 * 매칭 코어의 체결을 계좌 샤드별 Aeron 스트림으로 fan-out 발행하는 {@link MatchListener}
 * 구현체(ADR-032 U2, fork3 U2). {@link ShardRoutingTable}로 매수·매도 계좌가 각각 속한 샤드
 * endpoint를 계산해 같은 {@link FilledTrade}를 그 목적지들로 보낸다(수신 모델 A — 각 샤드가
 * 매수·매도 둘 다 시도해 자기 소유 계좌만 반영, 소유 검증·반영은 계좌측(유닛 3) 몫). 두 계좌가
 * 같은 endpoint면 offer를 한 번만 해 중복 발행을 막는다.
 *
 * <h3>never-drop offer</h3>
 * <p>{@link #onFill}은 매칭 단일 소비자 스레드에서만 불린다 — 인코딩 버퍼를 필드로 재사용해도
 * 안전하다(U1b류 멀티스레드 race와 무관). 같은 버퍼로 두 번 offer해도 안전한 이유는 {@code offer}가
 * 동기 복사라 두 번째 offer 전에 첫 번째 복사가 이미 끝나 있기 때문이다.
 * account-disruptor {@code AeronArchiveMatchingJournal}(저널)과 같은 이유로 never-drop이다:
 * 이 체결이 유실되면 계좌 축이 영원히 그 체결을 못 받으므로, {@code offer}가 성공(반환값 ≥ 0)할
 * 때까지 {@link BackoffIdleStrategy}로 재시도한다. CLOSED·MAX_POSITION_EXCEEDED는 스트림이
 * 복구 불가 상태라는 뜻이라 예외를 던져
 * {@link com.flab.stocktradingengine.matching.disruptor.handler.MatchingExceptionHandler}가
 * 소비자를 fail-fast로 멈추게 한다.</p>
 *
 * <p>{@code stockCode}·{@code tradeId}는 {@link FillResult}에 없다. stockCode는 {@link #onFill}의
 * 인자로 받고, tradeId는 여기서 체결 1건당 한 번만 발급한다.</p>
 */
public class AccountFillPublisher implements MatchListener {

    private static final int ENCODE_BUFFER_SIZE = 256;

    private final ShardRoutingTable shardRoutingTable;
    private final Map<String, ExclusivePublication> publicationsByEndpoint;
    private final SnowflakeIdGenerator snowflakeIdGenerator;
    private final FillCodec codec = new FillCodec();
    private final UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(ENCODE_BUFFER_SIZE));
    private final IdleStrategy idleStrategy = new BackoffIdleStrategy();

    public AccountFillPublisher(
            ShardRoutingTable shardRoutingTable,
            Map<String, ExclusivePublication> publicationsByEndpoint,
            SnowflakeIdGenerator snowflakeIdGenerator) {
        this.shardRoutingTable = shardRoutingTable;
        this.publicationsByEndpoint = publicationsByEndpoint;
        this.snowflakeIdGenerator = snowflakeIdGenerator;
    }

    @Override
    public void onFill(String stockCode, FillResult fill) {
        long tradeId = snowflakeIdGenerator.nextId();
        FilledTrade trade = new FilledTrade(
            tradeId, stockCode, fill.buyOrderId(), fill.buyAccountId(),
            fill.sellOrderId(), fill.sellAccountId(), fill.filledQuantity(), fill.matchPrice());
        int length = codec.encode(buffer, 0, trade);

        String buyEndpoint = shardRoutingTable.endpointFor(fill.buyAccountId());
        String sellEndpoint = shardRoutingTable.endpointFor(fill.sellAccountId());
        offerNeverDrop(buyEndpoint, length);
        if (!sellEndpoint.equals(buyEndpoint)) {
            offerNeverDrop(sellEndpoint, length);
        }
    }

    private void offerNeverDrop(String endpoint, int length) {
        ExclusivePublication publication = publicationsByEndpoint.get(endpoint);
        if (publication == null) {
            throw new IllegalStateException("체결 fan-out 목적지에 대응하는 발행 스트림이 없습니다: " + endpoint);
        }

        idleStrategy.reset();
        long result;
        while ((result = publication.offer(buffer, 0, length)) < 0) {
            if (result == Publication.CLOSED || result == Publication.MAX_POSITION_EXCEEDED) {
                throw new IllegalStateException(
                    "체결 발행 스트림이 복구 불가 상태입니다(offer 결과=" + result
                        + ", 목적지=" + endpoint + "): 계좌 축이 이 체결을 영원히 못 받으므로 소비자를 멈춥니다");
            }
            idleStrategy.idle();
        }
    }
}
