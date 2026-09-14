package com.flab.stocktradingengine.matching.worker.messaging;

import java.nio.ByteBuffer;

import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;

import com.flab.stocktradingengine.codec.FillCodec;
import com.flab.stocktradingengine.codec.FilledTrade;
import com.flab.stocktradingengine.matching.disruptor.io.MatchListener;
import com.flab.stocktradingengine.support.SnowflakeIdGenerator;
import com.flab.stocktradingengine.trading.matching.FillResult;

import io.aeron.ExclusivePublication;
import io.aeron.Publication;

/**
 * 매칭 코어의 체결을 Aeron 체결 스트림({@link com.flab.stocktradingengine.matching.worker.config.MatchingFillPublishConfig})
 * 으로 발행하는 {@link MatchListener} 구현체(ADR-032, U2). Kafka fan-out(매수·매도 계좌 앞으로 각각
 * 발행)을 대체한다 — IPC 단일 스트림엔 Kafka 파티션 같은 개념이 없어 두 번 보낼 이유가 없다.
 * 계좌 수신기가 이 메시지 1건으로 매수·매도 양쪽을 반영한다(유닛 4).
 *
 * <h3>never-drop offer</h3>
 * <p>{@link #onFill}은 매칭 단일 소비자 스레드에서만 불린다 — 인코딩 버퍼를 필드로 재사용해도
 * 안전하다. account-disruptor {@code AeronArchiveMatchingJournal}(저널)과 같은 이유로 never-drop
 * 이다: 이 체결이 유실되면 계좌 축이 영원히 그 체결을 못 받으므로, {@code offer}가 성공(반환값 ≥ 0)
 * 할 때까지 {@link BackoffIdleStrategy}로 재시도한다. CLOSED·MAX_POSITION_EXCEEDED는 스트림이
 * 복구 불가 상태라는 뜻이라 예외를 던져
 * {@link com.flab.stocktradingengine.matching.disruptor.handler.MatchingExceptionHandler}가
 * 소비자를 fail-fast로 멈추게 한다.</p>
 *
 * <p>{@code stockCode}·{@code tradeId}는 {@link FillResult}에 없다. stockCode는 {@link #onFill}의
 * 인자로 받고, tradeId는 여기서 체결 1건당 한 번만 발급한다.</p>
 */
public class AccountFillPublisher implements MatchListener {

    private static final int ENCODE_BUFFER_SIZE = 256;

    private final ExclusivePublication fillPublication;
    private final SnowflakeIdGenerator snowflakeIdGenerator;
    private final FillCodec codec = new FillCodec();
    private final UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(ENCODE_BUFFER_SIZE));
    private final IdleStrategy idleStrategy = new BackoffIdleStrategy();

    public AccountFillPublisher(ExclusivePublication fillPublication, SnowflakeIdGenerator snowflakeIdGenerator) {
        this.fillPublication = fillPublication;
        this.snowflakeIdGenerator = snowflakeIdGenerator;
    }

    @Override
    public void onFill(String stockCode, FillResult fill) {
        long tradeId = snowflakeIdGenerator.nextId();
        FilledTrade trade = new FilledTrade(
            tradeId, stockCode, fill.buyOrderId(), fill.buyAccountId(),
            fill.sellOrderId(), fill.sellAccountId(), fill.filledQuantity(), fill.matchPrice());
        int length = codec.encode(buffer, 0, trade);

        idleStrategy.reset();
        long result;
        while ((result = fillPublication.offer(buffer, 0, length)) < 0) {
            if (result == Publication.CLOSED || result == Publication.MAX_POSITION_EXCEEDED) {
                throw new IllegalStateException(
                    "체결 발행 스트림이 복구 불가 상태입니다(offer 결과=" + result
                        + "): 계좌 축이 이 체결을 영원히 못 받으므로 소비자를 멈춥니다");
            }
            idleStrategy.idle();
        }
    }
}
