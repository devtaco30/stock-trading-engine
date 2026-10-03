package com.flab.stocktradingengine.matching.worker.ltp;

import com.flab.stocktradingengine.matching.disruptor.io.MatchListener;
import com.flab.stocktradingengine.trading.matching.FillResult;
import com.flab.stocktradingengine.trading.matching.OrderBook;

import lombok.extern.slf4j.Slf4j;

/**
 * 호가창이 바뀌었다는 알림을 받아 상위 10단계를 계산해 {@link OrderbookSnapshotWriter}에 넘기는 decorator.
 *
 * <p>매칭 단일 소비자 스레드에서 불린다. 이 스레드가 하는 일은 상위 10단계 계산(v1 {@code getBidLevels(10)},
 * {@code getAskLevels(10)}와 같은 깊이)과 메모리 맵 기록뿐이고, JSON 직렬화와 Redis는 기록 스레드가 맡는다. 기록이 예외를 던져도 매칭과
 * 원래 listener 호출은 막지 않는다.</p>
 */
@Slf4j
public class OrderbookRecordingMatchListener implements MatchListener {

    /** v1 {@code MatchingConsumer.writeOrderbookSnapshot}과 같은 깊이 */
    static final int DEPTH = 10;
    private static final long WARN_INTERVAL_NANOS = 10_000_000_000L;

    private final MatchListener delegate;
    private final OrderbookSnapshotWriter writer;
    private long lastWarnNanos = System.nanoTime() - WARN_INTERVAL_NANOS;   // 매칭 단일 스레드만 접근한다

    public OrderbookRecordingMatchListener(MatchListener delegate, OrderbookSnapshotWriter writer) {
        this.delegate = delegate;
        this.writer = writer;
    }

    @Override
    public void onFill(String stockCode, FillResult fill) {
        delegate.onFill(stockCode, fill);
    }

    @Override
    public void onFill(String stockCode, FillResult fill, long emittedAtNanos, long aggressorOriginNanos) {
        delegate.onFill(stockCode, fill, emittedAtNanos, aggressorOriginNanos);
    }

    @Override
    public void onBookChanged(String stockCode, OrderBook book) {
        try {
            writer.record(stockCode, book.getBidLevels(DEPTH), book.getAskLevels(DEPTH));
        } catch (RuntimeException e) {
            long now = System.nanoTime();
            if (now - lastWarnNanos >= WARN_INTERVAL_NANOS) {
                lastWarnNanos = now;
                log.warn("[매칭] 호가창 스냅샷 기록 요청 실패(매칭은 계속함, 10초에 한 번만 남김): 종목={} 원인={}", stockCode, e.toString());
            }
        }
        delegate.onBookChanged(stockCode, book);
    }
}
