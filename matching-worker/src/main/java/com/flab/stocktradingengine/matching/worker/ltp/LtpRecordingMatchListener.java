package com.flab.stocktradingengine.matching.worker.ltp;

import com.flab.stocktradingengine.matching.disruptor.io.MatchListener;
import com.flab.stocktradingengine.trading.matching.FillResult;
import com.flab.stocktradingengine.trading.matching.OrderBook;

import lombok.extern.slf4j.Slf4j;

/**
 * 체결 listener를 감싸 체결가를 {@link LastTradedPriceWriter}에 넘긴 뒤 원래 listener를 부른다.
 *
 * <p>매칭 단일 소비자 스레드에서 불린다. 여기서 하는 일은 메모리 맵 기록뿐이라 Redis가 느리거나 죽어도 매칭은 영향이 없다. 기록이 예외를 던져도
 * 원래 listener 호출은 막지 않는다.</p>
 */
@Slf4j
public class LtpRecordingMatchListener implements MatchListener {

    private static final long WARN_INTERVAL_NANOS = 10_000_000_000L;

    private final MatchListener delegate;
    private final LastTradedPriceWriter writer;
    private long lastWarnNanos = System.nanoTime() - WARN_INTERVAL_NANOS;   // 매칭 단일 스레드만 접근한다

    public LtpRecordingMatchListener(MatchListener delegate, LastTradedPriceWriter writer) {
        this.delegate = delegate;
        this.writer = writer;
    }

    @Override
    public void onFill(String stockCode, FillResult fill) {
        recordSafely(stockCode, fill);
        delegate.onFill(stockCode, fill);
    }

    @Override
    public void onFill(String stockCode, FillResult fill, long emittedAtNanos, long aggressorOriginNanos) {
        recordSafely(stockCode, fill);
        delegate.onFill(stockCode, fill, emittedAtNanos, aggressorOriginNanos);
    }

    @Override
    public void onBookChanged(String stockCode, OrderBook book) {
        delegate.onBookChanged(stockCode, book);
    }

    private void recordSafely(String stockCode, FillResult fill) {
        try {
            writer.record(stockCode, fill.matchPrice());
        } catch (RuntimeException e) {
            long now = System.nanoTime();
            if (now - lastWarnNanos >= WARN_INTERVAL_NANOS) {
                lastWarnNanos = now;
                log.warn("[매칭] LTP 기록 요청 실패(체결 처리는 계속함, 10초에 한 번만 남김): 종목={} 원인={}", stockCode, e.toString());
            }
        }
    }
}
