package com.flab.stocktradingengine.matching.worker.ltp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.junit.jupiter.api.Test;

import com.flab.stocktradingengine.matching.disruptor.io.MatchListener;
import com.flab.stocktradingengine.trading.matching.FillResult;

/**
 * 매칭 단일 소비자 스레드에서 불리는 decorator가 Redis에 닿지 않는다는 것을 확인한다.
 */
class LtpRecordingMatchListenerTest {

    private static FillResult fill(String price) {
        return new FillResult(1L, 10L, 2L, 20L, 1, new BigDecimal(price));
    }

    private final List<String> events = Collections.synchronizedList(new ArrayList<>());

    private final MatchListener delegate = new MatchListener() {
        @Override
        public void onFill(String stockCode, FillResult fill) {
            events.add("delegate2:" + stockCode);
        }

        @Override
        public void onFill(String stockCode, FillResult fill, long emittedAtNanos, long aggressorOriginNanos) {
            events.add("delegate4:" + stockCode + ":" + emittedAtNanos + ":" + aggressorOriginNanos);
        }
    };

    @Test
    void onFill은_가격을_기록하고_원래_listener를_부른다() {
        LastTradedPriceWriter writer = new LastTradedPriceWriter((code, value) -> events.add("sink:" + code + "=" + value), 100);
        LtpRecordingMatchListener listener = new LtpRecordingMatchListener(delegate, writer);

        listener.onFill("A1", fill("24050"));
        writer.flushOnce();

        assertThat(events).containsExactly("delegate2:A1", "sink:A1=24050");
    }

    @Test
    void 계측_인자가_있는_onFill도_원래_listener의_같은_오버로드를_부른다() {
        LastTradedPriceWriter writer = new LastTradedPriceWriter((code, value) -> { }, 100);
        LtpRecordingMatchListener listener = new LtpRecordingMatchListener(delegate, writer);

        listener.onFill("A1", fill("100"), 7L, 9L);

        assertThat(events).containsExactly("delegate4:A1:7:9");
    }

    @Test
    void 기록이_예외를_던져도_원래_listener는_호출된다() {
        LastTradedPriceWriter broken = new LastTradedPriceWriter((code, value) -> { }, 100) {
            @Override
            void record(String stockCode, BigDecimal price) {
                throw new IllegalStateException("record failed");
            }
        };
        LtpRecordingMatchListener listener = new LtpRecordingMatchListener(delegate, broken);

        assertThatCode(() -> listener.onFill("A1", fill("100"))).doesNotThrowAnyException();
        assertThat(events).containsExactly("delegate2:A1");
    }

    @Test
    void Redis_기록이_영원히_막혀도_onFill은_바로_돌아온다() throws Exception {
        CountDownLatch neverReleased = new CountDownLatch(1);
        LastTradedPriceWriter writer = new LastTradedPriceWriter((code, value) -> {
            try {
                neverReleased.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, 5);
        writer.start();
        try {
            LtpRecordingMatchListener listener = new LtpRecordingMatchListener(delegate, writer);
            listener.onFill("A1", fill("100"));            // 기록 스레드가 이 값을 가져가 막힌다
            Thread.sleep(50);

            assertTimeoutPreemptively(Duration.ofMillis(500), () -> {
                for (int i = 0; i < 2_000; i++) {
                    listener.onFill("A" + (i % 30), fill(String.valueOf(100 + i)));
                }
            });
        } finally {
            neverReleased.countDown();
            writer.close();
        }
    }
}
