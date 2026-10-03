package com.flab.stocktradingengine.matching.worker.ltp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.junit.jupiter.api.Test;

import com.flab.stocktradingengine.matching.disruptor.io.MatchListener;
import com.flab.stocktradingengine.trading.entity.OrderSide;
import com.flab.stocktradingengine.trading.matching.FillResult;
import com.flab.stocktradingengine.trading.matching.OrderBook;
import com.flab.stocktradingengine.trading.matching.OrderEntry;

/**
 * 매칭 단일 소비자 스레드에서 불리는 decorator. 호가창 상위 10단계 계산까지만 이 스레드에서 하고 Redis에는 닿지 않는다.
 */
class OrderbookRecordingMatchListenerTest {

    private final List<String> events = Collections.synchronizedList(new ArrayList<>());

    private final MatchListener delegate = new MatchListener() {
        @Override
        public void onFill(String stockCode, FillResult fill) {
            events.add("delegate-fill:" + stockCode);
        }

        @Override
        public void onFill(String stockCode, FillResult fill, long emittedAtNanos, long aggressorOriginNanos) {
            events.add("delegate-fill4:" + stockCode);
        }

        @Override
        public void onBookChanged(String stockCode, OrderBook book) {
            events.add("delegate-book:" + stockCode);
        }
    };

    private static void add(OrderBook book, long id, OrderSide side, String price, int qty) {
        book.addOrder(new OrderEntry(id, 1L, "A1", side, new BigDecimal(price), qty, Instant.now()));
    }

    private static OrderBook bookWithLevels() {
        OrderBook book = new OrderBook();
        add(book, 1, OrderSide.BUY, "100", 5);
        add(book, 2, OrderSide.BUY, "100", 3);
        add(book, 3, OrderSide.BUY, "99", 2);
        add(book, 4, OrderSide.SELL, "101", 1);
        add(book, 5, OrderSide.SELL, "103", 4);
        return book;
    }

    @Test
    void onBookChanged는_상위_단계를_계산해_기록하고_원래_listener도_부른다() {
        List<String> json = Collections.synchronizedList(new ArrayList<>());
        OrderbookSnapshotWriter writer = new OrderbookSnapshotWriter((code, value) -> json.add(value), 100);
        OrderbookRecordingMatchListener listener = new OrderbookRecordingMatchListener(delegate, writer);

        listener.onBookChanged("A1", bookWithLevels());
        writer.flushOnce();

        assertThat(json).containsExactly(
            "{\"bids\":[{\"price\":100,\"quantity\":8},{\"price\":99,\"quantity\":2}],"
                + "\"asks\":[{\"price\":101,\"quantity\":1},{\"price\":103,\"quantity\":4}]}");
        assertThat(events).containsExactly("delegate-book:A1");
    }

    @Test
    void 체결_호출은_감싼_listener로_그대로_전달한다() {
        OrderbookSnapshotWriter writer = new OrderbookSnapshotWriter((code, value) -> { }, 100);
        OrderbookRecordingMatchListener listener = new OrderbookRecordingMatchListener(delegate, writer);
        FillResult fill = new FillResult(1L, 10L, 2L, 20L, 1, new BigDecimal("100"));

        listener.onFill("A1", fill);
        listener.onFill("A1", fill, 5L, 6L);

        assertThat(events).containsExactly("delegate-fill:A1", "delegate-fill4:A1");
    }

    @Test
    void 기록이_예외를_던져도_원래_listener는_호출된다() {
        OrderbookSnapshotWriter broken = new OrderbookSnapshotWriter((code, value) -> { }, 100) {
            @Override
            void record(String stockCode, List<java.util.Map.Entry<BigDecimal, Integer>> bids,
                        List<java.util.Map.Entry<BigDecimal, Integer>> asks) {
                throw new IllegalStateException("record failed");
            }
        };
        OrderbookRecordingMatchListener listener = new OrderbookRecordingMatchListener(delegate, broken);

        assertThatCode(() -> listener.onBookChanged("A1", bookWithLevels())).doesNotThrowAnyException();
        assertThat(events).containsExactly("delegate-book:A1");
    }

    @Test
    void Redis_기록이_영원히_막혀도_onBookChanged는_바로_돌아온다() throws Exception {
        CountDownLatch neverReleased = new CountDownLatch(1);
        OrderbookSnapshotWriter writer = new OrderbookSnapshotWriter((code, value) -> {
            try {
                neverReleased.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, 5);
        writer.start();
        try {
            OrderbookRecordingMatchListener listener = new OrderbookRecordingMatchListener(delegate, writer);
            OrderBook book = bookWithLevels();
            listener.onBookChanged("A0", book);        // 기록 스레드가 이 값을 가져가 막힌다
            Thread.sleep(50);

            assertTimeoutPreemptively(Duration.ofMillis(1_000), () -> {
                for (int i = 0; i < 2_000; i++) {
                    listener.onBookChanged("A" + (i % 30), book);
                }
            });
        } finally {
            neverReleased.countDown();
            writer.close();
        }
    }
}
