package com.flab.stocktradingengine.matching.worker.ltp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

/**
 * v1 {@code MatchingConsumer}가 이벤트마다 Redis에 쓰던 호가창 스냅샷을 v2 매칭 워커가 단일 매칭 스레드 밖에서 기록하는
 * 전용 기록 스레드. 종목별 마지막 스냅샷만 맵에 덮어쓰고, 바뀐 종목만 직렬화해 기록한다.
 */
class OrderbookSnapshotWriterTest {

    private final List<String> written = Collections.synchronizedList(new ArrayList<>());

    private static List<Map.Entry<BigDecimal, Integer>> levels(Object... priceAndQuantity) {
        List<Map.Entry<BigDecimal, Integer>> list = new ArrayList<>();
        for (int i = 0; i < priceAndQuantity.length; i += 2) {
            list.add(Map.entry(new BigDecimal(priceAndQuantity[i].toString()), (Integer) priceAndQuantity[i + 1]));
        }
        return list;
    }

    private OrderbookSnapshotWriter writerWith(OrderbookSnapshotWriter.SnapshotSink sink) {
        return new OrderbookSnapshotWriter(sink, 100);
    }

    @Test
    void JSON_형식은_v1_저장소와_같다() {
        OrderbookSnapshotWriter writer = writerWith((code, json) -> written.add(code + " " + json));

        writer.record("A000320", levels("24050", 3), levels());
        writer.flushOnce();

        assertThat(written).containsExactly("A000320 {\"bids\":[{\"price\":24050,\"quantity\":3}],\"asks\":[]}");
    }

    @Test
    void 매수_매도_여러_단계를_순서대로_담는다() {
        OrderbookSnapshotWriter writer = writerWith((code, json) -> written.add(json));

        writer.record("A1", levels("100", 5, "99", 2), levels("101", 1, "102", 7));
        writer.flushOnce();

        assertThat(written).containsExactly(
            "{\"bids\":[{\"price\":100,\"quantity\":5},{\"price\":99,\"quantity\":2}],"
                + "\"asks\":[{\"price\":101,\"quantity\":1},{\"price\":102,\"quantity\":7}]}");
    }

    @Test
    void 같은_종목_여러_스냅샷은_마지막_것만_기록한다() {
        OrderbookSnapshotWriter writer = writerWith((code, json) -> written.add(json));

        writer.record("A1", levels("100", 1), levels());
        writer.record("A1", levels("100", 2), levels());
        writer.flushOnce();

        assertThat(written).containsExactly("{\"bids\":[{\"price\":100,\"quantity\":2}],\"asks\":[]}");
    }

    @Test
    void 바뀐_종목만_기록한다() {
        OrderbookSnapshotWriter writer = writerWith((code, json) -> written.add(code));

        writer.record("A1", levels("100", 1), levels());
        writer.flushOnce();
        writer.record("B2", levels("200", 1), levels());
        writer.flushOnce();

        assertThat(written).containsExactly("A1", "B2");
    }

    @Test
    void 기록이_예외를_던지면_다음_주기에_마지막_상태로_다시_시도하고_밖으로_예외를_내지_않는다() {
        AtomicInteger calls = new AtomicInteger();
        OrderbookSnapshotWriter writer = writerWith((code, json) -> {
            if (calls.incrementAndGet() == 1) {
                throw new IllegalStateException("redis down");
            }
            written.add(json);
        });

        writer.record("A1", levels("100", 1), levels());
        writer.flushOnce();
        writer.record("A1", levels("100", 9), levels());
        writer.flushOnce();

        assertThat(written).containsExactly("{\"bids\":[{\"price\":100,\"quantity\":9}],\"asks\":[]}");
    }

    @Test
    void 시작하면_주기마다_자동으로_기록한다() throws Exception {
        OrderbookSnapshotWriter writer = new OrderbookSnapshotWriter((code, json) -> written.add(code), 10);
        writer.start();
        try {
            writer.record("A1", levels("100", 1), levels());
            long deadline = System.currentTimeMillis() + 2_000;
            while (written.isEmpty() && System.currentTimeMillis() < deadline) {
                Thread.sleep(5);
            }
            assertThat(written).containsExactly("A1");
        } finally {
            writer.close();
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void Redis_구현은_v1_저장소와_같은_키에_기록한다() {
        StringRedisTemplate template = mock(StringRedisTemplate.class);
        ValueOperations<String, String> ops = mock(ValueOperations.class);
        when(template.opsForValue()).thenReturn(ops);

        new OrderbookSnapshotWriter.RedisSnapshotSink(template).set("A000320", "{}");

        verify(ops).set("orderbook:A000320", "{}");
    }
}
