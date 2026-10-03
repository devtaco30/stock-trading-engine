package com.flab.stocktradingengine.matching.worker.ltp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

/**
 * v1 {@code MatchingConsumer}가 체결마다 Redis에 기록하던 최근 체결가(LTP)를 v2 매칭 워커가 단일 매칭 스레드 밖에서 기록하는
 * 전용 기록 스레드의 동작을 확인한다. 값은 종목별 마지막 값 하나만 필요해서 큐 대신 덮어쓰는 맵을 사용한다.
 */
class LastTradedPriceWriterTest {

    private final List<String> written = Collections.synchronizedList(new ArrayList<>());

    private LastTradedPriceWriter writerWith(LastTradedPriceWriter.LtpSink sink) {
        return new LastTradedPriceWriter(sink, 100);
    }

    @Test
    void 같은_종목_여러_체결은_마지막_값만_기록한다() {
        LastTradedPriceWriter writer = writerWith((code, value) -> written.add(code + "=" + value));

        writer.record("A000320", new BigDecimal("24050"));
        writer.record("A000320", new BigDecimal("24100"));
        writer.flushOnce();

        assertThat(written).containsExactly("A000320=24100");
    }

    @Test
    void 바뀐_종목만_기록한다() {
        LastTradedPriceWriter writer = writerWith((code, value) -> written.add(code + "=" + value));

        writer.record("A1", new BigDecimal("100"));
        writer.flushOnce();
        writer.record("B2", new BigDecimal("200"));
        writer.flushOnce();

        assertThat(written).containsExactly("A1=100", "B2=200");
    }

    @Test
    void 값_형식은_v1과_같은_toPlainString이다() {
        LastTradedPriceWriter writer = writerWith((code, value) -> written.add(value));

        writer.record("A1", new BigDecimal("1E+3"));
        writer.flushOnce();

        assertThat(written).containsExactly("1000");
    }

    @Test
    void 기록이_예외를_던지면_다음_주기에_다시_시도하고_밖으로_예외를_내지_않는다() {
        AtomicInteger calls = new AtomicInteger();
        LastTradedPriceWriter writer = writerWith((code, value) -> {
            if (calls.incrementAndGet() == 1) {
                throw new IllegalStateException("redis down");
            }
            written.add(code + "=" + value);
        });

        writer.record("A1", new BigDecimal("100"));
        writer.flushOnce();                       // 첫 시도는 실패하지만 예외가 나오지 않는다
        assertThat(written).isEmpty();

        writer.flushOnce();                       // 같은 종목이 다시 기록 대상으로 남아 있다
        assertThat(written).containsExactly("A1=100");
    }

    @Test
    void 실패한_사이에_새_체결이_오면_마지막_값을_기록한다() {
        AtomicInteger calls = new AtomicInteger();
        LastTradedPriceWriter writer = writerWith((code, value) -> {
            if (calls.incrementAndGet() == 1) {
                throw new IllegalStateException("redis down");
            }
            written.add(code + "=" + value);
        });

        writer.record("A1", new BigDecimal("100"));
        writer.flushOnce();
        writer.record("A1", new BigDecimal("105"));
        writer.flushOnce();

        assertThat(written).containsExactly("A1=105");
    }

    @Test
    void 시작하면_주기마다_자동으로_기록한다() throws Exception {
        LastTradedPriceWriter writer = new LastTradedPriceWriter((code, value) -> written.add(code + "=" + value), 10);
        writer.start();
        try {
            writer.record("A1", new BigDecimal("100"));
            long deadline = System.currentTimeMillis() + 2_000;
            while (written.isEmpty() && System.currentTimeMillis() < deadline) {
                Thread.sleep(5);
            }
            assertThat(written).containsExactly("A1=100");
        } finally {
            writer.close();
        }
    }

    @Test
    void Redis_구현은_v1_저장소와_같은_키와_값을_기록한다() {
        StringRedisTemplate template = mock(StringRedisTemplate.class);
        @SuppressWarnings("unchecked")
        ValueOperations<String, String> ops = mock(ValueOperations.class);
        when(template.opsForValue()).thenReturn(ops);

        new LastTradedPriceWriter.RedisLtpSink(template).set("A000320", "24050");

        verify(ops).set("ltp:A000320", "24050");
    }
}
