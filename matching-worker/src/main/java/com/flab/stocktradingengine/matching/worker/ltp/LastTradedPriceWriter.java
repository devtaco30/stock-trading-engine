package com.flab.stocktradingengine.matching.worker.ltp;

import java.math.BigDecimal;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.springframework.data.redis.core.StringRedisTemplate;

import com.flab.stocktradingengine.redis.RedisKeys;

import lombok.extern.slf4j.Slf4j;

/**
 * 최근 체결가(LTP)를 Redis에 기록하는 전용 스레드.
 *
 * <p>v1 {@code MatchingConsumer}는 체결마다 매칭 스레드에서 Redis에 기록했다. v2 매칭은 단일 소비자 스레드라 그 스레드가 Redis에 닿으면
 * Redis 지연이 매칭 전체를 늦춘다. 그래서 매칭 스레드는 {@link #record}로 종목별 마지막 값을 메모리 맵에 덮어쓰기만 하고(I/O 없음), 이 클래스의
 * 스레드가 주기마다 바뀐 종목만 Redis에 기록한다. 읽는 쪽(api)은 종목별 가장 최근 값 하나만 필요해서 큐 대신 덮어쓰는 맵을 사용한다. 맵 크기는
 * 종목 수로 묶여 있어 가득 차는 상태가 없다.</p>
 *
 * <p>같은 종목에 한 주기 안에 체결이 여러 번이면 중간 값은 건너뛰고 마지막 값만 기록된다. 반영은 최대 한 주기 늦다. 종료할 때 아직 기록하지
 * 못한 값은 버려진다(LTP는 다음 체결로 다시 채워지는 파생 값이다).</p>
 */
@Slf4j
public class LastTradedPriceWriter implements AutoCloseable {

    private static final long WARN_INTERVAL_NANOS = 10_000_000_000L;

    /** 종목 코드와 값 문자열을 한 곳에 기록한다. 테스트가 가짜로 바꾼다. */
    @FunctionalInterface
    public interface LtpSink {
        void set(String stockCode, String value);
    }

    /** v1 {@code LtpRedisRepository.set}과 같은 키(core {@link RedisKeys#ltp})와 값 형식을 사용한다. */
    public static final class RedisLtpSink implements LtpSink {

        private final StringRedisTemplate stringRedisTemplate;

        public RedisLtpSink(StringRedisTemplate stringRedisTemplate) {
            this.stringRedisTemplate = stringRedisTemplate;
        }

        @Override
        public void set(String stockCode, String value) {
            stringRedisTemplate.opsForValue().set(RedisKeys.ltp(stockCode), value);
        }
    }

    private final LtpSink sink;
    private final long flushIntervalMillis;
    private final ConcurrentHashMap<String, BigDecimal> latest = new ConcurrentHashMap<>();
    private final Set<String> dirty = ConcurrentHashMap.newKeySet();
    private final AtomicLong lastWarnNanos = new AtomicLong(System.nanoTime() - WARN_INTERVAL_NANOS);

    private volatile boolean running;
    private Thread writerThread;

    public LastTradedPriceWriter(LtpSink sink, long flushIntervalMillis) {
        this.sink = sink;
        this.flushIntervalMillis = flushIntervalMillis;
    }

    /** 매칭 스레드에서 불린다. 맵 덮어쓰기와 집합 추가뿐이라 I/O도 락 대기도 없다. */
    void record(String stockCode, BigDecimal price) {
        latest.put(stockCode, price);
        dirty.add(stockCode);
    }

    /** 바뀐 종목을 한 번 기록한다. 기록이 실패한 종목은 다음 호출에서 다시 시도하고, 예외는 밖으로 내보내지 않는다. */
    void flushOnce() {
        for (String stockCode : dirty) {
            if (!dirty.remove(stockCode)) {
                continue;
            }
            BigDecimal price = latest.get(stockCode);
            if (price == null) {
                continue;
            }
            try {
                sink.set(stockCode, price.toPlainString());
            } catch (RuntimeException e) {
                dirty.add(stockCode);
                warnRateLimited(stockCode, e);
            }
        }
    }

    public void start() {
        running = true;
        writerThread = new Thread(this::loop, "ltp-writer");
        writerThread.setDaemon(true);
        writerThread.start();
    }

    private void loop() {
        while (running) {
            flushOnce();
            try {
                Thread.sleep(flushIntervalMillis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private void warnRateLimited(String stockCode, RuntimeException e) {
        long now = System.nanoTime();
        long last = lastWarnNanos.get();
        if (now - last >= WARN_INTERVAL_NANOS && lastWarnNanos.compareAndSet(last, now)) {
            log.warn("[매칭] LTP 기록 실패(다음 주기에 다시 시도, 10초에 한 번만 남김): 종목={} 원인={}", stockCode, e.toString());
        }
    }

    @Override
    public void close() {
        running = false;
        if (writerThread != null) {
            writerThread.interrupt();
            try {
                writerThread.join(1_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
