package com.flab.stocktradingengine.matching.worker.ltp;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.springframework.data.redis.core.StringRedisTemplate;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flab.stocktradingengine.redis.RedisKeys;

import lombok.extern.slf4j.Slf4j;

/**
 * 호가창 스냅샷을 Redis에 기록하는 전용 스레드.
 *
 * <p>v1 {@code MatchingConsumer}는 이벤트마다 상위 호가 단계를 계산하고 JSON으로 직렬화해 Redis에 썼다. v2 매칭은 단일 소비자
 * 스레드라 그 스레드가 Redis나 JSON 직렬화에 닿으면 매칭 전체가 늦어진다. 그래서 매칭 스레드는 상위 단계 계산 결과(불변 값)를
 * {@link #record}로 종목별 맵에 덮어쓰기만 하고, 이 클래스의 스레드가 주기마다 바뀐 종목만 직렬화해 기록한다. 읽는 쪽(api 호가창
 * 조회)은 종목별 가장 최근 스냅샷 하나만 필요해서 큐 대신 덮어쓰는 맵을 사용하고, 맵 크기는 호가창이 있는 종목 수로 묶인다.</p>
 *
 * <p>한 주기 안의 중간 상태는 건너뛰고 마지막 상태만 기록되며 반영은 최대 한 주기 늦다. 종료할 때 아직 기록하지 못한 상태는
 * 버려진다. JSON 형식은 v1 {@code OrderbookRedisRepository}의 {@code Snapshot(bids, asks)}, {@code Level(price, quantity)}와
 * 같다.</p>
 */
@Slf4j
public class OrderbookSnapshotWriter implements AutoCloseable {

    private static final long WARN_INTERVAL_NANOS = 10_000_000_000L;

    /** 종목 코드와 JSON 문자열을 한 곳에 기록한다. 테스트가 가짜로 바꾼다. */
    @FunctionalInterface
    public interface SnapshotSink {
        void set(String stockCode, String json);
    }

    /** v1 {@code OrderbookRedisRepository.saveSnapshot}과 같은 키(core {@link RedisKeys#orderbook})에 기록한다. */
    public static final class RedisSnapshotSink implements SnapshotSink {

        private final StringRedisTemplate stringRedisTemplate;

        public RedisSnapshotSink(StringRedisTemplate stringRedisTemplate) {
            this.stringRedisTemplate = stringRedisTemplate;
        }

        @Override
        public void set(String stockCode, String json) {
            stringRedisTemplate.opsForValue().set(RedisKeys.orderbook(stockCode), json);
        }
    }

    public record Level(BigDecimal price, int quantity) {}

    public record Snapshot(List<Level> bids, List<Level> asks) {}

    private final SnapshotSink sink;
    private final long flushIntervalMillis;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final ConcurrentHashMap<String, Snapshot> latest = new ConcurrentHashMap<>();
    private final Set<String> dirty = ConcurrentHashMap.newKeySet();
    private final AtomicLong lastWarnNanos = new AtomicLong(System.nanoTime() - WARN_INTERVAL_NANOS);

    private volatile boolean running;
    private Thread writerThread;

    public OrderbookSnapshotWriter(SnapshotSink sink, long flushIntervalMillis) {
        this.sink = sink;
        this.flushIntervalMillis = flushIntervalMillis;
    }

    /** 매칭 스레드에서 불린다. 불변 값을 만들어 맵에 덮어쓰고 종목을 표시할 뿐이라 I/O도 락 대기도 없다. */
    void record(String stockCode, List<Map.Entry<BigDecimal, Integer>> bids, List<Map.Entry<BigDecimal, Integer>> asks) {
        latest.put(stockCode, new Snapshot(toLevels(bids), toLevels(asks)));
        dirty.add(stockCode);
    }

    private static List<Level> toLevels(List<Map.Entry<BigDecimal, Integer>> entries) {
        Level[] levels = new Level[entries.size()];
        for (int i = 0; i < levels.length; i++) {
            Map.Entry<BigDecimal, Integer> entry = entries.get(i);
            levels[i] = new Level(entry.getKey(), entry.getValue());
        }
        return List.of(levels);
    }

    /** 바뀐 종목을 한 번 기록한다. 실패한 종목은 다음 호출에서 마지막 상태로 다시 시도하고, 예외는 밖으로 내보내지 않는다. */
    void flushOnce() {
        for (String stockCode : dirty) {
            if (!dirty.remove(stockCode)) {
                continue;
            }
            Snapshot snapshot = latest.get(stockCode);
            if (snapshot == null) {
                continue;
            }
            try {
                sink.set(stockCode, objectMapper.writeValueAsString(snapshot));
            } catch (JsonProcessingException e) {
                // 직렬화는 입력과 무관하게 같은 결과이므로 다시 시도하지 않는다(v1도 직렬화 실패는 건너뛰고 경고만 남긴다)
                warnRateLimited(stockCode, e);
            } catch (RuntimeException e) {
                dirty.add(stockCode);
                warnRateLimited(stockCode, e);
            }
        }
    }

    public void start() {
        running = true;
        writerThread = new Thread(this::loop, "orderbook-writer");
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

    private void warnRateLimited(String stockCode, Exception e) {
        long now = System.nanoTime();
        long last = lastWarnNanos.get();
        if (now - last >= WARN_INTERVAL_NANOS && lastWarnNanos.compareAndSet(last, now)) {
            log.warn("[매칭] 호가창 스냅샷 기록 실패(다음 주기에 다시 시도, 10초에 한 번만 남김): 종목={} 원인={}", stockCode, e.toString());
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
