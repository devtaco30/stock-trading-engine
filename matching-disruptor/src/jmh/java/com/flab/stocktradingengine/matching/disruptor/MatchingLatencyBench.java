package com.flab.stocktradingengine.matching.disruptor;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Arrays;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.BusySpinWaitStrategy;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.flab.stocktradingengine.matching.disruptor.engine.MatchingEngine;
import com.flab.stocktradingengine.trading.entity.OrderSide;

/**
 * 매칭 코어의 지연(latency) 벤치 — WaitStrategy 별 비교.
 *
 * <h3>왜 처리량이 아니라 지연인가</h3>
 * <p>Unit 3(처리량 벤치)에서는 소비자를 포화시켜 재서 세 WaitStrategy 가 구별되지 않았다. 소비자가 쉴 틈이
 * 없어 "깨어나는 방식"이 드러나지 않았기 때문이다. 지연은 반대로 <b>한가할 때(한 건씩, 저부하)</b> 잰다.
 * 소비자가 이벤트 사이에 대기 상태로 들어가므로, 그 대기에서 깨어나는 비용(WaitStrategy)이 지연에 실린다.
 * busyspin(계속 돌며 감시)은 즉시 보고, blocking(조건변수로 잠듦)은 깨우는 신호를 기다린다.</p>
 *
 * <h3>측정 방법</h3>
 * <p>발행 직전 시각과 체결 도착 시각의 차이를 잰다. 체결 시각은 <b>소비자 스레드</b>에서 찍는다
 * ({@code onFill} 안에서). 대기 중인 다른 스레드를 깨우는 park/unpark 비용을 측정 구간에서 빼기 위함이다 —
 * 그 비용이 섞이면 WaitStrategy 차이가 가려진다. 한 번에 한 건만 흘려 보내(저부하), 각 샘플이 독립적이다.</p>
 *
 * <h3>워크로드</h3>
 * <p>큰 수량의 매수 잔량을 하나 세워 두고(측정 대상 아님), 각 샘플이 수량 1 매도로 그 잔량을 부분 체결시킨다.
 * 매도 하나당 체결 하나가 정확히 나오고, 매수 잔량은 조금씩 줄며 샘플 수만큼 버틴다.</p>
 *
 * <p>수치는 실행 기계·JVM 에 따라 흔들린다. 실행: {@code ./gradlew :matching-disruptor:latencyBench}</p>
 */
public final class MatchingLatencyBench {

    private static final int BUFFER_SIZE = 1 << 16;
    private static final String STOCK = "005930";
    private static final long ACCOUNT = 1L;
    private static final BigDecimal PRICE = BigDecimal.valueOf(10_000L);
    private static final Instant ORDER_AT = Instant.EPOCH;

    private static final int WARMUP_SAMPLES = 20_000;
    private static final int MEASURE_SAMPLES = 100_000;

    // 소비자 스레드(onFill)와 메인 스레드가 한 건씩 주고받는 신호. 한 번에 한 건만 흐르므로 필드 하나면 된다.
    private volatile long sendNanos;
    private volatile long latencyNanos;
    private volatile boolean filled;

    public static void main(String[] args) {
        MatchingLatencyBench bench = new MatchingLatencyBench();
        System.out.println("매칭 코어 지연 벤치 (저부하, 한 건씩) — 단위 µs. 수치는 기계별로 다름.");
        System.out.printf("%-10s %8s %8s %8s %8s %8s %8s%n",
            "strategy", "p50", "p90", "p99", "p99.9", "max", "mean");
        bench.run("blocking", new BlockingWaitStrategy());
        bench.run("yielding", new YieldingWaitStrategy());
        bench.run("busyspin", new BusySpinWaitStrategy());
    }

    private void run(String name, WaitStrategy waitStrategy) {
        MatchingEngine engine = new MatchingEngine(BUFFER_SIZE, waitStrategy, (stockCode, fill) -> {
            latencyNanos = System.nanoTime() - sendNanos;
            filled = true;
        });
        engine.start();

        long sellId = 2L;
        // 큰 매수 잔량 하나를 세운다(측정 대상 아님). 이후 모든 매도가 여기에 교차한다.
        int restingQuantity = WARMUP_SAMPLES + MEASURE_SAMPLES + 1_000;
        engine.publishPlace(1L, ACCOUNT, STOCK, OrderSide.BUY, PRICE, restingQuantity, ORDER_AT);

        for (int i = 0; i < WARMUP_SAMPLES; i++) {
            fireOne(engine, sellId++);
        }

        long[] samples = new long[MEASURE_SAMPLES];
        for (int i = 0; i < MEASURE_SAMPLES; i++) {
            samples[i] = fireOne(engine, sellId++);
        }

        engine.shutdown();
        report(name, samples);
    }

    /** 매도 한 건을 발행하고 체결이 올 때까지 스핀 대기한 뒤, 소비자가 찍은 지연을 돌려준다. */
    private long fireOne(MatchingEngine engine, long sellId) {
        filled = false;
        sendNanos = System.nanoTime();
        engine.publishPlace(sellId, ACCOUNT, STOCK, OrderSide.SELL, PRICE, 1, ORDER_AT);

        long spins = 0;
        while (!filled) {
            if (++spins > 5_000_000_000L) {
                throw new IllegalStateException("체결이 오지 않음 — sellId=" + sellId);
            }
            Thread.onSpinWait();
        }
        return latencyNanos;
    }

    private void report(String name, long[] samples) {
        Arrays.sort(samples);
        long sum = 0;
        for (long value : samples) {
            sum += value;
        }
        double mean = (double) sum / samples.length;
        System.out.printf("%-10s %8.2f %8.2f %8.2f %8.2f %8.2f %8.2f%n",
            name,
            micros(percentile(samples, 50.0)),
            micros(percentile(samples, 90.0)),
            micros(percentile(samples, 99.0)),
            micros(percentile(samples, 99.9)),
            micros(samples[samples.length - 1]),
            mean / 1_000.0);
    }

    private static long percentile(long[] sorted, double percent) {
        int index = (int) Math.ceil(percent / 100.0 * sorted.length) - 1;
        int clamped = Math.max(0, Math.min(index, sorted.length - 1));
        return sorted[clamped];
    }

    private static double micros(long nanos) {
        return nanos / 1_000.0;
    }
}
