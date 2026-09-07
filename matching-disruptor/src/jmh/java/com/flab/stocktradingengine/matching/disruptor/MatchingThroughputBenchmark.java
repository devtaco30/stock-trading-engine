package com.flab.stocktradingengine.matching.disruptor;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.BusySpinWaitStrategy;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.YieldingWaitStrategy;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import com.flab.stocktradingengine.trading.entity.OrderSide;

/**
 * 매칭 코어(링버퍼 → 저널 → 매칭 단일 소비자)의 처리량 벤치마크.
 *
 * <h3>무엇을 재나</h3>
 * <p>주문을 링버퍼에 발행해 단일 소비자가 매칭까지 끝내는 파이프라인의 처리량(orders/sec)이다.
 * {@link WaitStrategy}(소비자가 새 이벤트를 기다리는 방식)를 {@link Param} 으로 바꿔가며 비교한다.</p>
 *
 * <h3>왜 발행 속도만 재면 안 되나</h3>
 * <p>{@code publishPlace} 는 슬롯에 값만 쓰고 바로 돌아온다(비동기). 발행 루프 시간만 재면
 * "링버퍼를 채우는 속도"를 재게 된다. 그래서 invocation 마다 주문 {@link #ORDERS_PER_INVOCATION} 개를
 * 발행한 뒤, 소비자가 그 전부를 처리했음을 {@link CountDownLatch} 로 기다린 다음 op 를 끝낸다.
 * 워크로드상 크로싱 주문(홀수 인덱스) 하나가 정확히 체결 1건을 내므로, 기대 체결 수({@code N/2})를
 * 세면 마지막 주문까지 소비됐음을 알 수 있다.</p>
 *
 * <h3>워크로드 — 절반 잔량 · 절반 교차</h3>
 * <p>한 종목에 {@link #LEVELS} 개 가격대의 매도 호가를 미리 쌓아(@Setup) 호가창에 깊이를 만든다.
 * 타이밍 구간에서는 짝수 인덱스가 매도 잔량을 한 건 더 쌓고(잔량), 홀수 인덱스가 모든 호가 위 가격의
 * 매수로 최저 호가 한 건을 즉시 체결한다(교차). 쌓는 수와 걷어가는 수가 같아 호가창 깊이가
 * {@link #LEVELS} 근처에서 유지되므로, TreeMap 탐색 비용을 포함한 현실적인 매칭 부하가 걸린다.</p>
 *
 * <h3>공정 비교의 한계</h3>
 * <p>v1·v2 가 같은 {@code OrderBook} 을 쓰므로 여기서 재는 값은 "Disruptor 하네스 오버헤드 +
 * WaitStrategy 차이"다. HTTP·브로커·DB 까지 포함한 end-to-end 비교는 Aeron 이 붙는 이후 단위에서 한다.
 * 수치는 실행 기계·JVM 에 따라 흔들리므로 결과에 실행 환경을 함께 남긴다.</p>
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(1)
@State(Scope.Benchmark)
public class MatchingThroughputBenchmark {

    /** invocation 당 발행할 주문 수. @OperationsPerInvocation 과 반드시 같은 값이어야 한다. */
    private static final int ORDERS_PER_INVOCATION = 10_000;

    private static final int BUFFER_SIZE = 1 << 16; // 65536
    private static final String STOCK = "005930";
    private static final long ACCOUNT = 1L;
    private static final int LEVELS = 16;   // 미리 쌓아 둘 매도 호가 가격대 수(호가창 깊이)
    private static final int QTY = 10;      // 모든 주문 수량 동일 → 교차 1건당 체결 정확히 1건
    private static final long BASE_PRICE = 10_000L;
    private static final long TICK = 10L;

    @Param({"blocking", "yielding", "busyspin"})
    private String waitStrategy;

    private MatchingEngine engine;
    private volatile CountDownLatch fillLatch;
    private BigDecimal[] askPrices;    // 가격대별 매도 호가(미리 생성해 매 발행마다 새로 만들지 않음)
    private BigDecimal aggressivePrice; // 모든 호가 위 가격 → 최저 호가를 즉시 교차
    private Instant orderAt;
    private long nextId;

    @Setup(Level.Invocation)
    public void setUp() {
        askPrices = new BigDecimal[LEVELS];
        for (int level = 0; level < LEVELS; level++) {
            askPrices[level] = BigDecimal.valueOf(BASE_PRICE + (long) level * TICK);
        }
        aggressivePrice = BigDecimal.valueOf(BASE_PRICE + (long) LEVELS * TICK);
        orderAt = Instant.EPOCH;
        nextId = 0L;

        int expectedFills = ORDERS_PER_INVOCATION / 2;
        fillLatch = new CountDownLatch(expectedFills);

        engine = new MatchingEngine(BUFFER_SIZE, resolveWaitStrategy(), (stockCode, fill) -> fillLatch.countDown());
        engine.start();

        // 호가창에 깊이를 미리 만든다(타이밍 대상 아님). 매수가 없어 전부 잔량으로 남는다.
        for (int level = 0; level < LEVELS; level++) {
            engine.publishPlace(nextId++, ACCOUNT, STOCK, OrderSide.SELL, askPrices[level], QTY, orderAt);
        }
    }

    @TearDown(Level.Invocation)
    public void tearDown() {
        engine.shutdown();
    }

    @Benchmark
    @OperationsPerInvocation(ORDERS_PER_INVOCATION)
    public void matchThroughput() throws InterruptedException {
        for (int i = 0; i < ORDERS_PER_INVOCATION; i++) {
            if ((i & 1) == 0) {
                // 잔량: 가격대를 돌아가며 매도 호가를 한 건 더 쌓는다.
                int level = (i >> 1) % LEVELS;
                engine.publishPlace(nextId++, ACCOUNT, STOCK, OrderSide.SELL, askPrices[level], QTY, orderAt);
            } else {
                // 교차: 모든 호가 위 가격의 매수 → 최저 매도 호가 한 건을 즉시 체결.
                engine.publishPlace(nextId++, ACCOUNT, STOCK, OrderSide.BUY, aggressivePrice, QTY, orderAt);
            }
        }

        boolean drained = fillLatch.await(60, TimeUnit.SECONDS);
        if (!drained) {
            throw new IllegalStateException(
                "60초 안에 체결이 완료되지 않음 — 남은 체결 수=" + fillLatch.getCount());
        }
    }

    private WaitStrategy resolveWaitStrategy() {
        return switch (waitStrategy) {
            case "blocking" -> new BlockingWaitStrategy();
            case "yielding" -> new YieldingWaitStrategy();
            case "busyspin" -> new BusySpinWaitStrategy();
            default -> throw new IllegalArgumentException("알 수 없는 WaitStrategy: " + waitStrategy);
        };
    }
}
