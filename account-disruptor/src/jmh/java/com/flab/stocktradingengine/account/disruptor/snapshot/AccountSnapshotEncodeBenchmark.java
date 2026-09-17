package com.flab.stocktradingengine.account.disruptor.snapshot;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * {@link AccountSnapshotCodec#encode(AccountSnapshot)} 1회 호출 비용 벤치마크.
 *
 * <p>계좌 엔진의 single-writer 소비자 스레드(핫패스)는 저널 적용 순번이
 * {@code SNAPSHOT_INTERVAL_JOURNAL_ENTRIES}의 배수가 될 때마다 계좌 상태 전체를 직렬화해
 * 스냅샷으로 남긴다. 이 인터벌 N의 하한은 "스냅샷 1회 인코딩 비용"이 정하므로, 계좌 수를
 * {@link Param}으로 바꿔가며 그 비용을 잰다.</p>
 *
 * <p>계좌 하나당 채운 구성(가정값, 실측 운영값 아님): 매수 예약 2건, 매도 예약 1건, 보유 종목
 * 3개, {@code processedSettlementRefs} 100개, {@code processedRequestIds} 100개, tradeId 세대
 * 2개에 각 세대 500개.</p>
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
// Agrona(ExpandableArrayBuffer)가 JDK 17에서 jdk.internal.misc.Unsafe에 접근한다. JMH가 측정용으로
// 새로 포크하는 JVM에는 account-disruptor Test 태스크의 --add-opens가 적용되지 않으므로 여기서 직접 준다.
@Fork(value = 1, jvmArgsAppend = {
    "--add-opens", "java.base/jdk.internal.misc=ALL-UNNAMED",
    "--add-opens", "java.base/sun.nio.ch=ALL-UNNAMED"
})
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@State(Scope.Benchmark)
public class AccountSnapshotEncodeBenchmark {

    private static final String[] STOCK_CODES = {"005930", "000660", "035420"};

    @Param({"2", "100", "1000", "10000"})
    private int accountCount;

    private final AccountSnapshotCodec codec = new AccountSnapshotCodec();

    private AccountSnapshot snapshot;

    @Setup(Level.Trial)
    public void setUp() {
        Map<Long, AccountStateSnapshot> accountsById = new HashMap<>();
        for (long accountId = 1; accountId <= accountCount; accountId++) {
            accountsById.put(accountId, buildAccount(accountId));
        }
        snapshot = new AccountSnapshot(accountsById, accountCount * 10L, accountCount * 100L);

        byte[] encoded = codec.encode(snapshot);
        System.err.println("[AccountSnapshotEncodeBenchmark] accountCount=" + accountCount
            + " encodedBytes=" + encoded.length);
    }

    @Benchmark
    public void encode(Blackhole blackhole) {
        blackhole.consume(codec.encode(snapshot));
    }

    private AccountStateSnapshot buildAccount(long accountId) {
        Map<Long, BuyReservationSnapshot> reservations = new HashMap<>();
        reservations.put(accountId * 1000 + 1, new BuyReservationSnapshot(new BigDecimal("70000"), 3));
        reservations.put(accountId * 1000 + 2, new BuyReservationSnapshot(new BigDecimal("71000"), 5));

        Map<Long, SellReservationSnapshot> sellReservations = new HashMap<>();
        sellReservations.put(accountId * 1000 + 3, new SellReservationSnapshot(STOCK_CODES[0], 7));

        Map<String, Integer> holdings = new HashMap<>();
        holdings.put(STOCK_CODES[0], 10);
        holdings.put(STOCK_CODES[1], 20);
        holdings.put(STOCK_CODES[2], 30);

        List<TradeIdGenerationSnapshot> tradeIdGenerations = List.of(
            new TradeIdGenerationSnapshot(500L, tradeIdSet(accountId, 0, 500)),
            new TradeIdGenerationSnapshot(0L, tradeIdSet(accountId, 1, 500)));

        Set<Long> processedSettlementRefs = new HashSet<>();
        for (int i = 0; i < 100; i++) {
            processedSettlementRefs.add(accountId * 1_000_000L + i);
        }

        Set<String> processedRequestIds = new HashSet<>();
        for (int i = 0; i < 100; i++) {
            processedRequestIds.add("req-" + accountId + "-" + i);
        }

        return new AccountStateSnapshot(
            accountId,
            42L,
            new BigDecimal("1000000"),
            new BigDecimal("0.40"),
            reservations,
            sellReservations,
            holdings,
            tradeIdGenerations,
            processedSettlementRefs,
            processedRequestIds,
            new BigDecimal("1500"));
    }

    private Set<Long> tradeIdSet(long accountId, int generation, int size) {
        Set<Long> tradeIds = new HashSet<>();
        long base = accountId * 10_000_000L + (long) generation * 1_000_000L;
        for (int i = 0; i < size; i++) {
            tradeIds.add(base + i);
        }
        return tradeIds;
    }
}
