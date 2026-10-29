package com.flab.stocktradingengine.matching.disruptor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.flab.stocktradingengine.matching.disruptor.engine.MatchingEngine;
import com.flab.stocktradingengine.matching.disruptor.io.MatchListener;
import com.flab.stocktradingengine.matching.disruptor.snapshot.MatchingSnapshot;
import com.flab.stocktradingengine.matching.disruptor.snapshot.MatchingSnapshotCodec;
import com.flab.stocktradingengine.trading.entity.OrderSide;
import com.flab.stocktradingengine.trading.matching.FillResult;

/**
 * 2d-1a — 스냅샷 직렬화·복원 라운드트립 검증. 복구 배선(2d-1b, Archive position 부터 리플레이)은
 * 아직 안 건드린다 — 여기서는 "엔진A의 호가창을 찍은 스냅샷을 코덱으로 인코딩·디코딩한 뒤
 * 엔진B에 복원하면 같은 상태가 된다"만 증명한다.
 */
class MatchingEngineSnapshotTest {

    private static final String STOCK = "005930";
    private static final int BUFFER_SIZE = 1024;
    // 주문을 발행한 계좌 shard로 보는 Aeron sessionId(시험용 값)
    private static final int SESSION = 7;
    private static final MatchListener NO_OP_LISTENER = (stockCode, fill) -> {};

    private final MatchingSnapshotCodec codec = new MatchingSnapshotCodec();

    private MatchingEngine original;
    private MatchingEngine restored;

    @AfterEach
    void tearDown() {
        if (original != null) {
            original.shutdown();
        }
        if (restored != null) {
            restored.shutdown();
        }
    }

    @Test
    @DisplayName("미체결 주문을 스냅샷·코덱 왕복 후 복원하면 원본과 같은 잔량으로 남는다")
    void 미체결_주문이_스냅샷_왕복_후_복원된다() {
        original = new MatchingEngine(BUFFER_SIZE, NO_OP_LISTENER);
        original.start();
        original.publishPlace(1L, 100L, STOCK, OrderSide.BUY, new BigDecimal("10000"), 10, Instant.now(), 1L, SESSION);

        MatchingSnapshot snapshot = drainAndSnapshot(original, 1L);
        MatchingSnapshot decoded = codec.decode(codec.encode(snapshot));

        restored = new MatchingEngine(BUFFER_SIZE, NO_OP_LISTENER);
        restored.restore(decoded);
        restored.start();

        assertTrue(restored.containsOrder(STOCK, 1L));
    }

    @Test
    @DisplayName("복원한 미체결 주문은 이후 라이브 매칭에 정상 참여한다")
    void 복원한_주문이_라이브_매칭에_참여한다() throws InterruptedException {
        original = new MatchingEngine(BUFFER_SIZE, NO_OP_LISTENER);
        original.start();
        original.publishPlace(1L, 100L, STOCK, OrderSide.BUY, new BigDecimal("10000"), 10, Instant.now(), 1L, SESSION);

        MatchingSnapshot decoded = codec.decode(codec.encode(drainAndSnapshot(original, 1L)));

        CountDownLatch latch = new CountDownLatch(1);
        List<FillResult> fills = new CopyOnWriteArrayList<>();
        restored = new MatchingEngine(BUFFER_SIZE, (stockCode, fill) -> {
            fills.add(fill);
            latch.countDown();
        });
        restored.restore(decoded);
        restored.start();

        restored.publishPlace(2L, 200L, STOCK, OrderSide.SELL, new BigDecimal("10000"), 10, Instant.now());

        assertTrue(latch.await(1, TimeUnit.SECONDS), "복원된 매수 주문과 새 매도 주문이 체결돼야 한다");
        assertEquals(1L, fills.get(0).buyOrderId());
    }

    @Test
    @DisplayName("전량 체결된 주문의 멱등 캐시(filledOrderTimestamps)도 스냅샷에 담겨 복원된다")
    void 전량체결_멱등_캐시도_복원된다() {
        original = new MatchingEngine(BUFFER_SIZE, NO_OP_LISTENER);
        original.start();
        original.publishPlace(1L, 100L, STOCK, OrderSide.BUY, new BigDecimal("10000"), 10, Instant.now(), 1L, SESSION);
        original.publishPlace(2L, 200L, STOCK, OrderSide.SELL, new BigDecimal("10000"), 10, Instant.now().plusMillis(1), 2L, SESSION);

        MatchingSnapshot decoded = codec.decode(codec.encode(drainAndSnapshot(original, 2L)));

        restored = new MatchingEngine(BUFFER_SIZE, NO_OP_LISTENER);
        restored.restore(decoded);
        restored.start();

        assertTrue(restored.containsOrder(STOCK, 1L), "전량 체결된 주문도 멱등 캐시(TTL) 덕에 containsOrder는 true여야 한다");
    }

    /**
     * 마지막으로 발행한 주문이 처리된 것을 확인한 뒤 소비자 thread를 멈추고 스냅샷을 찍는다. {@code MatchingEngine#snapshot}은
     * 소비자 thread가 멈춘 시점에만 안전하다. 처리 중인 호가창을 테스트 thread가 읽으면 경합이 생겨 가끔 실패했다.
     * 처리 확인은 소비자가 주문을 반영한 뒤 남기는 발행자별 위치(동시성 맵)로 한다. 소비자 thread가 아직 돌기 전에
     * {@code shutdown}을 부르면 Disruptor가 적체를 보지 못하고 바로 멈추기 때문에, 위치 확인 없이 shutdown만 부르면 안 된다.
     */
    private MatchingSnapshot drainAndSnapshot(MatchingEngine engine, long lastPosition) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
        while (!Long.valueOf(lastPosition).equals(engine.lastAppliedOrderIntakePositions().get(SESSION))) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("3초 안에 주문이 반영되지 않음: 위치=" + lastPosition);
            }
            Thread.onSpinWait();
        }
        engine.shutdown();
        return engine.snapshot();
    }
}
