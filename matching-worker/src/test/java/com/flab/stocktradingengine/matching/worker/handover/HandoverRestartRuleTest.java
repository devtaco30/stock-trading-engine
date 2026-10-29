package com.flab.stocktradingengine.matching.worker.handover;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.IntFunction;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.flab.stocktradingengine.aeron.MatchingSlotTarget;
import com.flab.stocktradingengine.aeron.StockSlotHasher;
import com.flab.stocktradingengine.codec.EventType;
import com.flab.stocktradingengine.codec.JournaledOrder;
import com.flab.stocktradingengine.matching.disruptor.engine.MatchingEngine;
import com.flab.stocktradingengine.matching.disruptor.snapshot.MatchingSnapshotCodec;
import com.flab.stocktradingengine.trading.entity.OrderSide;

/**
 * 넘긴 쪽(X)의 재시작 규칙(U16c, 11-14절 3번). 저널 재적용이 호가창을 되살린 뒤 엔진을 시작하기 전에, 담당 목록에 없는데 호가창이
 * 있는 종목 slot을 맵 값으로 가른다.
 */
class HandoverRestartRuleTest {

    private static final StockSlotHasher HASHER = new StockSlotHasher(64);
    private static final String STOCK = "005930";
    private static final int SLOT = HASHER.slotFor(STOCK);
    private static final String OWN = "aeron:udp?endpoint=localhost:20020";
    private static final String OTHER = "aeron:udp?endpoint=localhost:20021";

    private final HandoverBookStore store = new HandoverBookStore();
    private MatchingEngine engine;

    @AfterEach
    void tearDown() {
        if (engine != null) {
            engine.shutdown();
        }
    }

    /** 저널 재적용으로 호가창이 되살아난 상태: 주문 1이 호가창에 있고 그 종목 slot을 넘겼다. */
    private MatchingEngine recoveredEngine() {
        MatchingEngine recovered = new MatchingEngine(1024, (stockCode, fill) -> { });
        recovered.recover(List.of(
            new JournaledOrder(EventType.PLACE, 1L, 100L, STOCK, OrderSide.BUY, new BigDecimal("10000"), 10, Instant.now()),
            JournaledOrder.slotReleased(SLOT)));
        return recovered;
    }

    private HandoverRestartRule rule(IntFunction<Optional<MatchingSlotTarget>> map) {
        return new HandoverRestartRule(OWN, map, store);
    }

    @Test
    void 맵이_자기_주소의_이동_중이면_호가창을_두고_보관을_다시_만든다() {
        engine = recoveredEngine();

        rule(slot -> Optional.of(new MatchingSlotTarget(OWN, true))).apply(engine, Set.of());

        assertThat(engine.containsOrder(STOCK, 1L)).isTrue();
        List<byte[]> stored = store.get(SLOT).orElseThrow();
        assertThat(stored).hasSize(1);
        assertThat(new MatchingSnapshotCodec().decode(stored.get(0)).booksByStock()).containsOnlyKeys(STOCK);
    }

    @Test
    void 보관을_다시_만든_종목_slot은_넘기는_중으로_표시되어_엔진을_시작해도_그_종목의_이벤트를_반영하지_않는다() throws Exception {
        engine = recoveredEngine();
        rule(slot -> Optional.of(new MatchingSlotTarget(OWN, true))).apply(engine, Set.of());
        engine.start();

        engine.publishCancel(1L, STOCK);
        engine.publishPlace(2L, 100L, "000660", OrderSide.BUY, new BigDecimal("10000"), 10, Instant.now());
        long deadline = System.nanoTime() + 3_000_000_000L;
        while (!engine.containsOrder("000660", 2L) && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }

        assertThat(engine.containsOrder(STOCK, 1L)).isTrue();
    }

    @Test
    void 맵이_다른_pod의_평상시_주소이면_호가창을_지운다() {
        engine = recoveredEngine();

        rule(slot -> Optional.of(new MatchingSlotTarget(OTHER, false))).apply(engine, Set.of());

        assertThat(engine.containsOrder(STOCK, 1L)).isFalse();
        assertThat(store.get(SLOT)).isEmpty();
    }

    @Test
    void 맵이_다른_pod의_이동_중_값이어도_지운다() {
        engine = recoveredEngine();

        rule(slot -> Optional.of(new MatchingSlotTarget(OTHER, true))).apply(engine, Set.of());

        assertThat(engine.containsOrder(STOCK, 1L)).as("다른 pod가 이 종목 slot을 넘기는 중이면 이 shard의 호가창은 낡은 복사본이다").isFalse();
        assertThat(store.get(SLOT)).isEmpty();
    }

    @Test
    void 맵_값이_없거나_자기_주소의_평상시_값이면_건드리지_않는다() {
        engine = recoveredEngine();

        rule(slot -> Optional.empty()).apply(engine, Set.of());
        rule(slot -> Optional.of(new MatchingSlotTarget(OWN, false))).apply(engine, Set.of());

        assertThat(engine.containsOrder(STOCK, 1L)).isTrue();
        assertThat(store.get(SLOT)).isEmpty();
    }

    @Test
    void 담당_목록에_있는_종목_slot은_맵_값과_상관없이_건드리지_않는다() {
        engine = recoveredEngine();

        rule(slot -> Optional.of(new MatchingSlotTarget(OTHER, false))).apply(engine, Set.of(SLOT));

        assertThat(engine.containsOrder(STOCK, 1L)).isTrue();
    }

    @Test
    void 지운_뒤_다시_재시작해도_저널_재적용으로_되살아난_호가창을_같은_규칙으로_다시_지운다() {
        engine = recoveredEngine();
        rule(slot -> Optional.of(new MatchingSlotTarget(OTHER, false))).apply(engine, Set.of());
        assertThat(engine.containsOrder(STOCK, 1L)).isFalse();
        engine.shutdown();

        engine = recoveredEngine(); // 재시작: 저널을 다시 적용하면 호가창이 되살아난다
        assertThat(engine.containsOrder(STOCK, 1L)).isTrue();
        rule(slot -> Optional.of(new MatchingSlotTarget(OTHER, false))).apply(engine, Set.of());

        assertThat(engine.containsOrder(STOCK, 1L)).isFalse();
    }

    @Test
    void 맵_읽기가_정한_시간_안에_끝나지_않으면_지우지_않고_경고만_남긴다() {
        engine = recoveredEngine();
        HandoverRestartRule rule = rule(slot -> Optional.of(new MatchingSlotTarget(OTHER, false)));

        boolean applied = rule.applyWhenMapReady(() -> {
            throw new IllegalStateException("초기 읽기가 시간 안에 끝나지 않았습니다");
        }, engine, Set.of());

        assertThat(applied).isFalse();
        assertThat(engine.containsOrder(STOCK, 1L)).isTrue();
    }

    @Test
    void 맵_읽기가_끝났으면_규칙을_적용한다() {
        engine = recoveredEngine();
        HandoverRestartRule rule = rule(slot -> Optional.of(new MatchingSlotTarget(OTHER, false)));

        boolean applied = rule.applyWhenMapReady(() -> { }, engine, Set.of());

        assertThat(applied).isTrue();
        assertThat(engine.containsOrder(STOCK, 1L)).isFalse();
    }
}
