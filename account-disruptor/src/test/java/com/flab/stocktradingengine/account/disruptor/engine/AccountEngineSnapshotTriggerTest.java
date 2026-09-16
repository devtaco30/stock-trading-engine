package com.flab.stocktradingengine.account.disruptor.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.flab.stocktradingengine.account.disruptor.domain.AccountState;
import com.flab.stocktradingengine.account.disruptor.domain.NoOpAccountResultListener;
import com.flab.stocktradingengine.account.disruptor.handler.AccountEventHandler;
import com.flab.stocktradingengine.account.disruptor.io.AccountSnapshotSink;
import com.flab.stocktradingengine.account.disruptor.io.MatchingOrderSender;
import com.flab.stocktradingengine.account.disruptor.snapshot.AccountSnapshot;
import com.flab.stocktradingengine.account.disruptor.snapshot.AccountSnapshotCodec;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.dsl.ProducerType;

/**
 * 1-3 — 저널 N건마다 소비자 스레드가 러닝 중 스냅샷을 {@link AccountSnapshotSink}로 내보내고,
 * 싱크가 durable을 보고하면 tradeId 세대를 가지치기하는지 검증한다. N(=10_000, {@code
 * AccountEventHandler.SNAPSHOT_INTERVAL_JOURNAL_ENTRIES})을 그대로 쓴다 — 같은 requestId
 * 재전송(빠른 dup 경로)으로 appliedSeq만 밀어 올리므로 순수 인메모리에서도 충분히 빠르다.
 */
class AccountEngineSnapshotTriggerTest {

    private static final String STOCK = "005930";
    private static final long NODE_ID = 0L;
    private static final long SNAPSHOT_INTERVAL = 10_000L;
    private static final MatchingOrderSender NO_OP_SENDER = (orderId, accountId, stockCode, side, price, quantity) -> {};

    private AccountEngine engine;

    @AfterEach
    void tearDown() {
        if (engine != null) {
            engine.shutdown();
        }
    }

    @Test
    @DisplayName("저널 적용 순번이 N에 도달하면 싱크에 스냅샷을 정확히 한 번 offer한다")
    void N건마다_스냅샷을_한번_offer한다() throws InterruptedException {
        RecordingSnapshotSink sink = new RecordingSnapshotSink();
        engine = new AccountEngine(1024, new BlockingWaitStrategy(), ProducerType.SINGLE, NODE_ID,
            NO_OP_SENDER, NoOpAccountResultListener.INSTANCE, new com.flab.stocktradingengine.account.disruptor.journal.InMemoryAccountJournal(), sink);
        engine.seed(1L, new BigDecimal("1000000"), new BigDecimal("0.40"));
        engine.start();

        // 같은 requestId를 N번 재전송 — 첫 건만 accept, 나머지는 빠른 dup 경로지만 appliedSeq는 매번 오른다.
        for (int i = 0; i < SNAPSHOT_INTERVAL; i++) {
            engine.publishBuy(1L, STOCK, new BigDecimal("10000"), 1, "same-request-id");
        }

        awaitOfferCount(sink, 1);

        assertEquals(1, sink.offers.size(), "정확히 N건째에 한 번만 offer돼야 한다");
        AccountSnapshotCodec codec = new AccountSnapshotCodec();
        AccountSnapshot decoded = codec.decode(sink.offers.get(0).snapshotBytes());
        assertTrue(decoded.accountsById().containsKey(1L), "스냅샷 바이트가 실제로 디코딩 가능해야 한다");
        assertEquals(SNAPSHOT_INTERVAL, sink.offers.get(0).appliedSeq());
    }

    @Test
    @DisplayName("싱크가 durable을 보고하기 전에는 세 번째 세대가 열려도 가장 오래된 세대의 tradeId가 가지치기되지 않는다")
    void durable_전에는_가지치기하지_않는다() throws InterruptedException {
        RecordingSnapshotSink sink = new RecordingSnapshotSink(); // durableSeq 기본 0 — 아무것도 durable 아님
        engine = new AccountEngine(1024, new BlockingWaitStrategy(), ProducerType.SINGLE, NODE_ID,
            NO_OP_SENDER, NoOpAccountResultListener.INSTANCE, new com.flab.stocktradingengine.account.disruptor.journal.InMemoryAccountJournal(), sink);
        engine.seed(1L, new BigDecimal("1000000"), new BigDecimal("0.40"));
        engine.start();

        engine.publishBuy(1L, STOCK, new BigDecimal("10000"), 3, "r1");
        awaitAccountSeq(1L, 1L);
        long orderId = orderId(1);
        engine.publishBuyFill(9001L, orderId, 1L, STOCK, new BigDecimal("10000"), 1, 100L); // 첫 세대에 tradeId 기록

        // 세대 경계를 두 번 넘긴다(2 × N건 이상) — durableSeq가 계속 0이라 가지치기는 안 일어나야 한다.
        fillDummyEvents(2 * SNAPSHOT_INTERVAL);
        awaitOfferCount(sink, 2);

        // 같은 tradeId 재도착 — durable 전이라 두 세대 전이어도 여전히 멱등(무시)돼야 한다.
        // 무시되면 balance는 안 바뀌므로, 그 대신 lastAppliedFillPosition으로 "이 이벤트를 소비자가
        // 처리했다"는 사실 자체를 기다린다(1-2).
        BigDecimal balanceBefore = engine.accountState(1L).balance();
        engine.publishBuyFill(9001L, orderId, 1L, STOCK, new BigDecimal("10000"), 1, 200L);
        awaitFillPosition(200L);
        assertEquals(0, engine.accountState(1L).balance().compareTo(balanceBefore),
            "durable 전이므로 재도착이 무시돼 잔고가 안 바뀌어야 한다");
    }

    @Test
    @DisplayName("싱크가 durable을 보고하면 두 세대 전 tradeId는 가지치기되어 재도착 시 다시 반영된다")
    void durable_보고되면_두세대전_가지치기된다() throws InterruptedException {
        RecordingSnapshotSink sink = new RecordingSnapshotSink();
        engine = new AccountEngine(1024, new BlockingWaitStrategy(), ProducerType.SINGLE, NODE_ID,
            NO_OP_SENDER, NoOpAccountResultListener.INSTANCE, new com.flab.stocktradingengine.account.disruptor.journal.InMemoryAccountJournal(), sink);
        engine.seed(1L, new BigDecimal("1000000"), new BigDecimal("0.40"));
        engine.start();

        engine.publishBuy(1L, STOCK, new BigDecimal("10000"), 5, "r1");
        awaitAccountSeq(1L, 1L);
        long orderId = orderId(1);
        engine.publishBuyFill(9001L, orderId, 1L, STOCK, new BigDecimal("10000"), 2, 100L); // 첫 세대(가장 오래됨이 될 세대)에 기록

        fillDummyEvents(2 * SNAPSHOT_INTERVAL); // 세대 경계 두 번 → 3세대 존재
        awaitOfferCount(sink, 2);

        sink.durableSeq.set(sink.offers.get(0).appliedSeq()); // 첫 스냅샷이 durable해졌다고 보고

        // pruneIfDurable은 "이번 이벤트를 다 처리한 뒤"에 실행된다(다음 이벤트의 가지치기 여부를
        // 정하는 것이지, durableSeq를 올린 그 순간 즉시 과거로 소급 적용되지 않는다) — 그래서
        // durableSeq를 올린 뒤 진짜(중복 아닌) 체결을 하나 흘려보내 가지치기를 한 번 통과시킨다.
        engine.publishBuyFill(9002L, orderId, 1L, STOCK, new BigDecimal("10000"), 1, 250L);
        awaitFillPosition(250L);

        BigDecimal balanceBeforeReplay = engine.accountState(1L).balance();
        engine.publishBuyFill(9001L, orderId, 1L, STOCK, new BigDecimal("10000"), 1, 300L); // 같은 tradeId, 남은 잔량 2
        awaitBalanceChanged(balanceBeforeReplay);

        assertFalse(engine.accountState(1L).balance().compareTo(balanceBeforeReplay) == 0,
            "가지치기된 tradeId 재도착은 새 체결로 취급돼 잔고가 바뀌어야 한다");
    }

    @Test
    @DisplayName("snapshotTriggerEnabled=false면 N건 경계를 넘겨도 싱크에 offer하지 않는다 (39 리뷰 비블로킹 ① — 복구 replay 전용 경로)")
    void snapshotTriggerEnabled_false면_offer_안_한다() {
        RecordingSnapshotSink sink = new RecordingSnapshotSink();
        Map<Long, AccountState> accounts = new HashMap<>();
        accounts.put(1L, new AccountState(1L, new BigDecimal("1000000"), new BigDecimal("0.40")));
        AccountEventHandler handler = new AccountEventHandler(accounts, new AccountOrderIdGenerator(NODE_ID),
            NO_OP_SENDER, NoOpAccountResultListener.INSTANCE, sink, false);

        AccountEvent scratch = new AccountEvent();
        for (long i = 0; i < SNAPSHOT_INTERVAL * 2; i++) {
            scratch.setBuy(1L, STOCK, new BigDecimal("10000"), 1, "r" + i, 0L);
            handler.onEvent(scratch, i, false);
        }

        assertTrue(sink.offers.isEmpty(), "트리거를 꺼두면 N건 경계를 두 번 넘어가도 offer가 없어야 한다(복구 replay 낭비 제거)");
    }

    /** 이미 처리된 requestId("r1")를 반복 재전송해 appliedSeq만 밀어 올린다 — 매번 빠른 dup 경로(빠름). */
    private void fillDummyEvents(long count) {
        for (long i = 0; i < count; i++) {
            engine.publishBuy(1L, STOCK, new BigDecimal("10000"), 1, "r1");
        }
    }

    private void awaitOfferCount(RecordingSnapshotSink sink, int expected) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (sink.offers.size() < expected) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("10초 안에 offer가 " + expected + "건 도착하지 않음(현재 " + sink.offers.size() + "건)");
            }
            Thread.onSpinWait();
        }
    }

    private void awaitAccountSeq(long accountId, long expectedSeq) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (engine.accountState(accountId).seq() < expectedSeq) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("2초 안에 seq가 " + expectedSeq + "에 도달하지 않음");
            }
            Thread.onSpinWait();
        }
    }

    private void awaitFillPosition(long expected) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (engine.lastAppliedFillPositions().getOrDefault(0, 0L) < expected) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("2초 안에 lastAppliedFillPositions이 " + expected + "에 도달하지 않음");
            }
            Thread.onSpinWait();
        }
    }

    private void awaitBalanceChanged(BigDecimal before) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (engine.accountState(1L).balance().compareTo(before) == 0) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("2초 안에 잔고가 바뀌지 않음");
            }
            Thread.onSpinWait();
        }
    }

    /** AccountOrderIdGenerator의 결정론 규칙((nodeId&lt;&lt;53)|counter, nodeId=0)으로 orderId를 예측한다. */
    private static long orderId(long counter) {
        return (NODE_ID << 53) | counter;
    }

    /** offer된 스냅샷을 그대로 기록하는 테스트용 싱크. durableSeq는 테스트가 직접 올린다. */
    private static final class RecordingSnapshotSink implements AccountSnapshotSink {
        private final List<Offer> offers = new CopyOnWriteArrayList<>();
        private final AtomicLong durableSeq = new AtomicLong(0L);

        @Override
        public boolean offer(byte[] snapshotBytes, Map<Integer, Long> fillPositions, long appliedSeq) {
            offers.add(new Offer(snapshotBytes, fillPositions, appliedSeq));
            return true;
        }

        @Override
        public long durableSeq() {
            return durableSeq.get();
        }

        private record Offer(byte[] snapshotBytes, Map<Integer, Long> fillPositions, long appliedSeq) {
        }
    }
}
