package com.flab.stocktradingengine.matching.disruptor.io;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.flab.stocktradingengine.codec.EventType;
import com.flab.stocktradingengine.codec.JournaledOrder;
import com.flab.stocktradingengine.matching.disruptor.engine.MatchingEngine;

/**
 * 종목 slot 받음·넘김 요청(U13). 요청은 Kafka 콜백 thread가 넣고, 주문을 링버퍼에 넣는 수신 thread(프로듀서 하나)가 꺼내
 * 이벤트로 발행한다. 링버퍼의 프로듀서가 하나({@code ProducerType.SINGLE})라서 콜백 thread가 직접 발행하지 않는다.
 */
class SlotEventRequestsTest {

    private MatchingEngine engine;

    @AfterEach
    void tearDown() {
        engine.shutdown();
    }

    @Test
    void 다른_thread가_넣은_요청이_넣은_순서대로_이벤트로_발행되어_저널에_기록된다() throws Exception {
        engine = new MatchingEngine(1024, (stockCode, fill) -> { });
        engine.start();
        SlotEventRequests requests = new SlotEventRequests();
        Thread callbackThread = new Thread(() -> {
            requests.requestAcquire(3);
            requests.requestRelease(3);
            requests.requestAcquire(8);
        });
        callbackThread.start();
        callbackThread.join();

        int drained = requests.drainTo(engine);

        assertEquals(3, drained);
        awaitJournalSize(3);
        List<JournaledOrder> entries = engine.journal().entries();
        assertEquals(EventType.SLOT_ACQUIRED, entries.get(0).type());
        assertEquals(3, entries.get(0).stockSlot());
        assertEquals(EventType.SLOT_RELEASED, entries.get(1).type());
        assertEquals(3, entries.get(1).stockSlot());
        assertEquals(EventType.SLOT_ACQUIRED, entries.get(2).type());
        assertEquals(8, entries.get(2).stockSlot());
    }

    @Test
    void 적재_요청도_수신_thread가_발행하고_같은_반복에서_완료_확인을_한다() throws Exception {
        java.util.List<Long> durableNotices = new java.util.concurrent.CopyOnWriteArrayList<>();
        java.util.concurrent.atomic.AtomicLong durable = new java.util.concurrent.atomic.AtomicLong();
        java.util.List<Long> offeredSeqs = new java.util.concurrent.CopyOnWriteArrayList<>();
        MatchingSnapshotSink sink = new MatchingSnapshotSink() {
            @Override
            public boolean offer(byte[] snapshotBytes, java.util.Map<Integer, Long> orderIntakePositions, long appliedSeq) {
                offeredSeqs.add(appliedSeq);
                return true;
            }

            @Override
            public long durableSeq() {
                return durable.get();
            }
        };
        engine = new MatchingEngine(1024, new com.lmax.disruptor.BlockingWaitStrategy(), (stockCode, fill) -> { },
            new com.flab.stocktradingengine.matching.disruptor.journal.InMemoryJournal(), sink, true,
            new com.flab.stocktradingengine.time.StageLatencyRegistry(false),
            new com.flab.stocktradingengine.aeron.StockSlotHasher(64), new SlotEventListener() {
                @Override
                public void onBooksLoadedDurable(long transferId, int stockSlot) {
                    durableNotices.add(transferId);
                }
            });
        engine.start();
        SlotEventRequests requests = new SlotEventRequests();
        Thread zReceiveThread = new Thread(() -> requests.requestLoad(99L, 5, List.of()));
        zReceiveThread.start();
        zReceiveThread.join();

        requests.drainTo(engine);
        long deadline = System.nanoTime() + 3_000_000_000L;
        while (offeredSeqs.isEmpty() && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        assertEquals(1, offeredSeqs.size(), "적재 뒤 강제 스냅샷이 찍혀야 한다");
        durable.set(offeredSeqs.get(0));
        requests.drainTo(engine);

        assertEquals(List.of(99L), durableNotices);
    }

    @Test
    void 요청이_없으면_발행하지_않고_0을_돌려준다() {
        engine = new MatchingEngine(1024, (stockCode, fill) -> { });
        engine.start();

        assertEquals(0, new SlotEventRequests().drainTo(engine));
        assertTrue(engine.journal().entries().isEmpty());
    }

    private void awaitJournalSize(int size) throws InterruptedException {
        long deadline = System.nanoTime() + 3_000_000_000L;
        while (engine.journal().entries().size() < size && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        assertTrue(engine.journal().entries().size() >= size, "3초 안에 저널에 " + size + "건이 기록되지 않음");
    }
}
