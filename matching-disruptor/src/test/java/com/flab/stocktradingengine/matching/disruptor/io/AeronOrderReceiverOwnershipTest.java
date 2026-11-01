package com.flab.stocktradingengine.matching.disruptor.io;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.IntPredicate;

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.flab.stocktradingengine.aeron.StockSlotHasher;
import com.flab.stocktradingengine.codec.EventType;
import com.flab.stocktradingengine.codec.JournaledOrder;
import com.flab.stocktradingengine.codec.OrderCodec;
import com.flab.stocktradingengine.matching.disruptor.engine.MatchingEngine;
import com.flab.stocktradingengine.trading.entity.OrderSide;

/**
 * 수신 때 담당 확인. 실제 Subscription 없이 {@code onFragment}를 직접 부르고, 담당 주문은 매칭 엔진에
 * 들어가 체결되는지(= {@code publishPlace}가 불렸는지), 담당이 아닌 주문은 엔진에 들어가지 않는지를
 * 체결 유무로 확인한다.
 */
class AeronOrderReceiverOwnershipTest {

    private static final StockSlotHasher STOCK_SLOT_HASHER = new StockSlotHasher(64);
    private static final String OWNED_STOCK = "005930";
    private static final String NOT_OWNED_STOCK = "000660";

    private final OrderCodec codec = new OrderCodec();
    private final List<String> filledStockCodes = new CopyOnWriteArrayList<>();
    private final List<JournaledOrder> notOwnedOrders = new ArrayList<>();
    private MatchingEngine engine;
    private AeronOrderReceiver receiver;

    @BeforeEach
    void setUp() {
        engine = new MatchingEngine(1024, (stockCode, fill) -> filledStockCodes.add(stockCode));
        engine.start();
        int ownedSlot = STOCK_SLOT_HASHER.slotFor(OWNED_STOCK);
        IntPredicate ownedSlots = slot -> slot == ownedSlot;
        receiver = new AeronOrderReceiver(null, engine, new org.agrona.concurrent.BackoffIdleStrategy(),
            ownedSlots, STOCK_SLOT_HASHER, notOwnedOrders::add);
    }

    @AfterEach
    void tearDown() {
        engine.shutdown();
    }

    @Test
    void 담당_종목_주문은_매칭_엔진에_들어간다() throws Exception {
        deliver(place(1L, OWNED_STOCK, OrderSide.SELL));
        deliver(place(2L, OWNED_STOCK, OrderSide.BUY));

        awaitFills(1);

        assertEquals(List.of(OWNED_STOCK), filledStockCodes);
        assertTrue(notOwnedOrders.isEmpty());
    }

    @Test
    void 담당이_아닌_종목_주문은_매칭_엔진에_들어가지_않는다() throws Exception {
        deliver(place(1L, NOT_OWNED_STOCK, OrderSide.SELL));
        deliver(place(2L, NOT_OWNED_STOCK, OrderSide.BUY));
        // 뒤따르는 담당 종목 주문 한 쌍이 체결된 시점에는 앞의 두 건도 엔진이 이미 처리했을 순서다.
        deliver(place(3L, OWNED_STOCK, OrderSide.SELL));
        deliver(place(4L, OWNED_STOCK, OrderSide.BUY));

        awaitFills(1);

        assertEquals(List.of(OWNED_STOCK), filledStockCodes, "담당이 아닌 종목은 체결되면 안 된다");
    }

    @Test
    void 받은_주문_수와_담당이_아니라_버린_주문_수와_엔진이_낸_체결_수를_센다() throws Exception {
        // 12-3절·8절 H5: 매칭 shard마다 받은 주문 수, 담당 밖이라 버린 주문 수, 낸 체결 수.
        assertEquals(0L, receiver.placeAcceptedCount());
        assertEquals(0L, receiver.notOwnedCount());
        assertEquals(0L, engine.fillCount());

        deliver(place(1L, NOT_OWNED_STOCK, OrderSide.SELL));
        deliver(place(2L, OWNED_STOCK, OrderSide.SELL));
        deliver(place(3L, OWNED_STOCK, OrderSide.BUY));
        deliver(place(4L, NOT_OWNED_STOCK, OrderSide.BUY));
        awaitFills(1);

        assertEquals(2L, receiver.placeAcceptedCount(), "담당 종목 주문만 받은 것으로 센다");
        assertEquals(2L, receiver.notOwnedCount());
        assertEquals(1L, engine.fillCount());
    }

    @Test
    void 담당이_아닌_주문은_예외_없이_지나가고_거부_회신_요청_하나가_만들어진다() {
        JournaledOrder notOwned = place(7L, NOT_OWNED_STOCK, OrderSide.BUY);

        deliver(notOwned);

        assertEquals(1, notOwnedOrders.size());
        assertEquals(7L, notOwnedOrders.get(0).orderId());
        assertEquals(NOT_OWNED_STOCK, notOwnedOrders.get(0).stockCode());
        assertEquals(notOwned.accountId(), notOwnedOrders.get(0).accountId());
    }

    @Test
    void 담당이_아닌_주문을_거부해도_그_주문의_위치는_엔진의_반영_위치에_남는다() throws Exception {
        deliver(place(7L, NOT_OWNED_STOCK, OrderSide.BUY));

        long deadline = System.nanoTime() + 3_000_000_000L;
        while (engine.lastAppliedOrderIntakePositions().isEmpty() && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }

        assertEquals(1, engine.lastAppliedOrderIntakePositions().size(), "거부한 주문의 발행자 위치가 반영되어야 한다");
        assertTrue(filledStockCodes.isEmpty());
    }

    @Test
    void 인테이크_와이어로_온_종목_slot_항목은_엔진에_들어가지_않는다() throws Exception {
        deliver(JournaledOrder.slotAcquired(5));
        deliver(JournaledOrder.slotReleased(5));
        deliver(place(1L, OWNED_STOCK, OrderSide.SELL));
        deliver(place(2L, OWNED_STOCK, OrderSide.BUY));

        awaitFills(1);

        assertEquals(2, engine.journal().entries().size(), "저널에는 주문 두 건만 있어야 한다(받음·넘김은 와이어로 받지 않는다)");
    }

    @Test
    void 수신_thread가_종목_slot_요청을_꺼내_이벤트로_발행한다() throws Exception {
        SlotEventRequests requests = new SlotEventRequests();
        AeronOrderReceiver withRequests = new AeronOrderReceiver(null, engine, new org.agrona.concurrent.BackoffIdleStrategy(),
            slot -> true, STOCK_SLOT_HASHER, notOwnedOrders::add, requests);
        requests.requestAcquire(5);

        int drained = withRequests.drainSlotEvents();

        assertEquals(1, drained);
        long deadline = System.nanoTime() + 3_000_000_000L;
        while (engine.journal().entries().isEmpty() && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        assertEquals(EventType.SLOT_ACQUIRED, engine.journal().entries().get(0).type());
    }

    private JournaledOrder place(long orderId, String stockCode, OrderSide side) {
        return new JournaledOrder(EventType.PLACE, orderId, 100L + orderId, stockCode, side,
            new BigDecimal("70000"), 10, Instant.parse("2026-10-29T00:00:00Z"));
    }

    private void deliver(JournaledOrder order) {
        UnsafeBuffer buffer = new UnsafeBuffer(new byte[256]);
        int length = codec.encode(buffer, 0, order);
        receiver.onFragment(buffer, 0, length, null);
    }

    private void awaitFills(int expected) throws InterruptedException {
        long deadline = System.nanoTime() + 3_000_000_000L;
        while (filledStockCodes.size() < expected && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        Thread.sleep(100);
    }
}
