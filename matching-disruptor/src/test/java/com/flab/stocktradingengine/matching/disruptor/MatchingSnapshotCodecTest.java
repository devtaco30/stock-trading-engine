package com.flab.stocktradingengine.matching.disruptor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.flab.stocktradingengine.matching.disruptor.snapshot.BookSnapshot;
import com.flab.stocktradingengine.matching.disruptor.snapshot.MatchingSnapshot;
import com.flab.stocktradingengine.matching.disruptor.snapshot.MatchingSnapshotCodec;
import com.flab.stocktradingengine.matching.disruptor.snapshot.RestingOrder;
import com.flab.stocktradingengine.trading.entity.OrderSide;

/**
 * 2d-1a — {@link MatchingSnapshotCodec} 인코딩·디코딩 단위 검증. {@link MatchingEngineSnapshotTest}가
 * 엔진을 통한 end-to-end 왕복을 보는 반면, 여기서는 코덱 자체가 다중 종목·다중 주문·빈 스냅샷 같은
 * 경계에서 오프셋을 정확히 계산하는지(스택으로 쌓이는 가변 길이 필드가 많아 밀릴 위험이 크다)를 본다.
 */
class MatchingSnapshotCodecTest {

    private final MatchingSnapshotCodec codec = new MatchingSnapshotCodec();

    @Test
    @DisplayName("빈 스냅샷도 왕복된다")
    void 빈_스냅샷_왕복() {
        MatchingSnapshot snapshot = new MatchingSnapshot(Map.of(), 12345L);

        MatchingSnapshot decoded = codec.decode(codec.encode(snapshot));

        assertTrue(decoded.booksByStock().isEmpty());
        assertEquals(12345L, decoded.journalPosition());
    }

    @Test
    @DisplayName("여러 종목·여러 미체결 주문·멱등 캐시가 뒤섞여도 정확히 왕복된다")
    void 다중_종목_다중_주문_왕복() {
        RestingOrder samsungBuy = new RestingOrder(1L, 100L, OrderSide.BUY, new BigDecimal("70000"), 10, 1_700_000_000_000L, 3, false);
        RestingOrder samsungSell = new RestingOrder(2L, 200L, OrderSide.SELL, new BigDecimal("71000"), 5, 1_700_000_000_001L, 0, false);
        RestingOrder skhynixBuy = new RestingOrder(3L, 300L, OrderSide.BUY, new BigDecimal("120000"), 7, 1_700_000_000_002L, 0, false);

        BookSnapshot samsungBook = new BookSnapshot(
            List.of(samsungBuy, samsungSell), Map.of(9001L, 1_700_000_000_100L));
        BookSnapshot skhynixBook = new BookSnapshot(
            List.of(skhynixBuy), Map.of());

        MatchingSnapshot snapshot = new MatchingSnapshot(
            Map.of("005930", samsungBook, "000660", skhynixBook), 999L);

        MatchingSnapshot decoded = codec.decode(codec.encode(snapshot));

        assertEquals(999L, decoded.journalPosition());
        assertEquals(2, decoded.booksByStock().size());

        BookSnapshot decodedSamsung = decoded.booksByStock().get("005930");
        assertEquals(2, decodedSamsung.restingOrders().size());
        assertTrue(decodedSamsung.restingOrders().contains(samsungBuy));
        assertTrue(decodedSamsung.restingOrders().contains(samsungSell));
        assertEquals(Map.of(9001L, 1_700_000_000_100L), decodedSamsung.filledOrderTimestampsEpochMillis());

        BookSnapshot decodedSkhynix = decoded.booksByStock().get("000660");
        assertEquals(List.of(skhynixBuy), decodedSkhynix.restingOrders());
        assertTrue(decodedSkhynix.filledOrderTimestampsEpochMillis().isEmpty());
    }

    @Test
    @DisplayName("전량 체결·취소 플래그까지 정확히 왕복된다")
    void filledQuantity_cancelled_플래그_왕복() {
        RestingOrder partiallyFilled = new RestingOrder(1L, 100L, OrderSide.BUY, new BigDecimal("70000"), 10, 1L, 4, false);
        BookSnapshot book = new BookSnapshot(List.of(partiallyFilled), Map.of());
        MatchingSnapshot snapshot = new MatchingSnapshot(Map.of("005930", book), 0L);

        MatchingSnapshot decoded = codec.decode(codec.encode(snapshot));

        RestingOrder decodedOrder = decoded.booksByStock().get("005930").restingOrders().get(0);
        assertEquals(4, decodedOrder.filledQuantity());
        assertEquals(false, decodedOrder.cancelled());
    }
}
