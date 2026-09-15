package com.flab.stocktradingengine.account.disruptor.snapshot;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.flab.stocktradingengine.account.disruptor.snapshot.AccountSnapshot;
import com.flab.stocktradingengine.account.disruptor.snapshot.AccountSnapshotCodec;
import com.flab.stocktradingengine.account.disruptor.snapshot.AccountStateSnapshot;
import com.flab.stocktradingengine.account.disruptor.snapshot.BuyReservationSnapshot;
import com.flab.stocktradingengine.account.disruptor.snapshot.SellReservationSnapshot;

/**
 * 2d-2a — {@link AccountSnapshotCodec} 인코딩·디코딩 단위 검증. {@link AccountEngineSnapshotTest}가
 * 엔진을 통한 end-to-end 왕복을 보는 반면, 여기서는 코덱 자체가 다중 계좌·다중 예약·빈 스냅샷 같은
 * 경계에서 오프셋을 정확히 계산하는지를 본다. matching {@code MatchingSnapshotCodecTest}와 같은 결.
 */
class AccountSnapshotCodecTest {

    private static final String STOCK = "005930";

    private final AccountSnapshotCodec codec = new AccountSnapshotCodec();

    @Test
    @DisplayName("빈 스냅샷도 왕복된다")
    void 빈_스냅샷_왕복() {
        AccountSnapshot snapshot = new AccountSnapshot(Map.of(), 7L, 12345L);

        AccountSnapshot decoded = codec.decode(codec.encode(snapshot));

        assertTrue(decoded.accountsById().isEmpty());
        assertEquals(7L, decoded.generatorCounter());
        assertEquals(12345L, decoded.journalPosition());
    }

    @Test
    @DisplayName("여러 계좌·여러 예약·멱등 캐시·requestId 집합이 뒤섞여도 정확히 왕복된다")
    void 다중_계좌_다중_예약_왕복() {
        AccountStateSnapshot account1 = new AccountStateSnapshot(
            1L, 42L, new BigDecimal("1000000"), new BigDecimal("0.40"),
            Map.of(10L, new BuyReservationSnapshot(new BigDecimal("70000"), 3)),
            Map.of(20L, new SellReservationSnapshot(STOCK, 5)),
            Map.of(STOCK, 10),
            Set.of(9001L, 9002L),
            Set.of(8001L),
            Set.of("r1", "r2"),
            new BigDecimal("1500"));

        AccountStateSnapshot account2 = new AccountStateSnapshot(
            2L, 0L, new BigDecimal("500000"), new BigDecimal("1.00"),
            Map.of(), Map.of(), Map.of(),
            Set.of(), Set.of(), Set.of(),
            BigDecimal.ZERO);

        AccountSnapshot snapshot = new AccountSnapshot(Map.of(1L, account1, 2L, account2), 99L, 555L);

        AccountSnapshot decoded = codec.decode(codec.encode(snapshot));

        assertEquals(99L, decoded.generatorCounter());
        assertEquals(555L, decoded.journalPosition());
        assertEquals(2, decoded.accountsById().size());

        AccountStateSnapshot decodedAccount1 = decoded.accountsById().get(1L);
        assertEquals(account1.seq(), decodedAccount1.seq());
        assertEquals(0, account1.balance().compareTo(decodedAccount1.balance()));
        assertEquals(0, account1.unpaid().compareTo(decodedAccount1.unpaid()));
        assertEquals(account1.holdings(), decodedAccount1.holdings());
        assertEquals(account1.processedTradeIds(), decodedAccount1.processedTradeIds());
        assertEquals(account1.processedSettlementRefs(), decodedAccount1.processedSettlementRefs());
        assertEquals(account1.processedRequestIds(), decodedAccount1.processedRequestIds());
        assertEquals(account1.reservations().get(10L), decodedAccount1.reservations().get(10L));
        assertEquals(account1.sellReservations().get(20L), decodedAccount1.sellReservations().get(20L));

        AccountStateSnapshot decodedAccount2 = decoded.accountsById().get(2L);
        assertTrue(decodedAccount2.reservations().isEmpty());
        assertTrue(decodedAccount2.holdings().isEmpty());
    }
}
