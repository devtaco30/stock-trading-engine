package com.flab.stocktradingengine.codec;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.util.Optional;

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 2b-1b — 계좌 저널 엔트리를 Aeron Archive 로 내보내기 전 바이트로 바꾸는 코덱의 라운드트립 검증.
 *
 * <p>다섯 타입 모두 필드 부재 패턴이 다르다({@code account.disruptor.AccountEvent}가 타입별로
 * 일부 필드만 채우고 나머지는 {@code clear()} 로 비워둔 상태라서) — price·stockCode·requestId 가
 * null 인 채로도 왕복해야 한다.</p>
 */
class AccountJournalEntryCodecTest {

    private static final String STOCK = "005930";
    private final AccountJournalEntryCodec codec = new AccountJournalEntryCodec();

    @Test
    @DisplayName("BUY 엔트리 — orderId·tradeId=0, 나머지 필드 전부 있음")
    void BUY_라운드트립() {
        AccountJournalEntry entry = new AccountJournalEntry(
            AccountEventType.BUY, 0L, 1L, STOCK, new BigDecimal("10000.50"), 10, "r1", 0L);

        AccountJournalEntry decoded = roundTrip(entry);

        assertEquals(AccountEventType.BUY, decoded.type());
        assertEquals(0L, decoded.orderId());
        assertEquals(1L, decoded.accountId());
        assertEquals(STOCK, decoded.stockCode());
        assertEquals(0, new BigDecimal("10000.50").compareTo(decoded.price()));
        assertEquals(10, decoded.quantity());
        assertEquals("r1", decoded.requestId());
        assertEquals(0L, decoded.tradeId());
    }

    @Test
    @DisplayName("SELL 엔트리도 BUY와 같은 필드 구성으로 왕복한다")
    void SELL_라운드트립() {
        AccountJournalEntry entry = new AccountJournalEntry(
            AccountEventType.SELL, 0L, 1L, STOCK, new BigDecimal("10000.50"), 4, "r2", 0L);

        AccountJournalEntry decoded = roundTrip(entry);

        assertEquals(AccountEventType.SELL, decoded.type());
        assertEquals(STOCK, decoded.stockCode());
        assertEquals(0, new BigDecimal("10000.50").compareTo(decoded.price()));
        assertEquals(4, decoded.quantity());
        assertEquals("r2", decoded.requestId());
    }

    @Test
    @DisplayName("BUY_FILL 엔트리 — orderId·tradeId 확정값, requestId는 null")
    void BUY_FILL_라운드트립_requestId_null() {
        AccountJournalEntry entry = new AccountJournalEntry(
            AccountEventType.BUY_FILL, 5L, 1L, STOCK, new BigDecimal("10000"), 10, null, 9001L);

        AccountJournalEntry decoded = roundTrip(entry);

        assertEquals(AccountEventType.BUY_FILL, decoded.type());
        assertEquals(5L, decoded.orderId());
        assertEquals(9001L, decoded.tradeId());
        assertEquals(STOCK, decoded.stockCode());
        assertEquals(0, new BigDecimal("10000").compareTo(decoded.price()));
        assertNull(decoded.requestId());
    }

    @Test
    @DisplayName("SELL_FILL 엔트리 — AccountEvent.setSellFill이 price를 안 채우므로 price도 null")
    void SELL_FILL_라운드트립_price도_null() {
        AccountJournalEntry entry = new AccountJournalEntry(
            AccountEventType.SELL_FILL, 6L, 1L, STOCK, null, 4, null, 9002L);

        AccountJournalEntry decoded = roundTrip(entry);

        assertEquals(AccountEventType.SELL_FILL, decoded.type());
        assertEquals(6L, decoded.orderId());
        assertEquals(9002L, decoded.tradeId());
        assertEquals(4, decoded.quantity());
        assertNull(decoded.price());
        assertNull(decoded.requestId());
    }

    @Test
    @DisplayName("SETTLEMENT 엔트리 — stockCode·requestId null, tradeId는 settlementRef")
    void SETTLEMENT_라운드트립_stockCode_null() {
        AccountJournalEntry entry = new AccountJournalEntry(
            AccountEventType.SETTLEMENT, 0L, 1L, null, new BigDecimal("1.00"), 0, null, 7001L);

        AccountJournalEntry decoded = roundTrip(entry);

        assertEquals(AccountEventType.SETTLEMENT, decoded.type());
        assertEquals(1L, decoded.accountId());
        assertNull(decoded.stockCode());
        assertEquals(0, new BigDecimal("1.00").compareTo(decoded.price()));
        assertEquals(0, decoded.quantity());
        assertNull(decoded.requestId());
        assertEquals(7001L, decoded.tradeId());
    }

    @Test
    @DisplayName("0이 아닌 오프셋에서도 인코딩·디코딩이 맞는다")
    void 오프셋_있어도_라운드트립() {
        UnsafeBuffer buffer = new UnsafeBuffer(new byte[256]);
        AccountJournalEntry entry = new AccountJournalEntry(
            AccountEventType.BUY, 0L, 1L, STOCK, new BigDecimal("10000"), 10, "r3", 0L);

        codec.encode(buffer, 13, entry);
        AccountJournalEntry decoded = codec.decode(buffer, 13);

        assertEquals(1L, decoded.accountId());
        assertEquals("r3", decoded.requestId());
    }

    @Test
    @DisplayName("정상 바이트는 tryDecode도 decode와 같은 값을 돌려준다")
    void tryDecode_정상바이트는_값을_돌려준다() {
        UnsafeBuffer buffer = new UnsafeBuffer(new byte[256]);
        AccountJournalEntry entry = new AccountJournalEntry(
            AccountEventType.BUY, 0L, 1L, STOCK, new BigDecimal("10000"), 10, "r4", 0L);
        codec.encode(buffer, 0, entry);

        Optional<AccountJournalEntry> decoded = codec.tryDecode(buffer, 0);

        assertTrue(decoded.isPresent());
        assertEquals("r4", decoded.get().requestId());
    }

    @Test
    @DisplayName("content-poison — type 바이트가 AccountEventType 범위를 벗어나면 tryDecode는 예외 대신 빈 값을 돌려준다")
    void tryDecode_손상된_type바이트는_빈값() {
        UnsafeBuffer buffer = new UnsafeBuffer(new byte[256]);
        AccountJournalEntry entry = new AccountJournalEntry(
            AccountEventType.BUY, 0L, 1L, STOCK, new BigDecimal("10000"), 10, "r5", 0L);
        codec.encode(buffer, 0, entry);
        buffer.putByte(0, (byte) 99); // AccountEventType.values().length(5)를 벗어난 값

        Optional<AccountJournalEntry> decoded = codec.tryDecode(buffer, 0);

        assertFalse(decoded.isPresent());
    }

    private AccountJournalEntry roundTrip(AccountJournalEntry entry) {
        UnsafeBuffer buffer = new UnsafeBuffer(new byte[256]);
        int length = codec.encode(buffer, 0, entry);
        AccountJournalEntry decoded = codec.decode(buffer, 0);
        assertEquals(length, codec.encode(buffer, 0, decoded)); // 재인코딩 길이도 같아야 한다
        return decoded;
    }
}
