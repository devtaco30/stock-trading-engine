package com.flab.stocktradingengine.account.disruptor.snapshot;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * {@link AccountSnapshot}을 바이트로 바꾸는 수동 코덱(2d-2a). matching
 * {@code MatchingSnapshotCodec}과 같은 근거로 core.codec이 아니라 account-disruptor에 둔다 —
 * 이 포맷은 계좌 엔진 안에서만 쓰는 것이라 account-worker와 주고받는 와이어 계약이 아니다.
 *
 * <p>계좌·예약·보유·멱등 캐시 수가 고정돼 있지 않아 {@link ExpandableArrayBuffer}(필요하면
 * 자동으로 커지는 버퍼)로 인코딩한다. BigDecimal은 {@code AccountJournalEntryCodec}과 같은 방식
 * (unscaledValue:long, scale:int)으로 담는다.</p>
 *
 * <h3>레이아웃 (running offset, 앞에서 뒤로)</h3>
 * <pre>
 * journalPosition:8, generatorCounter:8, accountCount:4,
 * (accountId:8, seq:8, balance(8+4), marginRate(8+4), unpaid(8+4),
 *  reservationCount:4, (orderId:8, price(8+4), remainingQuantity:4) × N,
 *  sellReservationCount:4, (orderId:8, stockCode:4+N, remainingQuantity:4) × N,
 *  holdingCount:4, (stockCode:4+N, quantity:4) × N,
 *  tradeIdGenerationCount:4, (boundarySeq:8, tradeIdCount:4, (tradeId:8) × M) × N,
 *  processedSettlementRefCount:4, (settlementRef:8) × N,
 *  processedRequestIdCount:4, (requestId:4+N) × N) × accountCount
 * </pre>
 */
public final class AccountSnapshotCodec {

    public byte[] encode(AccountSnapshot snapshot) {
        MutableDirectBuffer buffer = new ExpandableArrayBuffer();
        int length = encode(buffer, 0, snapshot);
        byte[] bytes = new byte[length];
        buffer.getBytes(0, bytes);
        return bytes;
    }

    public AccountSnapshot decode(byte[] bytes) {
        return decode(new UnsafeBuffer(bytes), 0).value();
    }

    private int encode(MutableDirectBuffer buffer, int offset, AccountSnapshot snapshot) {
        int position = offset;

        buffer.putLong(position, snapshot.journalPosition());
        position += Long.BYTES;
        buffer.putLong(position, snapshot.generatorCounter());
        position += Long.BYTES;
        buffer.putInt(position, snapshot.accountsById().size());
        position += Integer.BYTES;

        for (Map.Entry<Long, AccountStateSnapshot> entry : snapshot.accountsById().entrySet()) {
            position = encodeAccount(buffer, position, entry.getValue());
        }
        return position - offset;
    }

    private int encodeAccount(MutableDirectBuffer buffer, int offset, AccountStateSnapshot account) {
        int position = offset;

        buffer.putLong(position, account.accountId());
        position += Long.BYTES;
        buffer.putLong(position, account.seq());
        position += Long.BYTES;
        position = encodeBigDecimal(buffer, position, account.balance());
        position = encodeBigDecimal(buffer, position, account.marginRate());
        position = encodeBigDecimal(buffer, position, account.unpaid());

        buffer.putInt(position, account.reservations().size());
        position += Integer.BYTES;
        for (Map.Entry<Long, BuyReservationSnapshot> entry : account.reservations().entrySet()) {
            buffer.putLong(position, entry.getKey());
            position += Long.BYTES;
            position = encodeBigDecimal(buffer, position, entry.getValue().price());
            buffer.putInt(position, entry.getValue().remainingQuantity());
            position += Integer.BYTES;
        }

        buffer.putInt(position, account.sellReservations().size());
        position += Integer.BYTES;
        for (Map.Entry<Long, SellReservationSnapshot> entry : account.sellReservations().entrySet()) {
            buffer.putLong(position, entry.getKey());
            position += Long.BYTES;
            position += buffer.putStringAscii(position, entry.getValue().stockCode());
            buffer.putInt(position, entry.getValue().remainingQuantity());
            position += Integer.BYTES;
        }

        buffer.putInt(position, account.holdings().size());
        position += Integer.BYTES;
        for (Map.Entry<String, Integer> entry : account.holdings().entrySet()) {
            position += buffer.putStringAscii(position, entry.getKey());
            buffer.putInt(position, entry.getValue());
            position += Integer.BYTES;
        }

        buffer.putInt(position, account.tradeIdGenerations().size());
        position += Integer.BYTES;
        for (TradeIdGenerationSnapshot generation : account.tradeIdGenerations()) {
            buffer.putLong(position, generation.boundarySeq());
            position += Long.BYTES;
            buffer.putInt(position, generation.tradeIds().size());
            position += Integer.BYTES;
            for (long tradeId : generation.tradeIds()) {
                buffer.putLong(position, tradeId);
                position += Long.BYTES;
            }
        }

        buffer.putInt(position, account.processedSettlementRefs().size());
        position += Integer.BYTES;
        for (long settlementRef : account.processedSettlementRefs()) {
            buffer.putLong(position, settlementRef);
            position += Long.BYTES;
        }

        buffer.putInt(position, account.processedRequestIds().size());
        position += Integer.BYTES;
        for (String requestId : account.processedRequestIds()) {
            position += buffer.putStringAscii(position, requestId);
        }

        return position;
    }

    private int encodeBigDecimal(MutableDirectBuffer buffer, int offset, BigDecimal value) {
        int position = offset;
        buffer.putLong(position, value.unscaledValue().longValueExact());
        position += Long.BYTES;
        buffer.putInt(position, value.scale());
        position += Integer.BYTES;
        return position;
    }

    private DecodeResult<AccountSnapshot> decode(DirectBuffer buffer, int offset) {
        int position = offset;

        long journalPosition = buffer.getLong(position);
        position += Long.BYTES;
        long generatorCounter = buffer.getLong(position);
        position += Long.BYTES;
        int accountCount = buffer.getInt(position);
        position += Integer.BYTES;

        Map<Long, AccountStateSnapshot> accountsById = new HashMap<>();
        for (int i = 0; i < accountCount; i++) {
            DecodeResult<AccountStateSnapshot> result = decodeAccount(buffer, position);
            accountsById.put(result.value().accountId(), result.value());
            position = result.nextOffset();
        }

        return new DecodeResult<>(new AccountSnapshot(accountsById, generatorCounter, journalPosition), position);
    }

    private DecodeResult<AccountStateSnapshot> decodeAccount(DirectBuffer buffer, int offset) {
        int position = offset;

        long accountId = buffer.getLong(position);
        position += Long.BYTES;
        long seq = buffer.getLong(position);
        position += Long.BYTES;
        DecodeResult<BigDecimal> balanceResult = decodeBigDecimal(buffer, position);
        BigDecimal balance = balanceResult.value();
        position = balanceResult.nextOffset();
        DecodeResult<BigDecimal> marginRateResult = decodeBigDecimal(buffer, position);
        BigDecimal marginRate = marginRateResult.value();
        position = marginRateResult.nextOffset();
        DecodeResult<BigDecimal> unpaidResult = decodeBigDecimal(buffer, position);
        BigDecimal unpaid = unpaidResult.value();
        position = unpaidResult.nextOffset();

        int reservationCount = buffer.getInt(position);
        position += Integer.BYTES;
        Map<Long, BuyReservationSnapshot> reservations = new HashMap<>();
        for (int i = 0; i < reservationCount; i++) {
            long orderId = buffer.getLong(position);
            position += Long.BYTES;
            DecodeResult<BigDecimal> priceResult = decodeBigDecimal(buffer, position);
            position = priceResult.nextOffset();
            int remainingQuantity = buffer.getInt(position);
            position += Integer.BYTES;
            reservations.put(orderId, new BuyReservationSnapshot(priceResult.value(), remainingQuantity));
        }

        int sellReservationCount = buffer.getInt(position);
        position += Integer.BYTES;
        Map<Long, SellReservationSnapshot> sellReservations = new HashMap<>();
        for (int i = 0; i < sellReservationCount; i++) {
            long orderId = buffer.getLong(position);
            position += Long.BYTES;
            String stockCode = buffer.getStringAscii(position);
            position += Integer.BYTES + stockCode.length();
            int remainingQuantity = buffer.getInt(position);
            position += Integer.BYTES;
            sellReservations.put(orderId, new SellReservationSnapshot(stockCode, remainingQuantity));
        }

        int holdingCount = buffer.getInt(position);
        position += Integer.BYTES;
        Map<String, Integer> holdings = new HashMap<>();
        for (int i = 0; i < holdingCount; i++) {
            String stockCode = buffer.getStringAscii(position);
            position += Integer.BYTES + stockCode.length();
            int quantity = buffer.getInt(position);
            position += Integer.BYTES;
            holdings.put(stockCode, quantity);
        }

        int tradeIdGenerationCount = buffer.getInt(position);
        position += Integer.BYTES;
        List<TradeIdGenerationSnapshot> tradeIdGenerations = new ArrayList<>();
        for (int i = 0; i < tradeIdGenerationCount; i++) {
            long boundarySeq = buffer.getLong(position);
            position += Long.BYTES;
            int tradeIdCount = buffer.getInt(position);
            position += Integer.BYTES;
            Set<Long> tradeIds = new HashSet<>();
            for (int j = 0; j < tradeIdCount; j++) {
                tradeIds.add(buffer.getLong(position));
                position += Long.BYTES;
            }
            tradeIdGenerations.add(new TradeIdGenerationSnapshot(boundarySeq, tradeIds));
        }

        int processedSettlementRefCount = buffer.getInt(position);
        position += Integer.BYTES;
        Set<Long> processedSettlementRefs = new HashSet<>();
        for (int i = 0; i < processedSettlementRefCount; i++) {
            processedSettlementRefs.add(buffer.getLong(position));
            position += Long.BYTES;
        }

        int processedRequestIdCount = buffer.getInt(position);
        position += Integer.BYTES;
        Set<String> processedRequestIds = new HashSet<>();
        for (int i = 0; i < processedRequestIdCount; i++) {
            String requestId = buffer.getStringAscii(position);
            position += Integer.BYTES + requestId.length();
            processedRequestIds.add(requestId);
        }

        AccountStateSnapshot account = new AccountStateSnapshot(accountId, seq, balance, marginRate,
            reservations, sellReservations, holdings, tradeIdGenerations, processedSettlementRefs,
            processedRequestIds, unpaid);
        return new DecodeResult<>(account, position);
    }

    private DecodeResult<BigDecimal> decodeBigDecimal(DirectBuffer buffer, int offset) {
        int position = offset;
        long unscaledValue = buffer.getLong(position);
        position += Long.BYTES;
        int scale = buffer.getInt(position);
        position += Integer.BYTES;
        return new DecodeResult<>(BigDecimal.valueOf(unscaledValue, scale), position);
    }

    /** 디코딩한 값과, 그다음 필드를 읽어야 할 오프셋을 함께 돌려준다 — 가변 길이 필드가 섞여 있어 호출부가 오프셋을 직접 계산할 수 없다. */
    private record DecodeResult<T>(T value, int nextOffset) {
    }
}
