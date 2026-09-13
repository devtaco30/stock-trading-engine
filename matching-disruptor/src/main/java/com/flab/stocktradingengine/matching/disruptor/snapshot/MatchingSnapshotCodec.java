package com.flab.stocktradingengine.matching.disruptor.snapshot;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import com.flab.stocktradingengine.trading.entity.OrderSide;

/**
 * {@link MatchingSnapshot}을 바이트로 바꾸는 수동 코덱(2d-1a). core.codec의 {@code OrderCodec}과
 * 달리 이 포맷은 matching 엔진 안에서만 쓰는 것이라(파일로 디스크에 남을 뿐, account 등 다른
 * 모듈과 주고받는 와이어 계약이 아니다) matching-disruptor에 둔다.
 *
 * <p>스냅샷 크기(종목·미체결 주문 수)가 고정돼 있지 않아 {@link ExpandableArrayBuffer}(필요하면
 * 자동으로 커지는 버퍼)로 인코딩한다 — {@code OrderCodec}·{@code AccountJournalEntryCodec}처럼
 * 고정 크기를 미리 잡아둘 수 없다.</p>
 *
 * <h3>레이아웃 (running offset, 앞에서 뒤로)</h3>
 * <pre>
 * journalPosition:8, stockCount:4,
 * (stockCode:4+N, restingOrderCount:4,
 *   (orderId:8, accountId:8, side:1, priceUnscaled:8, priceScale:4,
 *    quantity:4, orderAtEpochMillis:8, filledQuantity:4, cancelled:1) × restingOrderCount,
 *  filledTimestampCount:4, (orderId:8, epochMillis:8) × filledTimestampCount) × stockCount
 * </pre>
 */
public final class MatchingSnapshotCodec {

    public byte[] encode(MatchingSnapshot snapshot) {
        MutableDirectBuffer buffer = new ExpandableArrayBuffer();
        int length = encode(buffer, 0, snapshot);
        byte[] bytes = new byte[length];
        buffer.getBytes(0, bytes);
        return bytes;
    }

    public MatchingSnapshot decode(byte[] bytes) {
        return decode(new UnsafeBuffer(bytes), 0);
    }

    private int encode(MutableDirectBuffer buffer, int offset, MatchingSnapshot snapshot) {
        int position = offset;

        buffer.putLong(position, snapshot.journalPosition());
        position += Long.BYTES;
        buffer.putInt(position, snapshot.booksByStock().size());
        position += Integer.BYTES;

        for (Map.Entry<String, BookSnapshot> entry : snapshot.booksByStock().entrySet()) {
            position += buffer.putStringAscii(position, entry.getKey());
            position = encodeBookSnapshot(buffer, position, entry.getValue());
        }
        return position - offset;
    }

    private int encodeBookSnapshot(MutableDirectBuffer buffer, int offset, BookSnapshot bookSnapshot) {
        int position = offset;

        List<RestingOrder> restingOrders = bookSnapshot.restingOrders();
        buffer.putInt(position, restingOrders.size());
        position += Integer.BYTES;
        for (RestingOrder restingOrder : restingOrders) {
            position = encodeRestingOrder(buffer, position, restingOrder);
        }

        Map<Long, Long> filledTimestamps = bookSnapshot.filledOrderTimestampsEpochMillis();
        buffer.putInt(position, filledTimestamps.size());
        position += Integer.BYTES;
        for (Map.Entry<Long, Long> entry : filledTimestamps.entrySet()) {
            buffer.putLong(position, entry.getKey());
            position += Long.BYTES;
            buffer.putLong(position, entry.getValue());
            position += Long.BYTES;
        }
        return position;
    }

    private int encodeRestingOrder(MutableDirectBuffer buffer, int offset, RestingOrder restingOrder) {
        int position = offset;

        buffer.putLong(position, restingOrder.orderId());
        position += Long.BYTES;
        buffer.putLong(position, restingOrder.accountId());
        position += Long.BYTES;
        buffer.putByte(position, (byte) restingOrder.side().ordinal());
        position += Byte.BYTES;

        BigDecimal price = restingOrder.price();
        buffer.putLong(position, price.unscaledValue().longValueExact());
        position += Long.BYTES;
        buffer.putInt(position, price.scale());
        position += Integer.BYTES;

        buffer.putInt(position, restingOrder.quantity());
        position += Integer.BYTES;
        buffer.putLong(position, restingOrder.orderAtEpochMillis());
        position += Long.BYTES;
        buffer.putInt(position, restingOrder.filledQuantity());
        position += Integer.BYTES;
        buffer.putByte(position, (byte) (restingOrder.cancelled() ? 1 : 0));
        position += Byte.BYTES;

        return position;
    }

    private MatchingSnapshot decode(DirectBuffer buffer, int offset) {
        int position = offset;

        long journalPosition = buffer.getLong(position);
        position += Long.BYTES;
        int stockCount = buffer.getInt(position);
        position += Integer.BYTES;

        Map<String, BookSnapshot> booksByStock = new HashMap<>();
        for (int i = 0; i < stockCount; i++) {
            String stockCode = buffer.getStringAscii(position);
            position += Integer.BYTES + stockCode.length();
            DecodeResult<BookSnapshot> result = decodeBookSnapshot(buffer, position);
            booksByStock.put(stockCode, result.value());
            position = result.nextOffset();
        }
        return new MatchingSnapshot(booksByStock, journalPosition);
    }

    private DecodeResult<BookSnapshot> decodeBookSnapshot(DirectBuffer buffer, int offset) {
        int position = offset;

        int restingOrderCount = buffer.getInt(position);
        position += Integer.BYTES;
        List<RestingOrder> restingOrders = new ArrayList<>(restingOrderCount);
        for (int i = 0; i < restingOrderCount; i++) {
            DecodeResult<RestingOrder> result = decodeRestingOrder(buffer, position);
            restingOrders.add(result.value());
            position = result.nextOffset();
        }

        int filledTimestampCount = buffer.getInt(position);
        position += Integer.BYTES;
        Map<Long, Long> filledTimestamps = new HashMap<>();
        for (int i = 0; i < filledTimestampCount; i++) {
            long orderId = buffer.getLong(position);
            position += Long.BYTES;
            long epochMillis = buffer.getLong(position);
            position += Long.BYTES;
            filledTimestamps.put(orderId, epochMillis);
        }

        return new DecodeResult<>(new BookSnapshot(restingOrders, filledTimestamps), position);
    }

    private DecodeResult<RestingOrder> decodeRestingOrder(DirectBuffer buffer, int offset) {
        int position = offset;

        long orderId = buffer.getLong(position);
        position += Long.BYTES;
        long accountId = buffer.getLong(position);
        position += Long.BYTES;
        OrderSide side = OrderSide.values()[buffer.getByte(position)];
        position += Byte.BYTES;

        long unscaledPrice = buffer.getLong(position);
        position += Long.BYTES;
        int priceScale = buffer.getInt(position);
        position += Integer.BYTES;
        BigDecimal price = BigDecimal.valueOf(unscaledPrice, priceScale);

        int quantity = buffer.getInt(position);
        position += Integer.BYTES;
        long orderAtEpochMillis = buffer.getLong(position);
        position += Long.BYTES;
        int filledQuantity = buffer.getInt(position);
        position += Integer.BYTES;
        boolean cancelled = buffer.getByte(position) != 0;
        position += Byte.BYTES;

        RestingOrder restingOrder = new RestingOrder(
            orderId, accountId, side, price, quantity, orderAtEpochMillis, filledQuantity, cancelled);
        return new DecodeResult<>(restingOrder, position);
    }

    /** 디코딩한 값과, 그다음 필드를 읽어야 할 오프셋을 함께 돌려준다 — 가변 길이 필드가 섞여 있어 호출부가 오프셋을 직접 계산할 수 없다. */
    private record DecodeResult<T>(T value, int nextOffset) {
    }
}
