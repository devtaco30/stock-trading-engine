package com.flab.stocktradingengine.codec;

import java.math.BigDecimal;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

/**
 * 2b-1b — {@link AccountJournalEntry}를 Aeron Archive 저널 스트림에 내보내기 전 바이트로 바꾸는
 * 수동 코덱. {@link AccountOrderCodec}과 같은 결(고정 필드 먼저, 가변 문자열은 맨 뒤, price는
 * unscaledValue+scale, 네이티브 바이트 순서)을 따르되, 이 레코드는 타입에 따라 price·stockCode·
 * requestId가 null일 수 있어({@code account.disruptor.AccountEvent}가 타입별로 일부 필드만
 * 채우고 나머지는 비워두기 때문) 필드마다 존재 여부를 표시하는 flags 바이트가 하나 더 붙는다.
 *
 * <h3>메시지 레이아웃 (running offset, 앞에서 뒤로)</h3>
 * <pre>
 * [type:1][flags:1][orderId:8][accountId:8][tradeId:8][quantity:4]
 *   (flags 의 PRICE 비트가 서면)     [priceUnscaled:8][priceScale:4]
 *   (flags 의 STOCK_CODE 비트가 서면) [stockCode:4+N]
 *   (flags 의 REQUEST_ID 비트가 서면) [requestId:4+M]
 * </pre>
 *
 * <p>⚠️ durable 포맷 — {@code type}을 {@link AccountEventType}의 ordinal로 저장한다. 이미 디스크에
 * 기록된 저널이 있는 채로 그 enum의 선언 순서를 바꾸거나 값을 끼워 넣으면 옛 기록을 잘못 디코딩한다.
 * 새 타입을 추가할 땐 반드시 끝에 붙인다.</p>
 */
public final class AccountJournalEntryCodec {

    private static final byte PRICE_PRESENT = 0b0000_0001;
    private static final byte STOCK_CODE_PRESENT = 0b0000_0010;
    private static final byte REQUEST_ID_PRESENT = 0b0000_0100;

    /**
     * 저널 엔트리를 버퍼에 인코딩한다.
     *
     * @return 기록한 바이트 수(= Aeron {@code offer}에 넘길 length)
     */
    public int encode(MutableDirectBuffer buffer, int offset, AccountJournalEntry entry) {
        int position = offset;

        buffer.putByte(position, (byte) entry.type().ordinal());
        position += Byte.BYTES;

        byte flags = flagsFor(entry);
        int flagsPosition = position;
        buffer.putByte(flagsPosition, flags);
        position += Byte.BYTES;

        buffer.putLong(position, entry.orderId());
        position += Long.BYTES;
        buffer.putLong(position, entry.accountId());
        position += Long.BYTES;
        buffer.putLong(position, entry.tradeId());
        position += Long.BYTES;
        buffer.putInt(position, entry.quantity());
        position += Integer.BYTES;

        if (entry.price() != null) {
            long unscaledPrice = entry.price().unscaledValue().longValueExact();
            buffer.putLong(position, unscaledPrice);
            position += Long.BYTES;
            buffer.putInt(position, entry.price().scale());
            position += Integer.BYTES;
        }
        if (entry.stockCode() != null) {
            position += buffer.putStringAscii(position, entry.stockCode());
        }
        if (entry.requestId() != null) {
            position += buffer.putStringAscii(position, entry.requestId());
        }
        return position - offset;
    }

    /** 버퍼에서 저널 엔트리 하나를 디코딩한다. */
    public AccountJournalEntry decode(DirectBuffer buffer, int offset) {
        int position = offset;

        AccountEventType type = AccountEventType.values()[buffer.getByte(position)];
        position += Byte.BYTES;
        byte flags = buffer.getByte(position);
        position += Byte.BYTES;

        long orderId = buffer.getLong(position);
        position += Long.BYTES;
        long accountId = buffer.getLong(position);
        position += Long.BYTES;
        long tradeId = buffer.getLong(position);
        position += Long.BYTES;
        int quantity = buffer.getInt(position);
        position += Integer.BYTES;

        BigDecimal price = null;
        if ((flags & PRICE_PRESENT) != 0) {
            long unscaledPrice = buffer.getLong(position);
            position += Long.BYTES;
            int priceScale = buffer.getInt(position);
            position += Integer.BYTES;
            price = BigDecimal.valueOf(unscaledPrice, priceScale);
        }

        String stockCode = null;
        if ((flags & STOCK_CODE_PRESENT) != 0) {
            int stockCodeLength = buffer.getInt(position);
            stockCode = buffer.getStringAscii(position);
            position += Integer.BYTES + stockCodeLength;
        }

        String requestId = null;
        if ((flags & REQUEST_ID_PRESENT) != 0) {
            requestId = buffer.getStringAscii(position);
        }

        return new AccountJournalEntry(type, orderId, accountId, stockCode, price, quantity, requestId, tradeId);
    }

    private byte flagsFor(AccountJournalEntry entry) {
        byte flags = 0;
        if (entry.price() != null) {
            flags |= PRICE_PRESENT;
        }
        if (entry.stockCode() != null) {
            flags |= STOCK_CODE_PRESENT;
        }
        if (entry.requestId() != null) {
            flags |= REQUEST_ID_PRESENT;
        }
        return flags;
    }
}
