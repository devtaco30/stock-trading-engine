package com.flab.stocktradingengine.matching.disruptor.handover;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * 호가창 이동(X→Z)에서 종목 하나의 호가창 바이트를 싣는 응답 메시지 하나(11-6절). 배치는
 * {@code [transferId:8][stockSlot:4][index:4][total:4]} 다음에 종목 하나의 호가창 바이트가 온다.
 *
 * <p>{@code transferId}는 Z가 요청에 정한 번호이고 X가 모든 응답에 그대로 되돌린다. {@code index}는 0부터 센 순번,
 * {@code total}은 이 이동에서 보내는 종목 수다. 종목이 하나도 없는 slot은 {@code total=0}, {@code index=0}인 본문 없는
 * 메시지 하나로 알린다.</p>
 */
public record HandoverMessage(long transferId, int stockSlot, int index, int total, byte[] body) {

    public static final int HEADER_BYTES = Long.BYTES + Integer.BYTES * 3;

    public byte[] encode() {
        ByteBuffer buffer = ByteBuffer.allocate(HEADER_BYTES + body.length);
        buffer.putLong(transferId);
        buffer.putInt(stockSlot);
        buffer.putInt(index);
        buffer.putInt(total);
        buffer.put(body);
        return buffer.array();
    }

    public static HandoverMessage decode(byte[] wire, int offset, int length) {
        if (length < HEADER_BYTES) {
            throw new IllegalArgumentException("호가창 이동 메시지가 앞머리(" + HEADER_BYTES + "바이트)보다 짧다: " + length);
        }
        ByteBuffer buffer = ByteBuffer.wrap(wire, offset, length);
        long transferId = buffer.getLong();
        int stockSlot = buffer.getInt();
        int index = buffer.getInt();
        int total = buffer.getInt();
        byte[] body = new byte[length - HEADER_BYTES];
        buffer.get(body);
        return new HandoverMessage(transferId, stockSlot, index, total, body);
    }

    // record의 기본 equals는 배열을 참조로 비교하므로 본문 내용을 비교하도록 다시 정의한다.
    @Override
    public boolean equals(Object other) {
        return other instanceof HandoverMessage that
            && transferId == that.transferId && stockSlot == that.stockSlot
            && index == that.index && total == that.total && Arrays.equals(body, that.body);
    }

    @Override
    public int hashCode() {
        return 31 * Arrays.hashCode(body) + Long.hashCode(transferId) + stockSlot + index + total;
    }

    @Override
    public String toString() {
        return "HandoverMessage[transferId=" + transferId + ", stockSlot=" + stockSlot + ", index=" + index
            + ", total=" + total + ", bodyBytes=" + body.length + "]";
    }
}
