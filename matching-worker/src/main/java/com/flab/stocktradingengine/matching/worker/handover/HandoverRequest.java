package com.flab.stocktradingengine.matching.worker.handover;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * 호가창 이동을 받는 쪽(Z)이 이전 담당(X)에게 보내는 요청(스트림 2008). 배치는
 * {@code [transferId:8][stockSlot:4][replyChannelLength:4][replyChannel:N]}이다. {@code replyChannel}은 응답(스트림 2009)을
 * 받을 Z의 주문 입구 주소(Aeron 채널 문자열)다.
 */
public record HandoverRequest(long transferId, int stockSlot, String replyChannel) {

    public byte[] encode() {
        byte[] channel = replyChannel.getBytes(StandardCharsets.US_ASCII);
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES + Integer.BYTES + Integer.BYTES + channel.length);
        buffer.putLong(transferId);
        buffer.putInt(stockSlot);
        buffer.putInt(channel.length);
        buffer.put(channel);
        return buffer.array();
    }

    public static HandoverRequest decode(byte[] wire, int offset, int length) {
        ByteBuffer buffer = ByteBuffer.wrap(wire, offset, length);
        long transferId = buffer.getLong();
        int stockSlot = buffer.getInt();
        int channelLength = buffer.getInt();
        byte[] channel = new byte[channelLength];
        buffer.get(channel);
        return new HandoverRequest(transferId, stockSlot, new String(channel, StandardCharsets.US_ASCII));
    }
}
