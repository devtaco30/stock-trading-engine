package com.flab.stocktradingengine.matching.disruptor.handover;

import java.util.ArrayList;
import java.util.List;

/**
 * 종목 slot 하나에 속한 종목들의 호가창 바이트(종목마다 하나)를 응답 메시지 목록으로 만든다(11-6절). 종목 하나가 메시지
 * 하나이고, 한 메시지가 최대 길이를 넘으면 잘라 보내지 않고 이동을 실패시킨다.
 */
public final class HandoverFragmenter {

    private HandoverFragmenter() {
    }

    /**
     * @param bodies 종목마다 하나씩, 종목 하나의 호가창 바이트. 비어 있으면 「보낼 것이 없다」 메시지 하나를 만든다.
     * @param maxMessageLength 메시지 하나(앞머리 포함)의 최대 바이트 수
     * @throws IllegalStateException 어느 종목의 바이트가 앞머리와 합쳐 최대 길이를 넘을 때
     */
    public static List<HandoverMessage> split(long transferId, int stockSlot, List<byte[]> bodies, int maxMessageLength) {
        if (bodies.isEmpty()) {
            return List.of(new HandoverMessage(transferId, stockSlot, 0, 0, new byte[0]));
        }
        int total = bodies.size();
        List<HandoverMessage> messages = new ArrayList<>(total);
        for (int index = 0; index < total; index++) {
            byte[] body = bodies.get(index);
            if (HandoverMessage.HEADER_BYTES + body.length > maxMessageLength) {
                throw new IllegalStateException("호가창 이동 메시지가 최대 길이를 넘는다: 종목 slot=" + stockSlot
                    + " 순번=" + index + " 길이=" + (HandoverMessage.HEADER_BYTES + body.length)
                    + " 최대=" + maxMessageLength);
            }
            messages.add(new HandoverMessage(transferId, stockSlot, index, total, body));
        }
        return messages;
    }
}
