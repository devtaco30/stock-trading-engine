package com.flab.stocktradingengine.matching.disruptor.handover;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;

/**
 * 호가창 이동(X→Z)의 메시지 나누기와 조립(U14, 11-6절). 종목 하나의 호가창 바이트가 메시지 하나이고,
 * 메시지마다 [전송 id][종목 slot][순번][총 개수]가 앞에 붙는다. 조립기는 전송 id별로 0부터 총 개수-1까지 모두
 * 도착했을 때만 완료를 돌려준다.
 */
class HandoverFragmenterTest {

    private static final long TRANSFER_ID = (7L << 53) | 1L;
    private static final int STOCK_SLOT = 12;
    private static final int MAX_MESSAGE_LENGTH = 1_376;

    @Test
    void 나눈_뒤_조립하면_원래_바이트와_같다_길이가_경계값일_때도() {
        int bodyMax = MAX_MESSAGE_LENGTH - HandoverMessage.HEADER_BYTES;
        List<byte[]> bodies = List.of(bytes(1, 0), bytes(2, 1), bytes(3, bodyMax - 1), bytes(4, bodyMax), bytes(5, 7));

        List<HandoverMessage> messages = HandoverFragmenter.split(TRANSFER_ID, STOCK_SLOT, bodies, MAX_MESSAGE_LENGTH);
        HandoverAssembler assembler = new HandoverAssembler();
        Optional<List<byte[]>> assembled = Optional.empty();
        for (HandoverMessage message : messages) {
            assembled = assembler.accept(message);
        }

        assertEquals(5, messages.size());
        assertTrue(assembled.isPresent());
        assertEquals(bodies.size(), assembled.get().size());
        for (int i = 0; i < bodies.size(); i++) {
            assertArrayEquals(bodies.get(i), assembled.get().get(i), "순번 " + i);
        }
    }

    @Test
    void 메시지가_하나_빠지면_조립이_끝나지_않는다() {
        List<HandoverMessage> messages = HandoverFragmenter.split(TRANSFER_ID, STOCK_SLOT,
            List.of(bytes(1, 10), bytes(2, 10), bytes(3, 10)), MAX_MESSAGE_LENGTH);
        HandoverAssembler assembler = new HandoverAssembler();

        assertTrue(assembler.accept(messages.get(0)).isEmpty());
        assertTrue(assembler.accept(messages.get(2)).isEmpty());
        assertTrue(assembler.isPending(TRANSFER_ID));

        assertTrue(assembler.accept(messages.get(1)).isPresent(), "빠졌던 순번이 오면 완료된다");
    }

    @Test
    void 같은_메시지가_두_번_와도_한_번만_센다() {
        byte[] first = bytes(1, 10);
        List<HandoverMessage> messages = HandoverFragmenter.split(TRANSFER_ID, STOCK_SLOT,
            List.of(first, bytes(2, 10)), MAX_MESSAGE_LENGTH);
        HandoverAssembler assembler = new HandoverAssembler();
        HandoverMessage duplicateOfZero = new HandoverMessage(TRANSFER_ID, STOCK_SLOT, 0, 2, bytes(9, 10));

        assertTrue(assembler.accept(messages.get(0)).isEmpty());
        assertTrue(assembler.accept(duplicateOfZero).isEmpty(), "순번 0이 두 번 왔다고 두 개로 세면 안 된다");
        Optional<List<byte[]>> assembled = assembler.accept(messages.get(1));

        assertTrue(assembled.isPresent());
        assertArrayEquals(first, assembled.get().get(0), "먼저 온 것을 남기고 나중 것을 버린다");
    }

    @Test
    void 전송_id가_다른_두_전송의_메시지가_섞여_와도_각각_조립된다() {
        long otherTransferId = (8L << 53) | 1L;
        List<HandoverMessage> first = HandoverFragmenter.split(TRANSFER_ID, 12, List.of(bytes(1, 5), bytes(2, 5)), MAX_MESSAGE_LENGTH);
        List<HandoverMessage> second = HandoverFragmenter.split(otherTransferId, 40, List.of(bytes(3, 5), bytes(4, 5)), MAX_MESSAGE_LENGTH);
        HandoverAssembler assembler = new HandoverAssembler();

        assertTrue(assembler.accept(first.get(0)).isEmpty());
        assertTrue(assembler.accept(second.get(1)).isEmpty());
        Optional<List<byte[]>> firstDone = assembler.accept(first.get(1));
        Optional<List<byte[]>> secondDone = assembler.accept(second.get(0));

        assertTrue(firstDone.isPresent());
        assertTrue(secondDone.isPresent());
        assertArrayEquals(bytes(1, 5), firstDone.get().get(0));
        assertArrayEquals(bytes(4, 5), secondDone.get().get(1));
    }

    @Test
    void 종목이_하나도_없는_slot은_총_개수_0인_메시지_하나로_알리고_바로_완료된다() {
        List<HandoverMessage> messages = HandoverFragmenter.split(TRANSFER_ID, STOCK_SLOT, List.of(), MAX_MESSAGE_LENGTH);

        assertEquals(1, messages.size());
        assertEquals(0, messages.get(0).total());
        assertEquals(0, messages.get(0).index());
        Optional<List<byte[]>> assembled = new HandoverAssembler().accept(messages.get(0));
        assertTrue(assembled.isPresent());
        assertTrue(assembled.get().isEmpty());
    }

    @Test
    void 메시지를_바이트로_바꿨다가_되돌리면_모든_칸이_같다() {
        HandoverMessage original = new HandoverMessage(TRANSFER_ID, STOCK_SLOT, 3, 9, bytes(6, 20));

        byte[] wire = original.encode();
        HandoverMessage decoded = HandoverMessage.decode(wire, 0, wire.length);

        assertEquals(HandoverMessage.HEADER_BYTES + 20, wire.length);
        assertEquals(TRANSFER_ID, decoded.transferId());
        assertEquals(STOCK_SLOT, decoded.stockSlot());
        assertEquals(3, decoded.index());
        assertEquals(9, decoded.total());
        assertArrayEquals(bytes(6, 20), decoded.body());
    }

    @Test
    void 종목_하나의_바이트가_메시지_최대_길이를_넘으면_나누지_않고_실패한다() {
        int tooLong = MAX_MESSAGE_LENGTH - HandoverMessage.HEADER_BYTES + 1;

        assertThrows(IllegalStateException.class, () ->
            HandoverFragmenter.split(TRANSFER_ID, STOCK_SLOT, List.of(bytes(1, tooLong)), MAX_MESSAGE_LENGTH));
    }

    @Test
    void 이미_끝난_전송을_버리면_그_전송의_진행_상태가_없다() {
        List<HandoverMessage> messages = HandoverFragmenter.split(TRANSFER_ID, STOCK_SLOT,
            List.of(bytes(1, 5), bytes(2, 5)), MAX_MESSAGE_LENGTH);
        HandoverAssembler assembler = new HandoverAssembler();
        assembler.accept(messages.get(0));
        assertTrue(assembler.isPending(TRANSFER_ID));

        assembler.discard(TRANSFER_ID);

        assertFalse(assembler.isPending(TRANSFER_ID));
    }

    private static byte[] bytes(int seed, int length) {
        byte[] out = new byte[length];
        for (int i = 0; i < length; i++) {
            out[i] = (byte) (seed * 31 + i);
        }
        return out;
    }
}
