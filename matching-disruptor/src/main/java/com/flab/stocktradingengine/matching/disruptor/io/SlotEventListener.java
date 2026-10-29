package com.flab.stocktradingengine.matching.disruptor.io;

import java.util.List;

/**
 * 종목 slot 받음·넘김 이벤트(U13)가 엔진 안에서 처리된 것을 바깥에 알리는 통지.
 *
 * <p>호출되는 thread가 메서드마다 다르다. {@link #onSlotAcquiredJournaled}는 저널 핸들러 thread에서,
 * {@link #onSlotHandedOver}는 매칭 핸들러 thread에서 불린다. 둘 다 엔진의 핵심 thread이므로 오래 걸리는 일을
 * 하지 않고 다른 thread로 넘긴다.</p>
 */
public interface SlotEventListener {

    SlotEventListener NO_OP = new SlotEventListener() { };

    /**
     * 종목 slot의 받음 이벤트가 저널에 기록된 직후 불린다. 이 매칭 shard가 그 종목 slot의 주문 접수를 켜는 시점이
     * 이 통지다 — 링버퍼에 넣은 때가 아니다. 켠 뒤에 들어온 주문은 저널에서 항상 받음 항목 뒤에 놓인다.
     */
    default void onSlotAcquiredJournaled(int stockSlot) {
    }

    /**
     * 종목 slot의 넘김 이벤트에 도달해 그 종목 slot의 종목들을 넘기는 중으로 바꾼 직후 불린다.
     *
     * @param bookBytesPerStock 그 종목 slot에 속한 종목마다 하나씩, 그 종목의 호가창을 이벤트 시점에 찍어 인코딩한
     *     {@code MatchingSnapshot} 바이트(종목 하나만 담긴다). 종목코드 순서이고, 호가창이 있는 종목이 없으면 빈 목록이다.
     *     종목 하나가 호가창 이동 응답 메시지 하나가 된다(11-6절).
     */
    default void onSlotHandedOver(int stockSlot, List<byte[]> bookBytesPerStock) {
    }

    /**
     * 호가창 이동을 받은 쪽(Z)에서 적재한 호가창이 강제로 찍은 스냅샷에 담겨 디스크에 확정되었다
     * ({@link MatchingSnapshotSink#durableSeq()}가 그 스냅샷의 appliedSeq 이상). 이동을 보낸 쪽(X)에 「받았다」를
     * 알리는 시점이다. {@code MatchingEngine#pollLoadCompletions}를 부르는 thread에서 불린다.
     */
    default void onBooksLoadedDurable(long transferId, int stockSlot) {
    }
}
