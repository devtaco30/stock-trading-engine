package com.flab.stocktradingengine.matching.disruptor.io;

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
     * @param bookBytes 그 종목 slot에 속한 종목들의 호가창을 이벤트 시점에 찍어 인코딩한 {@code MatchingSnapshot} 바이트
     */
    default void onSlotHandedOver(int stockSlot, byte[] bookBytes) {
    }
}
