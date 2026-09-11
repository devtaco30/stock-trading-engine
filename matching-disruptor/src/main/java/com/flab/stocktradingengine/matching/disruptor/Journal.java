package com.flab.stocktradingengine.matching.disruptor;

import java.util.List;

import com.flab.stocktradingengine.codec.JournaledOrder;

/**
 * 주문 기록 저장소.
 *
 * <p>매칭 전에 들어온 주문을 순서대로 남긴다. 프로세스가 죽어 인메모리 호가창이 사라져도,
 * 이 기록을 처음부터 재생(replay)하면 같은 상태를 다시 만들 수 있다.
 * Unit 2 에서는 인메모리 구현({@link InMemoryJournal})만 두고,
 * 이후 파일 기반 구현으로 교체할 수 있도록 인터페이스로 분리한다.</p>
 */
public interface Journal {

    /** 주문 스냅샷을 기록 끝에 덧붙인다. */
    void append(JournaledOrder order);

    /** 기록된 주문을 발행 순서대로 반환한다(복사본). */
    List<JournaledOrder> entries();

    /**
     * 지금까지 기록한 지점을 나타내는 저널 위치(2d-1). 엔진 스냅샷이 "이 위치까지는 스냅샷에
     * 담겼다"를 같이 저장해, 복구 때 저널을 처음부터가 아니라 이 위치부터만 리플레이하게 한다.
     * durable 구현(Aeron Archive)에서만 의미가 있다 — 인메모리 구현은 재생 대상 자체가 없어 0을 둔다.
     */
    long position();
}
