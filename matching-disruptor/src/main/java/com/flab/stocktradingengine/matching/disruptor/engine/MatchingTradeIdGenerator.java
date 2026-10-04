package com.flab.stocktradingengine.matching.disruptor.engine;

/**
 * v2 체결번호(tradeId) 발급기. 시스템 시계를 읽지 않고 {@code (nodeId << 53) | 카운터}로 만든다(ADR-023 갱신 2c).
 * 계좌 쪽 {@code AccountOrderIdGenerator}와 같은 형식이고, 같은 이유로 엔진 상태에 속한다: 카운터가 스냅샷에 들어가고
 * 저널 리플레이가 같은 인스턴스를 전진시키므로 같은 입력은 엔진을 다시 만들어도 같은 번호를 받는다.
 *
 * <p>스레드 안전하지 않다. 매칭 소비자 스레드 하나만 {@link #next}를 부르고(실행 핸들러와 복구 핸들러가 같은 인스턴스를
 * 번갈아 쓰되 동시에 쓰지 않는다), 스냅샷은 그 스레드가 이벤트 경계에서 {@link #counter}를 읽는다.</p>
 *
 * <p>nodeId는 0~1023(10비트)이고 카운터는 53비트다. 첫 번호는 카운터 1이다. 노드가 둘 이상일 때 nodeId가 서로 달라야
 * 번호가 겹치지 않는데, 이 클래스는 그것을 검사하지 않는다.</p>
 */
public final class MatchingTradeIdGenerator {

    private static final int MAX_NODE_ID = 1023;
    private static final int COUNTER_BITS = 53;
    private static final long COUNTER_MASK = (1L << COUNTER_BITS) - 1;

    private final long nodeIdBits;
    private long counter = 0L;

    public MatchingTradeIdGenerator(long nodeId) {
        long clamped = nodeId < 0 ? 0 : Math.min(nodeId, MAX_NODE_ID);
        this.nodeIdBits = clamped << COUNTER_BITS;
    }

    public long next() {
        counter++;
        return nodeIdBits | (counter & COUNTER_MASK);
    }

    /** 지금까지 발급한 횟수. 스냅샷에 넣는 값이다. */
    public long counter() {
        return counter;
    }

    /** 스냅샷에서 되살린 카운터로 되돌린다. 다음 {@link #next}는 {@code counter + 1}번 번호를 준다. */
    public void restoreCounter(long counter) {
        this.counter = counter;
    }
}
