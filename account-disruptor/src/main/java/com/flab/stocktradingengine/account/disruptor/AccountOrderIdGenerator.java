package com.flab.stocktradingengine.account.disruptor;

/**
 * 결정론적 orderId 발급기(2b-0) — 리플레이(2b)의 전제.
 *
 * <p>기존 Snowflake는 벽시계를 섞어 발급해 재시작·리플레이마다 다른 값이 나온다 — 같은 입력을
 * 다시 넣어도 매칭에 실어 보낸 orderId가 달라지면 복구가 매칭 쪽 상태와 어긋난다. 그래서 여기서는
 * {@code (nodeId, 카운터)}만으로 발급값을 결정한다 — 같은 nodeId로 같은 입력 시퀀스를 넣으면 항상
 * 같은 orderId가 나온다.</p>
 *
 * <p>카운터는 이 객체(=엔진 상태)에 있다 — 호스트가 아니라 엔진 쪽에 둬야 2d(스냅샷)에 담기고
 * 리플레이로 그대로 재현된다. requestId가 재전송이면(C5-2a) 호출부가 이 카운터를 건드리지 않고
 * 기억해둔 orderId를 그대로 돌려주므로, 리플레이 때도 같은 입력 순서면 카운터가 정확히 같은 횟수만
 * 증가해 같은 id가 나온다.</p>
 *
 * <h3>pack: {@code (nodeId << COUNTER_BITS) | counter}</h3>
 * <p>nodeId는 10비트(0~1023, 기존 {@code SnowflakeIdGenerator.MAX_NODE_ID}와 같은 범위 — 전역
 * 유일성 담당), counter는 53비트(1부터 증가 — 재현성 담당). 부호 비트(맨 앞 1비트)는 항상 0으로
 * 남아 양수 long을 보장한다(63비트만 씀).</p>
 */
public final class AccountOrderIdGenerator {

    public static final long MAX_NODE_ID = 1023L; // 10비트

    private static final int COUNTER_BITS = 53;
    private static final long COUNTER_MASK = (1L << COUNTER_BITS) - 1;

    private final long nodeId;
    private long counter = 0L;

    /** @param nodeId 노드/서버 구분자(0~1023). 범위 밖이면 0 또는 1023으로 클램프({@code SnowflakeIdGenerator}와 동일 정책). */
    public AccountOrderIdGenerator(long nodeId) {
        this.nodeId = nodeId < 0 ? 0 : Math.min(nodeId, MAX_NODE_ID);
    }

    /** 다음 orderId를 발급한다 — 카운터를 1 늘리고 pack한다. */
    public long next() {
        counter++;
        return (nodeId << COUNTER_BITS) | (counter & COUNTER_MASK);
    }

    /** 현재 카운터. 스냅샷(2d-2)이 이 값을 담아 복구 때 {@link #restoreCounter}로 재현한다. */
    public long counter() {
        return counter;
    }

    /**
     * 스냅샷(2d-2)에서 카운터를 복원한다. {@link #next}가 이 값 다음부터 이어 발급하게 한다 —
     * 저널 리플레이로 카운터를 진행시키는 {@link AccountEngine#recover}와 같은 이유로, 스냅샷
     * 복원도 라이브가 이어받을 지점을 정확히 맞춰야 발급 충돌이 없다.
     */
    public void restoreCounter(long counter) {
        this.counter = counter;
    }
}
