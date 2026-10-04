package com.flab.stocktradingengine.matching.worker.id;

import java.util.function.LongSupplier;

import com.flab.stocktradingengine.support.SnowflakeIdGenerator;

/**
 * 시계가 뒤로 가도 예외 없이 번호가 엄격히 늘어나는 체결번호 발급기(v2 매칭 프로그램 전용).
 *
 * <p>번호 배치는 공용 {@link SnowflakeIdGenerator}(relops Snowflake)와 같다:
 * {@code (시각 << 22) | (node << 12) | 순번}. relops는 시각이 직전보다 작으면 예외를 던져
 * 체결을 처리하는 스레드가 죽는다. 이 발급기는 마지막으로 쓴 시각({@code last})과 순번({@code seq})을
 * 들고, 읽은 시각이 {@code last}보다 클 때만 시각을 따르고 아니면 순번만 올린다.
 * 순번이 4095를 넘으면 {@code last}를 1 올리고 순번을 0으로 되돌린다. 그래서 (last, seq) 쌍이
 * 사전순으로 계속 늘어 한 프로세스 안에서 번호가 겹치지 않는다.</p>
 *
 * <p>시계가 뒤로 간 동안 번호 속 시각은 실제보다 앞선다. 번호에서 시각을 읽는 코드는 두지 않는다
 * (소스 검사 테스트가 막는다). 재시작 뒤 같은 번호가 다시 나오는 문제는 풀지 않는다(LLD_F의 (나)).</p>
 *
 * <p>공용 {@link SnowflakeIdGenerator}를 상속하는 것은 {@code AccountFillPublisher}와
 * {@code SnowflakeIdGeneratorHolder}가 그 타입을 받기 때문이다. 부모 쪽 relops 인스턴스는 쓰지 않는다.</p>
 */
public class MonotonicSnowflakeIdGenerator extends SnowflakeIdGenerator {

    private static final int NODE_SHIFT = 12;
    private static final int TIME_SHIFT = 22;
    private static final long MAX_SEQUENCE = (1L << NODE_SHIFT) - 1;

    private final long nodeBits;
    private final LongSupplier clockMillis;

    private long last;
    private long sequence;

    public MonotonicSnowflakeIdGenerator(long nodeId) {
        this(nodeId, System::currentTimeMillis);
    }

    /** 테스트가 시계를 직접 정할 수 있게 한 생성자. */
    public MonotonicSnowflakeIdGenerator(long nodeId, LongSupplier clockMillis) {
        super(nodeId);
        long clamped = nodeId < 0 ? 0 : Math.min(nodeId, MAX_NODE_ID);
        this.nodeBits = clamped << NODE_SHIFT;
        this.clockMillis = clockMillis;
    }

    @Override
    public synchronized long nextId() {
        long now = clockMillis.getAsLong();
        if (now > last) {
            last = now;
            sequence = 0;
        } else {
            sequence++;
            if (sequence > MAX_SEQUENCE) {
                last++;
                sequence = 0;
            }
        }
        return (last << TIME_SHIFT) | nodeBits | sequence;
    }
}
