package com.flab.stocktradingengine.matching.worker.handover;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;

/**
 * 같은 키(종목 slot)의 로그를 정한 간격 안에는 한 번만 내보내게 하는 제한기. 이전 담당이 연결된 채 응답하지 못하면 종목 slot마다 초당
 * 재요청과 경고가 나가 로그가 하루 수백만 줄이 되므로, 간격 안의 반복은 건너뛰고 다음에 내보낼 때 건너뛴 횟수를 알린다. 어느 thread에서나
 * 부를 수 있다.
 */
public final class RateLimitedLog {

    private record State(long lastEmittedAtMillis, int suppressed) {
    }

    // 키가 계속 새로 들어와도(이동 번호처럼 요청마다 새로 생기는 키) 상태가 늘어나지 않게 하는 상한. 근거 없는 임의 값이다.
    static final int MAX_KEYS = 1024;

    private final long intervalMillis;
    private final LongSupplier nowMillis;
    private final Map<Long, State> states = new ConcurrentHashMap<>();

    public RateLimitedLog(long intervalMillis, LongSupplier nowMillis) {
        this.intervalMillis = intervalMillis;
        this.nowMillis = nowMillis;
    }

    public RateLimitedLog(long intervalMillis) {
        this(intervalMillis, System::currentTimeMillis);
    }

    int stateCount() {
        return states.size();
    }

    /**
     * @return 지금 내보내도 되면 직전에 내보낸 뒤 건너뛴 횟수(0 이상), 간격 안이라 건너뛰어야 하면 -1
     */
    public int tryAcquire(long key) {
        long now = nowMillis.getAsLong();
        if (states.size() >= MAX_KEYS) {
            // 간격이 한참 지난 항목을 먼저 비우고, 그래도 상한이면 모두 비운다. 비우면 같은 키의 로그가 한두 번 더 나갈 뿐이다.
            states.values().removeIf(state -> now - state.lastEmittedAtMillis() >= intervalMillis * 10);
            if (states.size() >= MAX_KEYS) {
                states.clear();
            }
        }
        int[] result = new int[1];
        states.compute(key, (k, state) -> {
            if (state == null) {
                result[0] = 0;
                return new State(now, 0);
            }
            if (now - state.lastEmittedAtMillis() >= intervalMillis) {
                result[0] = state.suppressed();
                return new State(now, 0);
            }
            result[0] = -1;
            return new State(state.lastEmittedAtMillis(), state.suppressed() + 1);
        });
        return result[0];
    }
}
