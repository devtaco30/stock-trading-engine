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

    private final long intervalMillis;
    private final LongSupplier nowMillis;
    private final Map<Integer, State> states = new ConcurrentHashMap<>();

    public RateLimitedLog(long intervalMillis, LongSupplier nowMillis) {
        this.intervalMillis = intervalMillis;
        this.nowMillis = nowMillis;
    }

    public RateLimitedLog(long intervalMillis) {
        this(intervalMillis, System::currentTimeMillis);
    }

    /**
     * @return 지금 내보내도 되면 직전에 내보낸 뒤 건너뛴 횟수(0 이상), 간격 안이라 건너뛰어야 하면 -1
     */
    public int tryAcquire(int key) {
        long now = nowMillis.getAsLong();
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
