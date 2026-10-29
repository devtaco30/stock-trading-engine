package com.flab.stocktradingengine.matching.worker.handover;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;

/** 같은 키(종목 slot)의 로그를 정한 간격 안에는 한 번만 내보내고, 건너뛴 횟수를 다음 로그에 알린다. */
class RateLimitedLogTest {

    private final AtomicLong nowMillis = new AtomicLong(1_000L);
    private final RateLimitedLog limiter = new RateLimitedLog(10_000L, nowMillis::get);

    @Test
    void 처음은_내보내고_간격_안의_같은_키는_건너뛴다() {
        assertThat(limiter.tryAcquire(5)).isEqualTo(0);
        nowMillis.addAndGet(9_999L);
        assertThat(limiter.tryAcquire(5)).as("건너뜀").isEqualTo(-1);
    }

    @Test
    void 간격이_지나면_다시_내보내고_그_사이_건너뛴_횟수를_알려_준다() {
        limiter.tryAcquire(5);
        limiter.tryAcquire(5);
        limiter.tryAcquire(5);
        nowMillis.addAndGet(10_000L);

        assertThat(limiter.tryAcquire(5)).as("건너뛴 두 번").isEqualTo(2);
        assertThat(limiter.tryAcquire(5)).isEqualTo(-1);
    }

    @Test
    void 다른_키는_따로_센다() {
        assertThat(limiter.tryAcquire(5)).isEqualTo(0);
        assertThat(limiter.tryAcquire(6)).isEqualTo(0);
    }
}
