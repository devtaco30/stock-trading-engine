package com.flab.stocktradingengine.support;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.junit.jupiter.api.Test;

import com.flab.stocktradingengine.support.IdleStrategies.Mode;

/**
 * 쉬는 방식 변경 실험(EXPERIMENT_IDLE)의 모드 해석과 전략 생성. 기본 모드에서는 지금과 똑같은
 * {@link BackoffIdleStrategy}여야 하고, 실험 모드에서만 park 상한을 줄인 전략이 나온다.
 */
class IdleStrategiesTest {

    @Test
    void 값이_없거나_비었거나_default면_DEFAULT_모드다() {
        assertSame(Mode.DEFAULT, Mode.parse(null));
        assertSame(Mode.DEFAULT, Mode.parse(""));
        assertSame(Mode.DEFAULT, Mode.parse("  "));
        assertSame(Mode.DEFAULT, Mode.parse("default"));
    }

    @Test
    void threads와_driver는_대소문자를_가리지_않고_해석한다() {
        assertSame(Mode.THREADS, Mode.parse("threads"));
        assertSame(Mode.THREADS, Mode.parse("THREADS"));
        assertSame(Mode.DRIVER, Mode.parse("driver"));
        assertSame(Mode.DRIVER, Mode.parse(" Driver "));
    }

    @Test
    void 알_수_없는_값은_예외로_시작을_막는다() {
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class, () -> Mode.parse("spin"));
        assertTrue(error.getMessage().contains("spin"), "잘못된 값이 메시지에 들어 있어야 한다: " + error.getMessage());
    }

    @Test
    void DEFAULT에서_스레드_전략은_지금과_똑같은_BackoffIdleStrategy다() {
        IdleStrategy strategy = IdleStrategies.forThread(Mode.DEFAULT);

        assertEquals(BackoffIdleStrategy.class, strategy.getClass());
    }

    @Test
    void THREADS와_DRIVER에서_스레드_전략은_park_상한이_50마이크로초다() {
        for (Mode mode : new Mode[] {Mode.THREADS, Mode.DRIVER}) {
            IdleStrategy strategy = IdleStrategies.forThread(mode);

            assertTrue(strategy instanceof IdleStrategies.ShortParkBackoffIdleStrategy, mode.name());
            assertEquals(50_000L, ((IdleStrategies.ShortParkBackoffIdleStrategy) strategy).maxParkNs(), mode.name());
        }
    }

    @Test
    void 드라이버_전략은_DRIVER에서만_나오고_호출마다_새_객체다() {
        assertNull(IdleStrategies.forDriver(Mode.DEFAULT));
        assertNull(IdleStrategies.forDriver(Mode.THREADS));
        IdleStrategy first = IdleStrategies.forDriver(Mode.DRIVER);
        IdleStrategy second = IdleStrategies.forDriver(Mode.DRIVER);

        assertTrue(first instanceof IdleStrategies.ShortParkBackoffIdleStrategy);
        assertNotSame(first, second); // 전략은 spin·yield·park 카운터를 가진 상태 객체라 스레드마다 따로여야 한다
    }

    @Test
    void 줄인_전략은_일이_없으면_park_상한_안에서_돌아온다() {
        IdleStrategy strategy = IdleStrategies.forThread(Mode.THREADS);
        long start = System.nanoTime();
        for (int i = 0; i < 200; i++) {
            strategy.idle(0);
        }
        long elapsedNanos = System.nanoTime() - start;

        // 200번 쉬어도 기본 전략(최대 1ms씩 약 190번 park하면 190ms 이상)보다 훨씬 짧아야 한다.
        assertTrue(elapsedNanos < 100_000_000L, "elapsed=" + elapsedNanos);
    }
}
