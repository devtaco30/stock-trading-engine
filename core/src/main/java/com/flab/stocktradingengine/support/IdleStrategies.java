package com.flab.stocktradingengine.support;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.Locale;

import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;

/**
 * 쉬는 방식 변경 실험(측정 브랜치 한정)의 스위치. 환경 변수 {@value #ENV_NAME}으로 켠다.
 *
 * <ul>
 *   <li>없음·빈 값·{@code default} — 지금과 같다. 스레드마다 {@code new BackoffIdleStrategy()}, 드라이버는 설정하지 않는다.</li>
 *   <li>{@code threads} — 주문 이동 경로(A·C)의 우리 스레드 네 곳만 park 상한을 50µs로 줄인 Backoff를 쓴다.</li>
 *   <li>{@code driver} — 위 네 곳과 Aeron 미디어 드라이버의 sender·receiver 스레드가 같은 전략을 쓴다.</li>
 * </ul>
 *
 * <p>알 수 없는 값은 예외로 시작을 막는다(오타로 기본 모드로 도는 것을 막는다). 실험 모드면 프로세스마다 한 번 WARN으로
 * 선택한 모드를 남긴다.</p>
 */
public final class IdleStrategies {

    public static final String ENV_NAME = "EXPERIMENT_IDLE";

    /** 실험 모드의 park 상한. 기본 전략의 1ms를 줄인다. */
    public static final long SHORT_PARK_MAX_NS = 50_000L;

    private static final long MAX_SPINS = 10L;
    private static final long MAX_YIELDS = 5L;
    private static final long MIN_PARK_NS = 1_000L;

    private static final Logger LOG = System.getLogger(IdleStrategies.class.getName());

    private IdleStrategies() {
    }

    public enum Mode {
        DEFAULT, THREADS, DRIVER;

        public static Mode parse(String value) {
            if (value == null || value.isBlank()) {
                return DEFAULT;
            }
            String normalized = value.trim().toLowerCase(Locale.ROOT);
            return switch (normalized) {
                case "default" -> DEFAULT;
                case "threads" -> THREADS;
                case "driver" -> DRIVER;
                default -> throw new IllegalArgumentException(
                    ENV_NAME + " 값을 알 수 없다: '" + value + "' (default, threads, driver 중 하나)");
            };
        }
    }

    /** park 상한만 줄인 Backoff. {@link BackoffIdleStrategy}가 final이라 위임한다. */
    public static final class ShortParkBackoffIdleStrategy implements IdleStrategy {

        private final BackoffIdleStrategy delegate =
            new BackoffIdleStrategy(MAX_SPINS, MAX_YIELDS, MIN_PARK_NS, SHORT_PARK_MAX_NS);

        public long maxParkNs() {
            return SHORT_PARK_MAX_NS;
        }

        @Override
        public void idle(int workCount) {
            delegate.idle(workCount);
        }

        @Override
        public void idle() {
            delegate.idle();
        }

        @Override
        public void reset() {
            delegate.reset();
        }
    }

    private static final class Holder {
        static final Mode MODE = resolve();

        private static Mode resolve() {
            Mode mode = Mode.parse(System.getenv(ENV_NAME));
            if (mode != Mode.DEFAULT) {
                LOG.log(Level.WARNING, "[experiment] {0}={1}: park 상한 {2}ns 전략을 쓴다", ENV_NAME, mode, SHORT_PARK_MAX_NS);
            }
            return mode;
        }
    }

    /** 프로세스가 시작할 때 환경 변수에서 한 번 읽은 모드. */
    public static Mode mode() {
        return Holder.MODE;
    }

    /** 우리 스레드(주문 이동 경로)의 쉬는 방식. 기본 모드면 지금과 같은 {@code new BackoffIdleStrategy()}다. */
    public static IdleStrategy forThread() {
        return forThread(mode());
    }

    public static IdleStrategy forThread(Mode mode) {
        return mode == Mode.DEFAULT ? new BackoffIdleStrategy() : new ShortParkBackoffIdleStrategy();
    }

    /** 드라이버 sender·receiver의 쉬는 방식. {@code driver} 모드에서만 값이 있고, 아니면 null이라 Context를 건드리지 않는다. */
    public static IdleStrategy forDriver() {
        return forDriver(mode());
    }

    public static IdleStrategy forDriver(Mode mode) {
        return mode == Mode.DRIVER ? new ShortParkBackoffIdleStrategy() : null;
    }
}
