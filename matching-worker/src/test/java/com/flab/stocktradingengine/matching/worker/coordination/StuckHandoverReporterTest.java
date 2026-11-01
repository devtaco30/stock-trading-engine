package com.flab.stocktradingengine.matching.worker.coordination;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * 이동 중 어느 단계(판정 대기, 호가창 적재 대기, 받음 기록 확인 대기, 맵 전환 대기 등)에든 정한 시간 넘게 머문 종목 slot을 찾아 사람이 알아챌
 * ERROR 신호로 남기고 조회할 수 있게 한다. 시계는 시험이 직접 움직인다.
 */
class StuckHandoverReporterTest {

    private static final long THRESHOLD_MILLIS = 30_000L;
    private static final long REPEAT_MILLIS = 60_000L;

    private final AtomicReference<Set<Integer>> waiting = new AtomicReference<>(Set.of());
    private final AtomicLong nowMillis = new AtomicLong(1_000_000L);
    private final List<LogRecord> errors = new CopyOnWriteArrayList<>();
    private final Handler handler = new Handler() {
        @Override
        public void publish(LogRecord record) {
            if (record.getLevel().intValue() >= Level.SEVERE.intValue()) {
                errors.add(record);
            }
        }

        @Override
        public void flush() {
        }

        @Override
        public void close() {
        }
    };
    private final Logger julLogger = Logger.getLogger(StuckHandoverReporter.class.getName());
    private StuckHandoverReporter reporter;

    @BeforeEach
    void setUp() {
        julLogger.addHandler(handler);
        reporter = new StuckHandoverReporter(waiting::get, () -> 0L, nowMillis::get, THRESHOLD_MILLIS, REPEAT_MILLIS);
    }

    @AfterEach
    void tearDown() {
        julLogger.removeHandler(handler);
    }

    @Test
    void 정한_시간이_지나기_전에는_멈춘_것으로_보지_않는다() {
        waiting.set(Set.of(5));
        reporter.check();

        nowMillis.addAndGet(THRESHOLD_MILLIS - 1);
        reporter.check();

        assertThat(reporter.stuckHandoverSlots()).isEmpty();
        assertThat(errors).isEmpty();
    }

    @Test
    void 정한_시간을_넘겨_머문_종목_slot은_조회되고_사람이_X를_확인해야_한다는_ERROR가_종목_slot_번호와_함께_남는다() {
        waiting.set(Set.of(5, 6));
        reporter.check();
        nowMillis.addAndGet(THRESHOLD_MILLIS);
        waiting.set(Set.of(5, 6, 7)); // 7은 방금 들어왔다

        reporter.check();

        assertThat(reporter.stuckHandoverSlots()).containsExactlyInAnyOrder(5, 6);
        assertThat(errors).hasSize(1);
        assertThat(errors.get(0).getMessage()).contains("사람이").contains("이전 담당").contains("5").contains("6");
        assertThat(errors.get(0).getMessage()).doesNotContain("7");
    }

    @Test
    void 이동이_끝나_대기에서_빠진_종목_slot은_멈춘_것에서_빠지고_머문_시간도_다시_센다() {
        waiting.set(Set.of(5));
        reporter.check();
        nowMillis.addAndGet(THRESHOLD_MILLIS);
        reporter.check();
        assertThat(reporter.stuckHandoverSlots()).containsExactly(5);

        waiting.set(Set.of()); // 이동이 끝났다
        reporter.check();
        assertThat(reporter.stuckHandoverSlots()).isEmpty();

        waiting.set(Set.of(5)); // 나중에 다시 이동하기 시작한다
        reporter.check();
        nowMillis.addAndGet(THRESHOLD_MILLIS - 1);
        reporter.check();
        assertThat(reporter.stuckHandoverSlots()).as("앞선 머문 시간을 이어 세지 않는다").isEmpty();
    }

    @Test
    void 같은_종목_slot의_신호는_반복_간격이_지나기_전에는_다시_남기지_않는다() {
        waiting.set(Set.of(5));
        reporter.check();
        nowMillis.addAndGet(THRESHOLD_MILLIS);
        reporter.check();
        assertThat(errors).hasSize(1);

        nowMillis.addAndGet(REPEAT_MILLIS - 1);
        reporter.check();
        assertThat(errors).as("로그를 채우지 않는다").hasSize(1);

        nowMillis.addAndGet(1);
        reporter.check();
        assertThat(errors).hasSize(2);
    }
}
