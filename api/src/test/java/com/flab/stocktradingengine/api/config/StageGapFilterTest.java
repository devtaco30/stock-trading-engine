package com.flab.stocktradingengine.api.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockFilterChain;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import com.flab.stocktradingengine.time.EpochNanos;
import com.flab.stocktradingengine.time.StageLatencyRegistry;
import com.flab.stocktradingengine.time.StageSnapshot;

import jakarta.servlet.ServletException;

/**
 * 구간 분해 측정의 G(예정 도착 시각과 api 요청 처리 시작 시각의 차이, 설계서 4-4a절). k6가 헤더 X-Sched-At 에
 * 예정 도착 시각(epoch 마이크로초)을 실어 보내면 api 필터가 처리 시작 시각과의 차이를 G 에 기록한다.
 */
class StageGapFilterTest {

    private static long count(Map<String, StageSnapshot> snapshots) {
        StageSnapshot snapshot = snapshots.get("G");
        return snapshot == null ? 0L : snapshot.count();
    }

    private MockHttpServletRequest orderRequest(String path, String schedHeader) {
        MockHttpServletRequest request = new MockHttpServletRequest("POST", path);
        request.setRequestURI(path);
        if (schedHeader != null) {
            request.addHeader(StageGapFilter.HEADER, schedHeader);
        }
        return request;
    }

    private AtomicInteger run(StageLatencyRegistry registry, MockHttpServletRequest request) throws ServletException, IOException {
        AtomicInteger chained = new AtomicInteger();
        MockFilterChain chain = new MockFilterChain() {
            @Override
            public void doFilter(jakarta.servlet.ServletRequest req, jakarta.servlet.ServletResponse res) {
                chained.incrementAndGet();
            }
        };
        new StageGapFilter(registry).doFilter(request, new MockHttpServletResponse(), chain);
        return chained;
    }

    @Test
    void 헤더가_있으면_예정_시각과의_차이를_G에_한_건_기록하고_요청을_넘긴다() throws Exception {
        StageLatencyRegistry registry = new StageLatencyRegistry(true);
        long scheduledMicros = EpochNanos.now() / 1000 - 3000; // 3ms 전에 도착했어야 하는 요청
        AtomicInteger chained = run(registry, orderRequest("/api/v2/orders/buy", String.valueOf(scheduledMicros)));

        Map<String, StageSnapshot> snapshots = registry.snapshotAndResetAll();
        assertThat(count(snapshots)).isEqualTo(1L);
        assertThat(snapshots.get("G").p50Micros()).isBetween(3000L, 4000L);
        assertThat(chained.get()).isEqualTo(1);
    }

    @Test
    void v1_경로도_같은_코드로_기록한다() throws Exception {
        StageLatencyRegistry registry = new StageLatencyRegistry(true);
        run(registry, orderRequest("/api/v1/orders/sell", String.valueOf(EpochNanos.now() / 1000 - 100)));
        assertThat(count(registry.snapshotAndResetAll())).isEqualTo(1L);
    }

    @Test
    void 헤더가_없으면_기록하지_않는다() throws Exception {
        StageLatencyRegistry registry = new StageLatencyRegistry(true);
        AtomicInteger chained = run(registry, orderRequest("/api/v2/orders/buy", null));
        assertThat(count(registry.snapshotAndResetAll())).isZero();
        assertThat(chained.get()).isEqualTo(1);
    }

    @Test
    void 숫자가_아닌_헤더는_무시하고_요청은_넘긴다() throws Exception {
        StageLatencyRegistry registry = new StageLatencyRegistry(true);
        AtomicInteger chained = run(registry, orderRequest("/api/v2/orders/buy", "abc"));
        assertThat(count(registry.snapshotAndResetAll())).isZero();
        assertThat(chained.get()).isEqualTo(1);
    }

    @Test
    void 주문_경로가_아니면_기록하지_않는다() throws Exception {
        StageLatencyRegistry registry = new StageLatencyRegistry(true);
        run(registry, orderRequest("/api/v2/accounts/1", String.valueOf(EpochNanos.now() / 1000)));
        assertThat(count(registry.snapshotAndResetAll())).isZero();
    }

    @Test
    void 계측을_끄면_기록하지_않고_요청을_넘긴다() throws Exception {
        StageLatencyRegistry registry = new StageLatencyRegistry(false);
        AtomicInteger chained = run(registry, orderRequest("/api/v2/orders/buy", String.valueOf(EpochNanos.now() / 1000)));
        assertThat(count(registry.snapshotAndResetAll())).isZero();
        assertThat(chained.get()).isEqualTo(1);
    }

    @Test
    void 예정_시각이_미래이면_0으로_기록한다() throws Exception {
        StageLatencyRegistry registry = new StageLatencyRegistry(true);
        run(registry, orderRequest("/api/v2/orders/buy", String.valueOf(EpochNanos.now() / 1000 + 5_000_000)));
        StageSnapshot snapshot = registry.snapshotAndResetAll().get("G");
        assertThat(snapshot.count()).isEqualTo(1L);
        assertThat(snapshot.maxMicros()).isZero();
    }
}
