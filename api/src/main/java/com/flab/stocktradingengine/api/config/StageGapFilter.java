package com.flab.stocktradingengine.api.config;

import java.io.IOException;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import com.flab.stocktradingengine.time.EpochNanos;
import com.flab.stocktradingengine.time.LatencyHistogram;
import com.flab.stocktradingengine.time.StageLatencyRegistry;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

/**
 * 구간 분해 측정(measure/segments 브랜치 한정)의 G — 부하 생성기가 요청을 보냈어야 할 예정 도착 시각과 api가 요청 처리를
 * 시작한 시각의 차이(설계서 4-4a절, coordinated omission 확인용). k6(replay-e2e.js, SCHED_HEADER=1)가 헤더
 * {@value #HEADER}에 예정 도착 시각을 epoch 마이크로초로 싣고, 이 필터가 주문 요청마다 현재 시각과의 차이를 {@code G}에 기록한다.
 * v1과 v2 경로(/api/v1|v2/orders/...)에 같은 코드로 걸린다. 계측이 꺼져 있으면(기본) 분기 하나로 요청을 그대로 넘긴다.
 */
@Component
public class StageGapFilter extends OncePerRequestFilter {

    public static final String HEADER = "X-Sched-At";

    private final boolean enabled;
    private final LatencyHistogram gapStage;

    /** 등록부 빈이 없는 슬라이스 테스트 컨텍스트에서는 꺼진 등록부로 동작한다. */
    @Autowired
    public StageGapFilter(ObjectProvider<StageLatencyRegistry> registryProvider) {
        this(registryProvider.getIfAvailable(() -> new StageLatencyRegistry(false)));
    }

    public StageGapFilter(StageLatencyRegistry apiStageLatencyRegistry) {
        this.enabled = apiStageLatencyRegistry.isEnabled();
        this.gapStage = apiStageLatencyRegistry.stage("G");
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
        throws ServletException, IOException {
        if (enabled) {
            long arrivedAtNanos = EpochNanos.now();
            recordGap(request, arrivedAtNanos);
        }
        chain.doFilter(request, response);
    }

    private void recordGap(HttpServletRequest request, long arrivedAtNanos) {
        String path = request.getRequestURI();
        if (path == null || !(path.startsWith("/api/v1/orders/") || path.startsWith("/api/v2/orders/"))) {
            return;
        }
        String header = request.getHeader(HEADER);
        if (header == null || header.isBlank()) {
            return;
        }
        try {
            long scheduledMicros = Long.parseLong(header.trim());
            gapStage.recordNanos(arrivedAtNanos - scheduledMicros * 1000L);
        } catch (NumberFormatException ignored) {
            // 측정용 헤더라 잘못된 값은 건너뛴다(요청 처리에는 영향이 없다)
        }
    }
}
