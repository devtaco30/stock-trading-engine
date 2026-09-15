package com.flab.stocktradingengine.matching.worker.config;

import java.util.Map;

import io.aeron.ExclusivePublication;

import lombok.extern.slf4j.Slf4j;

/**
 * fork3, Unit 2 — 체결 fan-out 목적지 endpoint별 {@link ExclusivePublication} 묶음.
 * {@code Map} 빈은 Spring이 {@code destroyMethod}를 걸 수 없어, 닫기 책임만 지는 얇은 holder를
 * 따로 둔다({@link #close}가 담긴 Publication을 전부 닫는다).
 */
@Slf4j
public final class MatchingFillPublications implements AutoCloseable {

    private final Map<String, ExclusivePublication> byEndpoint;

    public MatchingFillPublications(Map<String, ExclusivePublication> byEndpoint) {
        this.byEndpoint = byEndpoint;
    }

    public Map<String, ExclusivePublication> byEndpoint() {
        return byEndpoint;
    }

    /** 하나가 close 중 예외를 던져도 나머지 Publication은 계속 닫는다(네이티브 자원 누수 방지). */
    @Override
    public void close() {
        for (Map.Entry<String, ExclusivePublication> entry : byEndpoint.entrySet()) {
            try {
                entry.getValue().close();
            } catch (RuntimeException e) {
                log.warn("[매칭] endpoint={} Publication close 실패, 나머지는 계속 닫음", entry.getKey(), e);
            }
        }
    }
}
