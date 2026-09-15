package com.flab.stocktradingengine.matching.worker.config;

import java.util.Map;

import io.aeron.ExclusivePublication;

/**
 * fork3, Unit 2 — 체결 fan-out 목적지 endpoint별 {@link ExclusivePublication} 묶음.
 * {@code Map} 빈은 Spring이 {@code destroyMethod}를 걸 수 없어, 닫기 책임만 지는 얇은 holder를
 * 따로 둔다({@link #close}가 담긴 Publication을 전부 닫는다).
 */
public final class MatchingFillPublications implements AutoCloseable {

    private final Map<String, ExclusivePublication> byEndpoint;

    public MatchingFillPublications(Map<String, ExclusivePublication> byEndpoint) {
        this.byEndpoint = byEndpoint;
    }

    public Map<String, ExclusivePublication> byEndpoint() {
        return byEndpoint;
    }

    @Override
    public void close() {
        byEndpoint.values().forEach(ExclusivePublication::close);
    }
}
