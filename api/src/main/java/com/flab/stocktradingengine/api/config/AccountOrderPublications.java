package com.flab.stocktradingengine.api.config;

import java.util.Map;

import io.aeron.Publication;

import lombok.extern.slf4j.Slf4j;

/**
 * I8 U1 — 계좌 인테이크 fan-out 목적지 endpoint별 {@link Publication} 묶음(matching-worker
 * {@code MatchingFillPublications}를 미러). {@code Map} 빈은 Spring이 {@code destroyMethod}를
 * 걸 수 없어, 닫기 책임만 지는 얇은 holder를 따로 둔다.
 *
 * <p>{@link io.aeron.ExclusivePublication}이 아니라 일반 {@link Publication}을 쓴다 — 여러 HTTP
 * 요청 스레드가 {@link com.flab.stocktradingengine.api.messaging.AeronAccountOrderSender#send}를
 * 동시에 부르므로, 단일 writer 전제인 ExclusivePublication을 쓸 수 없다.</p>
 */
@Slf4j
public final class AccountOrderPublications implements AutoCloseable {

    private final Map<String, Publication> byEndpoint;

    public AccountOrderPublications(Map<String, Publication> byEndpoint) {
        this.byEndpoint = byEndpoint;
    }

    public Map<String, Publication> byEndpoint() {
        return byEndpoint;
    }

    /** 하나가 close 중 예외를 던져도 나머지 Publication은 계속 닫는다(네이티브 자원 누수 방지). */
    @Override
    public void close() {
        for (Map.Entry<String, Publication> entry : byEndpoint.entrySet()) {
            try {
                entry.getValue().close();
            } catch (RuntimeException e) {
                log.warn("[api] endpoint={} Publication close 실패, 나머지는 계속 닫음", entry.getKey(), e);
            }
        }
    }
}
