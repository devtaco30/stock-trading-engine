package com.flab.stocktradingengine.matching.worker.config;

import java.util.Map;

import io.aeron.ExclusivePublication;

import lombok.extern.slf4j.Slf4j;

/** 거부 회신 스트림(6003)의 계좌 shard 주소별 {@link ExclusivePublication} 묶음. 닫기 책임만 진다. */
@Slf4j
public final class RejectReplyPublications implements AutoCloseable {

    private final Map<String, ExclusivePublication> byEndpoint;

    public RejectReplyPublications(Map<String, ExclusivePublication> byEndpoint) {
        this.byEndpoint = byEndpoint;
    }

    public Map<String, ExclusivePublication> byEndpoint() {
        return byEndpoint;
    }

    @Override
    public void close() {
        for (Map.Entry<String, ExclusivePublication> entry : byEndpoint.entrySet()) {
            try {
                entry.getValue().close();
            } catch (RuntimeException e) {
                log.warn("[매칭] 거부 회신 endpoint={} Publication close 실패, 나머지는 계속 닫음", entry.getKey(), e);
            }
        }
    }
}
