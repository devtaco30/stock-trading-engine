package com.flab.stocktradingengine.matching.worker.config;

import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.flab.stocktradingengine.aeron.AeronStreamIds;
import com.flab.stocktradingengine.aeron.ShardRoutingTable;
import com.flab.stocktradingengine.matching.worker.messaging.AccountFillPublisher;
import com.flab.stocktradingengine.support.SnowflakeIdGenerator;

import io.aeron.Aeron;
import io.aeron.ExclusivePublication;

/**
 * 매칭 체결을 계좌 샤드별 Aeron 스트림으로 fan-out 발행하는 배선(ADR-032 U2, fork3 U2).
 * {@link MatchingOrderIntakeConfig}가 만든 matching-worker 자체 Aeron 클라이언트를 그대로 쓴다.
 *
 * <p>durable 녹화는 이 클래스에 없다 — fork3 U2에서 매칭측 LOCAL 녹화를 제거하고, 복구 주체인
 * 수신측(계좌)이 자기 Archive로 REMOTE 녹화하도록 U3에서 옮긴다(제거+추가=이동, U2~U3 사이 과도기
 * 동안만 durable 녹화 공백, 순차 유닛이라 브랜치가 그 상태로 배포되지 않는다).</p>
 */
@Configuration
public class MatchingFillPublishConfig {

    /**
     * {@link ShardRoutingTable}이 가리키는 목적지 endpoint마다 {@link ExclusivePublication}을
     * 하나씩 연다. {@link AccountFillPublisher}(매칭 단일 소비자 스레드) 하나만 쓰는 발행
     * 스트림이라 각각 {@link ExclusivePublication}으로 연다. 모든 endpoint가 채널만 다르고
     * 스트림 ID는 {@link AeronStreamIds#FILL}을 공유한다(fork1 udp: endpoint=주소, streamId 고정).
     */
    @Bean(destroyMethod = "close")
    public MatchingFillPublications matchingFillPublications(Aeron aeron, ShardRoutingTable shardRoutingTable) {
        Map<String, ExclusivePublication> byEndpoint = new LinkedHashMap<>();
        for (String endpoint : shardRoutingTable.distinctEndpoints()) {
            byEndpoint.put(endpoint, aeron.addExclusivePublication(endpoint, AeronStreamIds.FILL));
        }
        return new MatchingFillPublications(byEndpoint);
    }

    @Bean
    public AccountFillPublisher accountFillPublisher(
            ShardRoutingTable shardRoutingTable,
            MatchingFillPublications matchingFillPublications,
            SnowflakeIdGenerator snowflakeIdGenerator) {
        return new AccountFillPublisher(shardRoutingTable, matchingFillPublications.byEndpoint(), snowflakeIdGenerator);
    }
}
