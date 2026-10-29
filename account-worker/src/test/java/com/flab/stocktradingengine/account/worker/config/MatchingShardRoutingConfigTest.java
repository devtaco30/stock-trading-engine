package com.flab.stocktradingengine.account.worker.config;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.jupiter.api.Test;

/**
 * 맵 토픽을 만드는 쪽은 매칭 shard다. 계좌 shard의 읽기 컨슈머가 존재하지 않는 맵 토픽을 조회하다가
 * 1파티션짜리 빈 토픽을 자동 생성하면 매칭 shard가 만들 토픽과 어긋나므로 자동 생성을 끈다.
 */
class MatchingShardRoutingConfigTest {

    @Test
    void 맵_토픽_읽기_컨슈머는_토픽_자동_생성을_허용하지_않는다() {
        Properties properties = MatchingShardRoutingConfig.matchingShardMapConsumerProperties("localhost:9092");

        assertEquals(false, properties.get(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG));
        assertEquals("localhost:9092", properties.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
    }
}
