package com.flab.stocktradingengine.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

import com.flab.stocktradingengine.kafka.KafkaTopics;

/**
 * orders 토픽 선언. 발행자는 order-engine 뿐이다.
 * 종목은 파티션 키(stockCode)로 분배된다 — 같은 종목은 같은 파티션 → single-writer 순서 보장.
 * 파티션 수는 매칭 병렬 목표를 보고 한 번에 정한다(늘리면 key 재해시로 순서가 깨지므로 넉넉히).
 */
@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic ordersTopic(
            @Value("${app.kafka.orders.partitions:50}") int partitions) {
        return TopicBuilder.name(KafkaTopics.orders())
            .partitions(partitions)
            .replicas(1)
            .build();
    }
}
