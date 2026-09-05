package com.flab.stocktradingengine.api.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

import com.flab.stocktradingengine.kafka.KafkaTopics;

/**
 * order-requests 토픽 선언. 이 토픽의 발행자는 api 뿐이다("발행자가 토픽 소유").
 * 파티션 수를 코드에 고정해 브로커 auto-create 기본값(1)에 휘둘리지 않게 한다.
 * accountId 로 파티셔닝되므로 계좌 병렬 처리 목표에 맞춰 정한다.
 */
@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic orderRequestsTopic(
            @Value("${app.kafka.order-requests.partitions:3}") int partitions) {
        return TopicBuilder.name(KafkaTopics.orderRequests())
            .partitions(partitions)
            .replicas(1)
            .build();
    }
}
