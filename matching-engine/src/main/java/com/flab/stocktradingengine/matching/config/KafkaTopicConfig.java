package com.flab.stocktradingengine.matching.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

import com.flab.stocktradingengine.kafka.KafkaTopics;

/**
 * fills 토픽 선언. 발행자는 matching-engine 뿐이다.
 * 종목은 파티션 키(stockCode)로 분배된다. 파티션 수는 orders 와 같은 기준으로 맞춘다.
 */
@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic fillsTopic(
            @Value("${app.kafka.fills.partitions:50}") int partitions) {
        return TopicBuilder.name(KafkaTopics.fills())
            .partitions(partitions)
            .replicas(1)
            .build();
    }
}
