package com.flab.stocktradingengine.matching.worker.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.flab.stocktradingengine.matching.worker.id.MonotonicSnowflakeIdGenerator;
import com.flab.stocktradingengine.support.SnowflakeIdGenerator;
import com.flab.stocktradingengine.support.SnowflakeIdGeneratorHolder;
import com.flab.stocktradingengine.support.SnowflakeNodeIdResolver;

@Configuration
public class SnowflakeConfig {

    @Bean
    public SnowflakeIdGenerator snowflakeIdGenerator(
        @Value("${snowflake.node-id:}") String nodeIdConfig
    ) {
        long nodeId = SnowflakeNodeIdResolver.resolve(nodeIdConfig);
        SnowflakeIdGenerator generator = new MonotonicSnowflakeIdGenerator(nodeId);
        SnowflakeIdGeneratorHolder.set(generator);
        return generator;
    }
}
