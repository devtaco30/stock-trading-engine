package com.flab.stocktradingengine.api.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.flab.stocktradingengine.support.SnowflakeIdGenerator;
import com.flab.stocktradingengine.support.SnowflakeIdGeneratorHolder;
import com.flab.stocktradingengine.support.SnowflakeNodeIdResolver;

/**
 * Snowflake ID 생성기 빈 등록.
 * <p>node-id 결정은 {@link com.flab.stocktradingengine.api.support.SnowflakeNodeIdResolver}에서 수행.</p>
 */
@Configuration
public class SnowflakeConfig {

    @Bean
    public SnowflakeIdGenerator snowflakeIdGenerator(
        @Value("${snowflake.node-id:}") String nodeIdConfig
    ) {
        long nodeId = SnowflakeNodeIdResolver.resolve(nodeIdConfig);
        SnowflakeIdGenerator generator = new SnowflakeIdGenerator(nodeId);
        SnowflakeIdGeneratorHolder.set(generator);
        return generator;
    }
}
