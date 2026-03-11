package com.flab.stocktradingengine.trading.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.flab.stocktradingengine.support.SnowflakeIdGenerator;
import com.flab.stocktradingengine.support.SnowflakeIdGeneratorHolder;
import com.flab.stocktradingengine.support.SnowflakeNodeIdResolver;

/**
 * Snowflake ID 생성기 폴백 빈 등록.
 * <p>api 모듈의 SnowflakeConfig가 없는 환경(matching/settlement 워커)에서 동작한다.</p>
 */
@Configuration
public class SnowflakeFallbackConfig {

    @Bean
    @ConditionalOnMissingBean
    public SnowflakeIdGenerator snowflakeIdGenerator(
        @Value("${snowflake.node-id:}") String nodeIdConfig
    ) {
        long nodeId = SnowflakeNodeIdResolver.resolve(nodeIdConfig);
        SnowflakeIdGenerator generator = new SnowflakeIdGenerator(nodeId);
        SnowflakeIdGeneratorHolder.set(generator);
        return generator;
    }
}
