package com.flab.stocktradingengine.account.worker;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.flab.stocktradingengine.support.SnowflakeIdGenerator;
import com.flab.stocktradingengine.support.SnowflakeIdGeneratorHolder;
import com.flab.stocktradingengine.support.SnowflakeNodeIdResolver;

/**
 * C5-2a — 매수·매도 첫 접수(requestId 첫 등장)마다 발급하는 orderId의 시드.
 * matching-worker {@code SnowflakeConfig}와 같은 패턴.
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
