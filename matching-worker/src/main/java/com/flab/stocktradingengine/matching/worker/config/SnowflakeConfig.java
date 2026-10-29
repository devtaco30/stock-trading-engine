package com.flab.stocktradingengine.matching.worker.config;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.flab.stocktradingengine.matching.worker.id.MonotonicSnowflakeIdGenerator;
import com.flab.stocktradingengine.support.SnowflakeIdGenerator;
import com.flab.stocktradingengine.support.SnowflakeIdGeneratorHolder;
import com.flab.stocktradingengine.support.SnowflakeNodeIdResolver;

/**
 * 매칭 shard의 tradeId에 들어가는 서버 번호(Snowflake nodeId)를 정한다. 신원 이름
 * ({@code matching-worker.instance-id}, Kafka 그룹의 {@code group.instance.id}와 같은 값)이 있으면 그
 * 끝 순번에 {@link #NODE_ID_BASE}를 더한 값이다. 매칭 shard마다 신원이 다르므로 서버 번호도 다르고,
 * 재시작해도 같은 신원이면 같은 번호다. 신원 이름이 없으면(단일 워커 실행) 예전처럼
 * {@code snowflake.node-id} 설정, 그것도 없으면 호스트명으로 정한다.
 */
@Configuration
public class SnowflakeConfig {

    /** 레포에서 이미 쓰는 번호 0~5(api 0, 옛 v1 매칭 1·2, 계좌 워커 3·5, 옛 매칭 워커 기본값 4)를 피하는 가장 작은 값. */
    static final long NODE_ID_BASE = 6L;

    private static final Pattern TRAILING_DIGITS = Pattern.compile("(\\d+)$");

    @Bean
    public SnowflakeIdGenerator snowflakeIdGenerator(
        @Value("${matching-worker.instance-id:}") String instanceId,
        @Value("${snowflake.node-id:}") String nodeIdConfig
    ) {
        long nodeId = instanceId.isBlank()
            ? SnowflakeNodeIdResolver.resolve(nodeIdConfig)
            : nodeIdFromInstanceId(instanceId);
        SnowflakeIdGenerator generator = new MonotonicSnowflakeIdGenerator(nodeId);
        SnowflakeIdGeneratorHolder.set(generator);
        return generator;
    }

    /** 신원 이름 끝 순번 + {@link #NODE_ID_BASE}. 끝 순번을 읽지 못하거나 번호가 범위를 넘으면 기동을 막는다. */
    static long nodeIdFromInstanceId(String instanceId) {
        Matcher matcher = TRAILING_DIGITS.matcher(instanceId.trim());
        if (!matcher.find()) {
            throw new IllegalStateException(
                "matching-worker.instance-id에서 끝 순번을 읽지 못해 서버 번호를 정할 수 없다: " + instanceId);
        }
        long nodeId = NODE_ID_BASE + Long.parseLong(matcher.group(1));
        if (nodeId > SnowflakeIdGenerator.MAX_NODE_ID) {
            throw new IllegalStateException(
                "서버 번호가 최대값 " + SnowflakeIdGenerator.MAX_NODE_ID + "을 넘는다: " + instanceId + " → " + nodeId);
        }
        return nodeId;
    }
}
