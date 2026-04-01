package com.flab.stocktradingengine.api.config;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.data.redis.RedisProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.lang.NonNull;

import lombok.extern.slf4j.Slf4j;

import java.util.Objects;

/**
 * Redis 빈 수동 등록.
 * <p>자동 설정({@code RedisAutoConfiguration})은 멀티모듈 + 컴포넌트 스캔 순서 때문에
 * {@link StringRedisTemplate}이 필요한 시점에 생성되지 않는 경우가 있어, exclude 후 이 설정에서
 * {@link RedisConnectionFactory}, {@code redisTemplate}, {@link StringRedisTemplate}을 직접 등록한다.</p>
 * <ul>
 *   <li>{@code spring.data.redis.host}가 있을 때만 로드 ({@link ConditionalOnProperty})</li>
 *   <li>테스트 프로파일({@code test})에서는 미로드 → Redis 빈 없음, Mock TokenService 사용</li>
 * </ul>
 */
@Slf4j
@Configuration
@Profile("!test")
@ConditionalOnProperty(prefix = "spring.data.redis", name = "host")
@EnableConfigurationProperties(RedisProperties.class)
public class RedisConfig {

	@Bean
	public RedisConnectionFactory redisConnectionFactory(@NonNull RedisProperties redisProperties) {
		String host = Objects.requireNonNullElse(redisProperties.getHost(), "localhost");
		log.info("Redis 연결 설정: host = {}, port = {}", host, redisProperties.getPort());
		RedisStandaloneConfiguration config = new RedisStandaloneConfiguration(host, redisProperties.getPort());
		return new LettuceConnectionFactory(config);
	}

	@Bean(name = "redisTemplate")
	public RedisTemplate<Object, Object> redisTemplate(@NonNull RedisConnectionFactory redisConnectionFactory) {
		RedisTemplate<Object, Object> template = new RedisTemplate<>();
		template.setConnectionFactory(redisConnectionFactory);
		return template;
	}

	@Bean
	public StringRedisTemplate stringRedisTemplate(@NonNull RedisConnectionFactory redisConnectionFactory) {
		return new StringRedisTemplate(redisConnectionFactory);
	}
}
