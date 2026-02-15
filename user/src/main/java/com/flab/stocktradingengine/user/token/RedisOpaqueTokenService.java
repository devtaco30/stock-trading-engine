package com.flab.stocktradingengine.user.token;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.HexFormat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.context.annotation.DependsOn;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Service;

import com.flab.stocktradingengine.user.exception.InvalidTokenException;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;

/**
 * 불투명 토큰 + Redis 기반 {@link TokenService} 구현체.
 * <p>Redis 키는 토큰 해시(SHA-256)로 저장해 덤프 시 원문 노출을 막음.</p>
 * <p>{@link org.springframework.data.redis.core.StringRedisTemplate}이 있을 때만 빈 등록되며,
 * 테스트 등 Redis 미사용 시에는 Mock/Stub TokenService 사용.</p>
 */
@Service
@ConditionalOnBean(StringRedisTemplate.class)
@DependsOn("stringRedisTemplate")
@RequiredArgsConstructor
public class RedisOpaqueTokenService implements TokenService {

	private static final Logger log = LoggerFactory.getLogger(RedisOpaqueTokenService.class);

	private static final String KEY_PREFIX = "auth:token:";
	private static final int TOKEN_BYTES = 32;
	private static final Duration DEFAULT_TTL = Duration.ofDays(7);

	private final StringRedisTemplate redisTemplate;

	@Value("${user.token.expire-seconds:#{null}}")
	private Long expireSeconds;

	private Duration ttl = DEFAULT_TTL;

	@PostConstruct
	void init() {
		log.info("user.token.expire-seconds = {} (TTL 적용)", expireSeconds);
		if (expireSeconds != null && expireSeconds > 0) {
			this.ttl = Duration.ofSeconds(expireSeconds);
		}
	}

	@Override
	public String issueToken(long userId) {
		String token = generateOpaqueToken();
		String key = keyFromToken(token);
		String value = String.valueOf(userId);
		if (key == null || value == null || ttl == null) {
			throw new IllegalStateException("토큰 저장에 필요한 key, value, ttl이 null일 수 없습니다");
		}
		redisTemplate.opsForValue().set(key, value, ttl);
		return token;
	}

	@Override
	public long validateAndGetUserId(String token) {
		if (token == null || token.isBlank()) {
			throw new InvalidTokenException("토큰이 비어 있습니다");
		}
		String key = keyFromToken(token);
		String value = redisTemplate.opsForValue().get(key);
		if (value == null || value.isBlank()) {
			throw new InvalidTokenException("유효하지 않거나 만료된 토큰입니다");
		}
		try {
			return Long.parseLong(value.trim());
		} catch (NumberFormatException e) {
			throw new InvalidTokenException("토큰에 해당하는 사용자 정보가 올바르지 않습니다", e);
		}
	}

	@Override
	public boolean isValid(String token) {
		if (token == null || token.isBlank()) {
			return false;
		}
		return Boolean.TRUE.equals(redisTemplate.hasKey(keyFromToken(token)));
	}

	@Override
	public void revokeToken(String token) {
		if (token == null || token.isBlank()) {
			return;
		}
		redisTemplate.delete(keyFromToken(token));
	}

	private static String generateOpaqueToken() {
		SecureRandom random = new SecureRandom();
		byte[] bytes = new byte[TOKEN_BYTES];
		random.nextBytes(bytes);
		return HexFormat.of().formatHex(bytes);
	}

	@NonNull
	private static String keyFromToken(String token) {
		return KEY_PREFIX + sha256Hex(token);
	}

	private static String sha256Hex(String input) {
		MessageDigest digest;
		try {
			digest = MessageDigest.getInstance("SHA-256");
		} catch (NoSuchAlgorithmException e) {
			throw new IllegalStateException("SHA-256 not available", e);
		}
		byte[] hash = digest.digest(input.getBytes(StandardCharsets.UTF_8));
		return HexFormat.of().formatHex(hash);
	}
}
