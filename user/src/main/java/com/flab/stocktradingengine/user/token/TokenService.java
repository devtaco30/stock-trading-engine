package com.flab.stocktradingengine.user.token;

/**
 * 토큰 발급·검증·폐기 서비스 (user 모듈).
 * <p>구현체 예: 불투명 토큰 + Redis. 인터페이스로 둔 이유: (1) 테스트 시 Mock 용이 (2) 구현체 교체 시 계약만 유지 (3) api는 추상에만 의존.</p>
 */
public interface TokenService {

	/**
	 * 사용자 ID로 토큰 발급.
	 *
	 * @param userId 사용자 ID
	 * @return 발급된 토큰 문자열 (클라이언트에 전달)
	 */
	String issueToken(long userId);

	/**
	 * 토큰 검증 후 사용자 ID 반환.
	 *
	 * @param token Bearer 제외한 토큰 문자열
	 * @return 해당 사용자 ID
	 * @throws com.flab.stocktradingengine.user.exception.InvalidTokenException 토큰이 유효하지 않을 때
	 */
	long validateAndGetUserId(String token);

	/**
	 * 토큰 유효 여부만 검사.
	 */
	boolean isValid(String token);

	/**
	 * 토큰 폐기 (로그아웃 등). 이미 없어도 예외 없음.
	 *
	 * @param token Bearer 제외한 토큰 문자열
	 */
	void revokeToken(String token);
}
