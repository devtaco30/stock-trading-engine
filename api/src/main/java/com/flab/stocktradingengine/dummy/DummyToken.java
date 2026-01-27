package com.flab.stocktradingengine.dummy;

import java.util.Set;

/**
 * 더미 토큰 관리
 * 
 * <p>통합 테스트를 위한 더미 토큰을 제공합니다.
 * 실제 인증 구현 전까지 사용됩니다.
 */
public class DummyToken {

    /**
     * 유효한 더미 토큰 목록
     * 
     * <p>현재는 모든 계좌에 대해 동일한 토큰을 사용합니다.
     * 실제 구현 시에는 토큰에서 사용자 정보를 추출하여 계좌 소유권을 검증합니다.
     */
    private static final Set<String> VALID_TOKENS = Set.of(
        "dummy-token-001",
        "dummy-token-002",
        "dummy-token-003",
        "dummy-token-004"
    );

    /**
     * 토큰 유효성 검증
     * 
     * @param token 검증할 토큰
     * @return 유효한 토큰이면 true, 아니면 false
     */
    public static boolean isValid(String token) {
        return token != null && VALID_TOKENS.contains(token);
    }
}
