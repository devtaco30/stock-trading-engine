package com.flab.stocktradingengine.interceptor;

import com.flab.stocktradingengine.dummy.DummyToken;
import com.flab.stocktradingengine.exception.AuthenticationException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerInterceptor;

/**
 * 인증 헤더 검증 Interceptor
 * 
 * <p>현재는 더미 검증만 수행합니다.
 * 실제 토큰 검증 로직은 auth 모듈 구현 후 추가 예정입니다.
 */
@Component
public class AuthenticationInterceptor implements HandlerInterceptor {

    private static final String AUTHORIZATION_HEADER = "Authorization";
    private static final String BEARER_PREFIX = "Bearer ";

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
        // OPTIONS 요청은 CORS preflight이므로 통과
        if ("OPTIONS".equals(request.getMethod())) {
            return true;
        }

        String authorizationHeader = request.getHeader(AUTHORIZATION_HEADER);

        // 헤더 없음
        if (authorizationHeader == null || authorizationHeader.isBlank()) {
            throw AuthenticationException.authRequired();
        }

        // Bearer 토큰 형식 검증
        if (!authorizationHeader.startsWith(BEARER_PREFIX)) {
            throw AuthenticationException.invalidTokenFormat("토큰 형식이 올바르지 않습니다. Bearer 토큰을 사용해주세요");
        }

        String token = authorizationHeader.substring(BEARER_PREFIX.length()).trim();
        
        // 토큰이 비어있음
        if (token.isBlank()) {
            throw AuthenticationException.invalidTokenFormat("토큰이 비어있습니다");
        }

        // 더미 토큰 검증
        if (!DummyToken.isValid(token)) {
            throw AuthenticationException.invalidTokenFormat("유효하지 않은 토큰입니다");
        }

        // TODO: 실제 토큰 검증 로직 구현
        // 1. JWT 토큰 파싱
        // 2. 토큰 서명 검증
        // 3. 토큰 만료 시간 확인
        // 4. 사용자 정보 추출 및 SecurityContext에 저장

        return true;
    }
}
