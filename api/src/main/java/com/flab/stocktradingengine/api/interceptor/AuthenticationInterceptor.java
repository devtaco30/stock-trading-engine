package com.flab.stocktradingengine.api.interceptor;

import org.springframework.lang.NonNull;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerInterceptor;

import com.flab.stocktradingengine.api.exception.AuthenticationException;
import com.flab.stocktradingengine.user.exception.InvalidTokenException;
import com.flab.stocktradingengine.user.token.TokenService;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;

/**
 * 인증 헤더 검증 Interceptor
 * <p>user 모듈의 {@link TokenService}로 토큰 검증 후 request에 userId 설정.</p>
 */
@Component
@RequiredArgsConstructor
public class AuthenticationInterceptor implements HandlerInterceptor {

    private static final String AUTHORIZATION_HEADER = "Authorization";
    private static final String BEARER_PREFIX = "Bearer ";

    private final TokenService tokenService;

    @Override
    public boolean preHandle(@NonNull HttpServletRequest request, @NonNull HttpServletResponse response, @NonNull Object handler) {
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

        try {
            long userId = tokenService.validateAndGetUserId(token);
            request.setAttribute("userId", userId);
        } catch (InvalidTokenException e) {
            throw AuthenticationException.invalidTokenFormat(e.getMessage());
        }

        return true;
    }
}
