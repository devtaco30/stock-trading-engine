package com.flab.stocktradingengine.api.config;

import com.flab.stocktradingengine.api.interceptor.AuthenticationInterceptor;
import com.flab.stocktradingengine.api.resolver.CurrentUserIdArgumentResolver;
import java.util.List;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.method.support.HandlerMethodArgumentResolver;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

/**
 * Spring MVC 설정
 *
 * <p>Interceptor·ArgumentResolver 등록 및 기타 MVC 설정을 관리합니다.
 */
@Configuration
public class WebMvcConfig implements WebMvcConfigurer {

    private final AuthenticationInterceptor authenticationInterceptor;

    public WebMvcConfig(AuthenticationInterceptor authenticationInterceptor) {
        this.authenticationInterceptor = authenticationInterceptor;
    }

    @Override
    public void addArgumentResolvers(List<HandlerMethodArgumentResolver> resolvers) {
        resolvers.add(new CurrentUserIdArgumentResolver());
    }

    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        registry.addInterceptor(authenticationInterceptor)
            .addPathPatterns("/api/v1/**", "/api/v2/**")  // v1·v2 공통 인증(fork5 U1a — v2 게이트웨이도 같은 토큰 인증을 탄다)
            .excludePathPatterns(
                "/api/v1/health",   // 헬스체크는 인증 제외 (필요시)
                "/api/v1/stocks/**", // 시세/종목은 공개 API
                "/error"            // 에러 페이지는 인증 제외
            );
    }
}
