package com.flab.stocktradingengine.api.resolver;

import jakarta.servlet.http.HttpServletRequest;
import org.springframework.core.MethodParameter;
import org.springframework.lang.Nullable;
import org.springframework.web.bind.support.WebDataBinderFactory;
import org.springframework.web.context.request.NativeWebRequest;
import org.springframework.web.method.support.HandlerMethodArgumentResolver;
import org.springframework.web.method.support.ModelAndViewContainer;

/**
 * {@link CurrentUserId}가 붙은 Long 파라미터에, request attribute "userId"를 주입한다.
 * <p>Interceptor에서 토큰 검증 후 request.setAttribute("userId", userId)로 설정한 값을 사용한다.</p>
 */
public class CurrentUserIdArgumentResolver implements HandlerMethodArgumentResolver {

    private static final String USER_ID_ATTRIBUTE = "userId";

    @Override
    public boolean supportsParameter(MethodParameter parameter) {
        return parameter.hasParameterAnnotation(CurrentUserId.class)
            && Long.class.equals(parameter.getParameterType());
    }

    @Override
    @Nullable
    public Object resolveArgument(
            MethodParameter parameter,
            @Nullable ModelAndViewContainer mavContainer,
            NativeWebRequest webRequest,
            @Nullable WebDataBinderFactory binderFactory) {
        HttpServletRequest request = webRequest.getNativeRequest(HttpServletRequest.class);
        if (request == null) {
            return null;
        }
        return request.getAttribute(USER_ID_ATTRIBUTE);
    }
}
