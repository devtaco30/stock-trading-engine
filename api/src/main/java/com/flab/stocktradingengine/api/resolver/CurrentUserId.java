package com.flab.stocktradingengine.api.resolver;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 컨트롤러 메서드 파라미터에 붙이면, 인증 Interceptor가 설정한 현재 사용자 ID를 주입한다.
 * <p>{@link CurrentUserIdArgumentResolver}가 request attribute "userId"에서 값을 꺼내 넣어준다.</p>
 */
@Target(ElementType.PARAMETER)
@Retention(RetentionPolicy.RUNTIME)
public @interface CurrentUserId {
}
