package com.flab.stocktradingengine.account.worker.support;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

import com.flab.stocktradingengine.account.disruptor.io.MatchingOrderSender;

/**
 * {@link NoOpMatchingOrderSender}를 {@code @Primary}로 등록해, 매칭 전달을 검증하지 않는 테스트가
 * 진짜 {@code AeronMatchingOrderSender}(그 빈 자체는 프로덕션 설정 그대로 계속 만들어진다 — 이
 * 설정은 그걸 대체하는 게 아니라 {@code AccountEngineConfig.accountEngine}이 주입받는 자리에서만
 * 우선하게 만든다) 대신 이걸 쓰게 한다. `@Import`(단일 `@SpringBootTest`) 또는
 * `SpringApplicationBuilder.sources(...)`(수동 기동)로 등록한다.
 *
 * <h3>⚠️ 일부러 {@code @Configuration}을 안 붙인다</h3>
 * <p>이 클래스는 {@code AccountWorkerApplication}의 컴포넌트 스캔 범위
 * ({@code com.flab.stocktradingengine.account.worker}) 아래 패키지에 있다 — {@code @Configuration}을
 * 붙이면 스캔이 이 클래스를 자동으로 주워 등록해서, 명시적으로 {@code @Import}·{@code .sources(...)}
 * 하지 않은 테스트(매칭 전달을 실제로 단언하는 {@code AccountToMatchingForwardingIntegrationTest}
 * 포함)까지 이 더블이 이겨버린다 — 지금 고치는 중인 "주문이 조용히 매칭에 안 감" 버그를 테스트
 * 환경에 그대로 심는 셈이다(이 프로젝트에서 실제로 한 번 재현됨). 이 파일의 다른 {@code RecorderConfig}류
 * 테스트 설정들과 같은 이유로 {@code @Bean}만 있는 평범한 클래스로 두고, 명시적으로 등록한
 * 컨텍스트에서만 처리되게 한다.</p>
 */
public class NoOpMatchingOrderSenderTestConfig {

    @Bean
    @Primary
    public MatchingOrderSender noOpMatchingOrderSender() {
        return new NoOpMatchingOrderSender();
    }
}
