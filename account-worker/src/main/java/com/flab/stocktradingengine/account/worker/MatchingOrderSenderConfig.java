package com.flab.stocktradingengine.account.worker;

import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import io.aeron.Aeron;
import io.aeron.Publication;

/**
 * 계좌가 accept한 주문을 매칭 인테이크로 보내는 발신 배선(②-b). {@link AccountOrderIntakeConfig}가
 * 만든 account-worker 자체 Aeron(MediaDriver)을 그대로 써서 Publication만 새로 연다.
 *
 * <p>채널·스트림은 matching-worker의 {@code MatchingOrderIntakeConfig}(파이프라인 연결 ①)가 구독하는
 * 값과 반드시 같아야 한다 — account-worker는 모듈 경계상 matching-worker에 의존하지 않아 상수를
 * 공유하지 못하고 값만 맞춘다. 지금은 인프로세스 검증까지다(같은 임베디드 드라이버를 공유하는
 * 테스트 프로세스 안에서만 실제로 연결된다) — 실제 두 프로세스 연결은 C5-4에서 다룬다.</p>
 */
@Configuration
public class MatchingOrderSenderConfig {

    // 패키지 가시성 — 테스트(같은 패키지)가 이 값을 그대로 참조해 매칭 인테이크와 같은 스트림을 구독한다.
    static final String MATCHING_CHANNEL = "aeron:ipc";
    static final int MATCHING_STREAM_ID = 2002; // matching-worker MatchingOrderIntakeConfig.INTAKE_STREAM_ID와 동일해야 함

    @Bean(destroyMethod = "close")
    public Publication matchingOrderPublication(Aeron aeron) {
        return aeron.addPublication(MATCHING_CHANNEL, MATCHING_STREAM_ID);
    }

    /**
     * 반환형을 구현 클래스로 둔다 — {@link AccountEngineConfig#accountEngine}은 이 빈을
     * {@code MatchingOrderSender} 인터페이스로 주입받고, {@link #aeronMatchingOrderSenderLifecycle}은
     * {@code start()}·{@code close()}(publisher 스레드 제어) 때문에 구현 클래스가 필요하다.
     */
    @Bean
    public AeronMatchingOrderSender matchingOrderSender(Publication matchingOrderPublication) {
        return new AeronMatchingOrderSender(matchingOrderPublication);
    }

    @Bean
    public SmartLifecycle aeronMatchingOrderSenderLifecycle(AeronMatchingOrderSender matchingOrderSender) {
        return new AeronMatchingOrderSenderLifecycle(matchingOrderSender);
    }
}
