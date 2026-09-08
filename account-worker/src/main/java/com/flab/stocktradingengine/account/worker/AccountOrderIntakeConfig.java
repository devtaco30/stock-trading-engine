package com.flab.stocktradingengine.account.worker;

import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.flab.stocktradingengine.account.disruptor.AccountEngine;
import com.flab.stocktradingengine.account.disruptor.AccountOrderReceiver;

import io.aeron.Aeron;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;

/**
 * C5-1b — 매수·매도 주문을 Aeron IPC로 받는 인테이크 경로 배선.
 *
 * <p>채널은 {@code aeron:ipc}(같은 머신, 공유 메모리)로 최소 형태다 — 실 UDP·게이트웨이의
 * accountId→스트림 라우팅 맵은 C5-4에서 다룬다. 그때까지 스트림 ID는 이 워커 인스턴스 전체가
 * 공유하는 고정 상수다.</p>
 *
 * <p>빈 생성 순서(MediaDriver→Aeron→Subscription)가 그대로 소멸 순서의 역방향이 되도록
 * {@code destroyMethod}만 지정한다 — Spring이 빈 의존 그래프를 보고 Subscription을 Aeron보다,
 * Aeron을 MediaDriver보다 먼저 닫아준다.</p>
 */
@Configuration
public class AccountOrderIntakeConfig {

    // 패키지 가시성 — 테스트(같은 패키지)가 프로덕션과 같은 채널·스트림으로 발행하도록 이 상수를 그대로 참조한다.
    static final String INTAKE_CHANNEL = "aeron:ipc";
    static final int INTAKE_STREAM_ID = 4004;

    @Bean(destroyMethod = "close")
    public MediaDriver mediaDriver() {
        return MediaDriver.launchEmbedded();
    }

    @Bean(destroyMethod = "close")
    public Aeron aeron(MediaDriver mediaDriver) {
        return Aeron.connect(new Aeron.Context().aeronDirectoryName(mediaDriver.aeronDirectoryName()));
    }

    @Bean(destroyMethod = "close")
    public Subscription accountOrderSubscription(Aeron aeron) {
        return aeron.addSubscription(INTAKE_CHANNEL, INTAKE_STREAM_ID);
    }

    @Bean
    public AccountOrderReceiver accountOrderReceiver(Subscription accountOrderSubscription, AccountEngine accountEngine) {
        return new AccountOrderReceiver(accountOrderSubscription, accountEngine);
    }

    /**
     * {@link AccountEngineConfig#accountEngineLifecycle}보다 늦은 phase로 둔다 — 엔진 소비자
     * 스레드가 먼저 돌기 시작한 뒤에 수신 스레드가 발행을 시작하게 하려는 의도다(엄밀히 필수는
     * 아니다 — 링버퍼는 소비자가 아직 안 돌아도 발행을 받아준다 — 그래도 방어적으로 순서를 준다).
     */
    @Bean
    public SmartLifecycle accountOrderReceiverLifecycle(AccountOrderReceiver receiver) {
        return new AccountOrderReceiverLifecycle(receiver);
    }
}
