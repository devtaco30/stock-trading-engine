package com.flab.stocktradingengine.api.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.flab.stocktradingengine.api.messaging.AeronAccountOrderSender;

import io.aeron.Aeron;
import io.aeron.CommonContext;
import io.aeron.Publication;
import io.aeron.driver.MediaDriver;

/**
 * fork5, U1b — v2 게이트웨이가 계좌 인테이크로 주문을 발신할 Aeron 배선. 발신만 하는 클라이언트라
 * Archive(durable 녹화)는 안 둔다 — account-worker/matching-worker의 {@code *IntakeConfig}와
 * 달리 {@link MediaDriver}만 띄운다.
 *
 * <h3>범위 — 같은 머신 IPC까지 (C5)</h3>
 * <p>채널은 {@code aeron:ipc}다. 지금 api와 account-worker가 실제로는 같은 프로세스 경계를
 * 넘어 붙지 않는다(각자 별도 임베디드 드라이버) — 실제 두 프로세스 연결(UDP로 채널 주소만
 * 바뀌는 전환)은 C5 몫이고, 이 유닛은 발신 로직·인코딩까지만 검증한다.</p>
 *
 * <h3>스트림 상수 복제</h3>
 * <p>account-worker {@code AccountOrderIntakeConfig.INTAKE_CHANNEL/INTAKE_STREAM_ID}와 반드시
 * 같아야 한다. 그 상수가 account-worker 안에서 package-private이라 api 모듈에서 직접 참조하지
 * 못해 값만 복제한다(다른 워커 간 스트림 상수들과 같은 관례 — core 공유 상수 통합은 별도 개선,
 * 이번엔 안 한다).</p>
 */
@Configuration
public class AccountOrderPublishConfig {

    // 패키지 가시성 — 테스트(같은 패키지)가 이 값을 그대로 참조한다.
    static final String ACCOUNT_INTAKE_CHANNEL = "aeron:ipc";
    static final int ACCOUNT_INTAKE_STREAM_ID = 4004; // account-worker AccountOrderIntakeConfig.INTAKE_STREAM_ID와 동일해야 함

    @Bean(destroyMethod = "close")
    public MediaDriver accountOrderMediaDriver() {
        String aeronDirectoryName = CommonContext.generateRandomDirName();
        return MediaDriver.launchEmbedded(new MediaDriver.Context().aeronDirectoryName(aeronDirectoryName));
    }

    @Bean(destroyMethod = "close")
    public Aeron accountOrderAeron(MediaDriver accountOrderMediaDriver) {
        return Aeron.connect(new Aeron.Context().aeronDirectoryName(accountOrderMediaDriver.aeronDirectoryName()));
    }

    @Bean(destroyMethod = "close")
    public Publication accountOrderPublication(Aeron accountOrderAeron) {
        return accountOrderAeron.addPublication(ACCOUNT_INTAKE_CHANNEL, ACCOUNT_INTAKE_STREAM_ID);
    }

    @Bean
    public AeronAccountOrderSender aeronAccountOrderSender(Publication accountOrderPublication) {
        return new AeronAccountOrderSender(accountOrderPublication);
    }
}
