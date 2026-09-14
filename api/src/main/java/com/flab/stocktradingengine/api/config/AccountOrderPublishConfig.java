package com.flab.stocktradingengine.api.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.flab.stocktradingengine.aeron.AeronStreamIds;
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
 * <h3>스트림 ID·채널 (fork1, LLD §3-1·§3-2)</h3>
 * <p>스트림 ID는 {@link AeronStreamIds#ACCOUNT_INTAKE}로 account-worker
 * {@code AccountOrderIntakeConfig}와 core에서 공유한다. 채널은 {@code transport.account-intake.channel}
 * 속성으로 외부화돼 있다 — 두 앱은 모듈 경계상 서로 의존하지 않아 값 일치를 코드가 강제하진
 * 못하므로, 같은 값을 각자의 config에 넣어야 한다(fork1은 unicast 고정 endpoint, 라우팅 맵은
 * fork2).</p>
 */
@Configuration
public class AccountOrderPublishConfig {

    // 패키지 가시성 — 테스트(같은 패키지)가 이 값을 그대로 참조한다.
    static final String DEFAULT_ACCOUNT_INTAKE_CHANNEL = "aeron:ipc";

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
    public Publication accountOrderPublication(
            Aeron accountOrderAeron,
            @Value("${transport.account-intake.channel:" + DEFAULT_ACCOUNT_INTAKE_CHANNEL + "}") String accountIntakeChannel) {
        return accountOrderAeron.addPublication(accountIntakeChannel, AeronStreamIds.ACCOUNT_INTAKE);
    }

    @Bean
    public AeronAccountOrderSender aeronAccountOrderSender(Publication accountOrderPublication) {
        return new AeronAccountOrderSender(accountOrderPublication);
    }
}
