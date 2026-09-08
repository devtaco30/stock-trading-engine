package com.flab.stocktradingengine.matching.worker;

import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.flab.stocktradingengine.matching.disruptor.AeronOrderReceiver;
import com.flab.stocktradingengine.matching.disruptor.MatchingEngine;

import io.aeron.Aeron;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;

/**
 * 파이프라인 연결 ① — 주문을 Aeron IPC로 받는 인테이크 경로 배선.
 * account-worker {@code AccountOrderIntakeConfig}와 같은 결(대칭 미러).
 *
 * <p>채널은 {@code aeron:ipc}(같은 머신, 공유 메모리)로 최소 형태다 — 실 UDP·게이트웨이의
 * accountId→스트림 라우팅 맵은 C5-4에서 다룬다. 스트림 ID(2002)는 matching-disruptor 코어의
 * {@code AeronMatchingEndToEndTest}가 이미 쓰는 값과 맞췄다 — account intake(4004)와는 값이
 * 달라도 어차피 프로세스별 별도 임베디드 드라이버라 충돌하지 않는다.</p>
 *
 * <p>빈 생성 순서(MediaDriver→Aeron→Subscription)가 그대로 소멸 순서의 역방향이 되도록
 * {@code destroyMethod}만 지정한다 — Spring이 빈 의존 그래프를 보고 Subscription을 Aeron보다,
 * Aeron을 MediaDriver보다 먼저 닫아준다.</p>
 */
@Configuration
public class MatchingOrderIntakeConfig {

    // 패키지 가시성 — 테스트(같은 패키지)가 프로덕션과 같은 채널·스트림으로 발행하도록 이 상수를 그대로 참조한다.
    static final String INTAKE_CHANNEL = "aeron:ipc";
    static final int INTAKE_STREAM_ID = 2002;

    @Bean(destroyMethod = "close")
    public MediaDriver mediaDriver() {
        return MediaDriver.launchEmbedded();
    }

    @Bean(destroyMethod = "close")
    public Aeron aeron(MediaDriver mediaDriver) {
        return Aeron.connect(new Aeron.Context().aeronDirectoryName(mediaDriver.aeronDirectoryName()));
    }

    @Bean(destroyMethod = "close")
    public Subscription matchingOrderSubscription(Aeron aeron) {
        return aeron.addSubscription(INTAKE_CHANNEL, INTAKE_STREAM_ID);
    }

    @Bean
    public AeronOrderReceiver matchingOrderReceiver(Subscription matchingOrderSubscription, MatchingEngine matchingEngine) {
        return new AeronOrderReceiver(matchingOrderSubscription, matchingEngine);
    }

    /**
     * {@link MatchingEngineLifecycle}(기본 phase 0)보다 늦은 phase로 둔다 — 엔진 소비자 스레드가
     * 먼저 돌기 시작한 뒤에 수신 스레드가 발행을 시작하게 하려는 의도다(엄밀히 필수는 아니다 —
     * 링버퍼는 소비자가 아직 안 돌아도 발행을 받아준다 — 그래도 방어적으로 순서를 준다).
     */
    @Bean
    public SmartLifecycle matchingOrderReceiverLifecycle(AeronOrderReceiver matchingOrderReceiver) {
        return new MatchingOrderReceiverLifecycle(matchingOrderReceiver);
    }
}
