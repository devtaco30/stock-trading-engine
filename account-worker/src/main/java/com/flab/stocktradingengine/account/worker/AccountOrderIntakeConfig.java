package com.flab.stocktradingengine.account.worker;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.flab.stocktradingengine.account.disruptor.AccountEngine;
import com.flab.stocktradingengine.account.disruptor.AccountOrderReceiver;

import io.aeron.Aeron;
import io.aeron.CommonContext;
import io.aeron.Subscription;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchivingMediaDriver;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.driver.MediaDriver;

/**
 * C5-1b — 매수·매도 주문을 Aeron IPC로 받는 인테이크 경로 배선. 2a부터는 이 인테이크 스트림을
 * Aeron Archive로 durable 녹화한다(입력 로그 — 리플레이는 2b, 매칭 쪽 저널→Archive는 2c).
 *
 * <p>채널은 {@code aeron:ipc}(같은 머신, 공유 메모리)로 최소 형태다 — 실 UDP·게이트웨이의
 * accountId→스트림 라우팅 맵은 C5-4에서 다룬다. 그때까지 스트림 ID는 이 워커 인스턴스 전체가
 * 공유하는 고정 상수다.</p>
 *
 * <h3>ArchivingMediaDriver (2a)</h3>
 * <p>plain {@code MediaDriver.launchEmbedded()} 대신 {@link ArchivingMediaDriver}(임베디드
 * MediaDriver + Archive가 한 프로세스에 묶인 조합)를 띄운다. {@code launchEmbedded()}가 내부적으로
 * 하는 것과 같이 {@link CommonContext#generateRandomDirName}으로 aeron 디렉터리를 매 기동마다
 * 무작위로 잡아 다른 인스턴스(다른 테스트)와 충돌하지 않게 한다. archive 디렉터리도 임시 디렉터리로
 * 매번 새로 만든다 — 지금 단위는 "녹화가 된다"까지고, 카탈로그를 프로세스 재기동 사이에 남기는
 * 정책은 이후(2b 리플레이) 단위에서 다룬다.</p>
 *
 * <h3>녹화 시작 순서 (주문 유실 방지)</h3>
 * <p>{@link #accountIntakeRecordingSubscriptionId}가 {@link #accountOrderSubscription}보다 먼저
 * 만들어지도록 그 빈을 파라미터로 받아 의존시킨다 — Spring은 빈 그래프 순서로 생성하므로,
 * Archive가 이 채널·스트림을 녹화 시작한 뒤에야 우리 자신의 Subscription이 만들어진다(그 뒤에
 * {@link AccountOrderReceiverLifecycle}이 폴링을 시작한다). 녹화가 나중에 시작되면 그 사이 도착한
 * 주문이 기록에서 빠질 수 있어 순서를 지킨다.</p>
 *
 * <p>빈 생성 순서(ArchivingMediaDriver→Aeron→AeronArchive→녹화→Subscription)가 그대로 소멸
 * 순서의 역방향이 되도록 {@code destroyMethod}만 지정한다 — Spring이 빈 의존 그래프를 보고
 * 역순으로 닫아준다.</p>
 */
@Configuration
public class AccountOrderIntakeConfig {

    // 패키지 가시성 — 테스트(같은 패키지)가 프로덕션과 같은 채널·스트림으로 발행하도록 이 상수를 그대로 참조한다.
    static final String INTAKE_CHANNEL = "aeron:ipc";
    static final int INTAKE_STREAM_ID = 4004;

    // Archive 제어 채널(주문 데이터 자체가 아니라 "녹화 시작해라" 같은 제어 요청/응답이 오가는 채널) —
    // 기본값이 없어 필수다. aeron-io/aeron 저장소 자체 테스트 헬퍼(TestContexts.localhostArchive/
    // localhostAeronArchive)가 쓰는 것과 같은 관례값이다: 요청 채널은 널리 쓰이는 고정 포트(8010),
    // 응답·리플리케이션 채널은 ephemeral(포트 0, OS가 배정). 이 유닛은 원격·리플리케이션 자체를
    // 쓰지 않는다 — 가치·주문 데이터는 여전히 aeron:ipc(INTAKE_CHANNEL)로만 오간다.
    private static final String CONTROL_REQUEST_CHANNEL = "aeron:udp?endpoint=localhost:8010";
    private static final String CONTROL_RESPONSE_CHANNEL = "aeron:udp?endpoint=localhost:0";
    private static final String REPLICATION_CHANNEL = "aeron:udp?endpoint=localhost:0";

    @Bean(destroyMethod = "close")
    public ArchivingMediaDriver archivingMediaDriver() throws IOException {
        String aeronDirectoryName = CommonContext.generateRandomDirName();
        File archiveDir = Files.createTempDirectory("account-worker-archive-").toFile();

        return ArchivingMediaDriver.launch(
            new MediaDriver.Context().aeronDirectoryName(aeronDirectoryName),
            new Archive.Context()
                .controlChannel(CONTROL_REQUEST_CHANNEL)
                .replicationChannel(REPLICATION_CHANNEL)
                .deleteArchiveOnStart(true)
                .archiveDir(archiveDir)
        );
    }

    @Bean(destroyMethod = "close")
    public Aeron aeron(ArchivingMediaDriver archivingMediaDriver) {
        return Aeron.connect(new Aeron.Context()
            .aeronDirectoryName(archivingMediaDriver.mediaDriver().aeronDirectoryName()));
    }

    @Bean(destroyMethod = "close")
    public AeronArchive aeronArchive(Aeron aeron) {
        return AeronArchive.connect(new AeronArchive.Context()
            .aeron(aeron)
            .ownsAeronClient(false) // Aeron 빈은 우리가 별도로 소유·소멸시킨다(위 aeron() 빈)
            .controlRequestChannel(CONTROL_REQUEST_CHANNEL)
            .controlResponseChannel(CONTROL_RESPONSE_CHANNEL));
    }

    /**
     * 인테이크 채널·스트림 녹화를 시작한다(2a). 반환값(Archive 구독 ID)은 안 쓴다 — 이 빈이 존재하는
     * 이유는 {@link #accountOrderSubscription}이 이 빈에 의존하게 만들어 생성 순서를 강제하는
     * 것뿐이다(클래스 javadoc "녹화 시작 순서" 참고).
     */
    @Bean
    public Long accountIntakeRecordingSubscriptionId(AeronArchive aeronArchive) {
        return aeronArchive.startRecording(INTAKE_CHANNEL, INTAKE_STREAM_ID, SourceLocation.LOCAL);
    }

    @Bean(destroyMethod = "close")
    public Subscription accountOrderSubscription(Aeron aeron, Long accountIntakeRecordingSubscriptionId) {
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
