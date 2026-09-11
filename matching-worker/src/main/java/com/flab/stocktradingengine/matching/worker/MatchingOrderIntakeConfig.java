package com.flab.stocktradingengine.matching.worker;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.flab.stocktradingengine.matching.disruptor.AeronOrderReceiver;
import com.flab.stocktradingengine.matching.disruptor.MatchingEngine;

import io.aeron.Aeron;
import io.aeron.CommonContext;
import io.aeron.Subscription;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchivingMediaDriver;
import io.aeron.archive.client.AeronArchive;
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
 * <h3>ArchivingMediaDriver (2c-1 — 저널 녹화 대상 이동)</h3>
 * <p>plain {@code MediaDriver.launchEmbedded()} 대신 {@link ArchivingMediaDriver}(임베디드
 * MediaDriver + Archive가 한 프로세스에 묶인 조합)를 띄운다. account-worker
 * {@code AccountOrderIntakeConfig}와 같은 이유로, 녹화 대상은 이 인테이크 스트림(2002)이 아니라
 * 저널 스트림({@link MatchingJournalArchiveConfig})이다 — 이 클래스는 녹화하지 않는다.</p>
 *
 * <p>빈 생성 순서(ArchivingMediaDriver→Aeron→Subscription)가 그대로 소멸 순서의 역방향이 되도록
 * {@code destroyMethod}만 지정한다 — Spring이 빈 의존 그래프를 보고 Subscription을 Aeron보다,
 * Aeron을 ArchivingMediaDriver보다 먼저 닫아준다.</p>
 */
@Configuration
public class MatchingOrderIntakeConfig {

    // 패키지 가시성 — 테스트(같은 패키지)가 프로덕션과 같은 채널·스트림으로 발행하도록 이 상수를 그대로 참조한다.
    static final String INTAKE_CHANNEL = "aeron:ipc";
    static final int INTAKE_STREAM_ID = 2002;

    // Archive 제어 채널 — account-worker AccountOrderIntakeConfig와 같은 관례값이다.
    private static final String CONTROL_REQUEST_CHANNEL = "aeron:udp?endpoint=localhost:8010";
    private static final String CONTROL_RESPONSE_CHANNEL = "aeron:udp?endpoint=localhost:0";
    private static final String REPLICATION_CHANNEL = "aeron:udp?endpoint=localhost:0";

    /**
     * @param archiveDirConfig {@code matching.worker.archive-dir} — 저널 녹화(2c-1)를 담는
     *                         Archive 카탈로그 디렉터리. 비워두면(기본, 테스트) 매 기동마다 새
     *                         임시 디렉터리를 쓴다(격리). 값을 주면(운영, K8s PV 마운트 지점처럼
     *                         재시작 사이 살아남는 고정 경로) 그 경로를 그대로 쓴다.
     */
    @Bean(destroyMethod = "close")
    public ArchivingMediaDriver archivingMediaDriver(
            @Value("${matching.worker.archive-dir:}") String archiveDirConfig) throws IOException {
        String aeronDirectoryName = CommonContext.generateRandomDirName();
        File archiveDir = resolveArchiveDir(archiveDirConfig);

        return ArchivingMediaDriver.launch(
            new MediaDriver.Context().aeronDirectoryName(aeronDirectoryName),
            new Archive.Context()
                .controlChannel(CONTROL_REQUEST_CHANNEL)
                .replicationChannel(REPLICATION_CHANNEL)
                .deleteArchiveOnStart(false) // 저널은 재시작 넘어 보존해야 한다(2c-1)
                .archiveDir(archiveDir)
        );
    }

    private File resolveArchiveDir(String archiveDirConfig) throws IOException {
        if (archiveDirConfig == null || archiveDirConfig.isBlank()) {
            return Files.createTempDirectory("matching-worker-archive-").toFile();
        }
        File archiveDir = new File(archiveDirConfig);
        Files.createDirectories(archiveDir.toPath());
        return archiveDir;
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
