package com.flab.stocktradingengine.account.worker.config;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.flab.stocktradingengine.account.disruptor.engine.AccountEngine;
import com.flab.stocktradingengine.account.disruptor.io.AccountOrderReceiver;
import com.flab.stocktradingengine.account.worker.lifecycle.AccountOrderReceiverLifecycle;
import com.flab.stocktradingengine.aeron.AeronStreamIds;

import io.aeron.Aeron;
import io.aeron.CommonContext;
import io.aeron.Subscription;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchivingMediaDriver;
import io.aeron.archive.client.AeronArchive;
import io.aeron.driver.MediaDriver;

/**
 * C5-1b — 매수·매도 주문을 Aeron IPC로 받는 인테이크 경로 배선.
 *
 * <p>채널은 {@code aeron:ipc}(같은 머신, 공유 메모리)로 최소 형태다 — 실 UDP·게이트웨이의
 * accountId→스트림 라우팅 맵은 C5-4에서 다룬다. 그때까지 스트림 ID는 이 워커 인스턴스 전체가
 * 공유하는 고정 상수다.</p>
 *
 * <h3>ArchivingMediaDriver (2a → 2b-1b에서 녹화 대상 이동)</h3>
 * <p>plain {@code MediaDriver.launchEmbedded()} 대신 {@link ArchivingMediaDriver}(임베디드
 * MediaDriver + Archive가 한 프로세스에 묶인 조합)를 띄운다. {@code launchEmbedded()}가 내부적으로
 * 하는 것과 같이 {@link CommonContext#generateRandomDirName}으로 aeron 디렉터리를 매 기동마다
 * 무작위로 잡는다 — IPC 버퍼는 프로세스 생애만큼만 필요해 재시작 사이 보존할 이유가 없다.</p>
 *
 * <p>2a에서는 이 인테이크 스트림(4004) 자체를 녹화했지만, 체결·정산은 Kafka 컨슈머가
 * {@code AccountEngine}에 직접 publish 해 인테이크를 거치지 않는다 — 인테이크만 녹화하면 리플레이가
 * 체결·정산을 놓친다. 그래서 2b-1b부터 녹화 대상을 저널 스트림({@link AccountJournalArchiveConfig},
 * 다섯 타입 전부를 처리 순서대로 보는 유일한 지점)으로 옮겼다. 이 클래스는 더 이상 녹화하지 않는다
 * — 인테이크 in-transit 유실은 게이트웨이 재전송(requestId 멱등, C5-1a)이 책임진다("링버퍼 앞=전송
 * 책임 / 뒤=저널·리플레이 책임"의 경계).</p>
 */
@Configuration
public class AccountOrderIntakeConfig {

    // 패키지 가시성 — 테스트(같은 패키지)가 프로덕션과 같은 채널로 발행하도록 이 기본값을 그대로
    // 참조한다. 실제 채널은 transport.account-intake.channel 속성에서 해석된다(fork1 Unit 1).
    // 스트림 ID는 core AeronStreamIds.ACCOUNT_INTAKE로 공유한다.
    static final String DEFAULT_INTAKE_CHANNEL = "aeron:ipc";

    // Archive 제어 채널(주문 데이터 자체가 아니라 "녹화 시작해라" 같은 제어 요청/응답이 오가는 채널) —
    // account.worker.archive.control-channel 속성에서 해석된다(fork1 Unit 1 — 3-JVM 실행 시
    // matching-worker와 포트가 겹치지 않게 워커별로 다른 값을 준다). 기본값은 aeron-io/aeron
    // 저장소 자체 테스트 헬퍼(TestContexts.localhostArchive/localhostAeronArchive)가 쓰는 것과
    // 같은 관례값: 요청 채널은 널리 쓰이는 고정 포트(8010), 응답·리플리케이션 채널은 ephemeral
    // (포트 0, OS가 배정) — 이 둘은 3-JVM에서도 그대로 둔다. 가치·주문 데이터는 여전히
    // transport.account-intake.channel(DEFAULT_INTAKE_CHANNEL)로만 오간다.
    private static final String DEFAULT_CONTROL_REQUEST_CHANNEL = "aeron:udp?endpoint=localhost:8010";
    private static final String CONTROL_RESPONSE_CHANNEL = "aeron:udp?endpoint=localhost:0";
    private static final String REPLICATION_CHANNEL = "aeron:udp?endpoint=localhost:0";

    /**
     * @param archiveDirConfig {@code account.worker.archive-dir} — 저널 녹화(2b-1b)·스냅샷
     *                         파일(2d-2b, {@code AccountSnapshotConfig})을 같이 담는 durable
     *                         디렉터리. 비워두면(기본, 테스트) 매 기동마다 새 임시 디렉터리를
     *                         쓴다(격리). 값을 주면(운영, K8s PV 마운트 지점처럼 재시작 사이
     *                         살아남는 고정 경로) 그 경로를 그대로 쓴다 — {@code deleteArchiveOnStart(false)}
     *                         와 짝을 이뤄야 재시작 뒤에도 이전 저널 녹화가 카탈로그에 남는다.
     */
    @Bean
    public File accountArchiveDir(@Value("${account.worker.archive-dir:}") String archiveDirConfig) throws IOException {
        if (archiveDirConfig == null || archiveDirConfig.isBlank()) {
            return Files.createTempDirectory("account-worker-archive-").toFile();
        }
        File archiveDir = new File(archiveDirConfig);
        Files.createDirectories(archiveDir.toPath());
        return archiveDir;
    }

    /**
     * @param segmentFileLength   {@code account.worker.archive.segment-file-length} — Archive 세그먼트
     *                            파일 길이(바이트). 비워두면(기본, 운영) Aeron 기본값(128MB)을 그대로
     *                            쓴다. **ADR-032 I5 U3 전용** — `AccountArchiveSegmentPurger`의 회수가
     *                            실제로 세그먼트를 지우는지 통합테스트에서 관측하려면 기본 세그먼트
     *                            크기로는 테스트 데이터로 세그먼트 경계를 못 넘어 "아무것도 안 지워진
     *                            채 통과"하는 거짓 green이 난다(U1·U2에서 먼저 겪음). **운영에서 이
     *                            값을 작게 주면 세그먼트 파일 개수가 폭증하니 절대 운영에 설정하지
     *                            말 것.**
     * @param ipcTermBufferLength {@code account.worker.archive.ipc-term-buffer-length} — 이 드라이버의
     *                            모든 IPC 채널(저널 포함) term 버퍼 길이. 비워두면 Aeron 기본값을
     *                            그대로 쓴다. Archive는 recording의 실제 segmentFileLength를
     *                            {@code max(segmentFileLength, termBufferLength)}로 강제하므로
     *                            (서버 소스로 확인), segmentFileLength만 줄여도 term 길이가 기본값
     *                            (수십 MB)이면 세그먼트가 그 값으로 도로 커진다 — U3에서 이 값도
     *                            같이 줄여야 세그먼트가 실제로 작아진다. **segmentFileLength와 같은
     *                            이유로 운영에 설정하지 말 것.**
     */
    @Bean(destroyMethod = "close")
    public ArchivingMediaDriver archivingMediaDriver(
            File accountArchiveDir,
            @Value("${account.worker.archive.control-channel:" + DEFAULT_CONTROL_REQUEST_CHANNEL + "}") String controlRequestChannel,
            @Value("${account.worker.archive.segment-file-length:0}") int segmentFileLength,
            @Value("${account.worker.archive.ipc-term-buffer-length:0}") int ipcTermBufferLength) {
        String aeronDirectoryName = CommonContext.generateRandomDirName();

        MediaDriver.Context mediaDriverContext = new MediaDriver.Context().aeronDirectoryName(aeronDirectoryName);
        if (ipcTermBufferLength > 0) {
            mediaDriverContext.ipcTermBufferLength(ipcTermBufferLength);
        }

        Archive.Context archiveContext = new Archive.Context()
            .controlChannel(controlRequestChannel)
            .replicationChannel(REPLICATION_CHANNEL)
            .deleteArchiveOnStart(false) // 저널은 재시작 넘어 보존해야 한다(2b-1b)
            .archiveDir(accountArchiveDir);
        if (segmentFileLength > 0) {
            archiveContext.segmentFileLength(segmentFileLength);
        }

        return ArchivingMediaDriver.launch(mediaDriverContext, archiveContext);
    }

    @Bean(destroyMethod = "close")
    public Aeron aeron(ArchivingMediaDriver archivingMediaDriver) {
        return Aeron.connect(new Aeron.Context()
            .aeronDirectoryName(archivingMediaDriver.mediaDriver().aeronDirectoryName()));
    }

    @Bean(destroyMethod = "close")
    public AeronArchive aeronArchive(
            Aeron aeron,
            @Value("${account.worker.archive.control-channel:" + DEFAULT_CONTROL_REQUEST_CHANNEL + "}") String controlRequestChannel) {
        return AeronArchive.connect(new AeronArchive.Context()
            .aeron(aeron)
            .ownsAeronClient(false) // Aeron 빈은 우리가 별도로 소유·소멸시킨다(위 aeron() 빈)
            .controlRequestChannel(controlRequestChannel)
            .controlResponseChannel(CONTROL_RESPONSE_CHANNEL));
    }

    @Bean(destroyMethod = "close")
    public Subscription accountOrderSubscription(
            Aeron aeron,
            @Value("${transport.account-intake.channel:" + DEFAULT_INTAKE_CHANNEL + "}") String intakeChannel) {
        return aeron.addSubscription(intakeChannel, AeronStreamIds.ACCOUNT_INTAKE);
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
