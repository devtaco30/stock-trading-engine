package com.flab.stocktradingengine.account.worker;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import org.springframework.beans.factory.annotation.Value;
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

    @Bean(destroyMethod = "close")
    public ArchivingMediaDriver archivingMediaDriver(File accountArchiveDir) {
        String aeronDirectoryName = CommonContext.generateRandomDirName();

        return ArchivingMediaDriver.launch(
            new MediaDriver.Context().aeronDirectoryName(aeronDirectoryName),
            new Archive.Context()
                .controlChannel(CONTROL_REQUEST_CHANNEL)
                .replicationChannel(REPLICATION_CHANNEL)
                .deleteArchiveOnStart(false) // 저널은 재시작 넘어 보존해야 한다(2b-1b)
                .archiveDir(accountArchiveDir)
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
