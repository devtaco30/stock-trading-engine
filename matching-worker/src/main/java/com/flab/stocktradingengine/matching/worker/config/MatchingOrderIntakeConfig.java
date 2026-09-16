package com.flab.stocktradingengine.matching.worker.config;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.flab.stocktradingengine.aeron.AeronStreamIds;
import com.flab.stocktradingengine.codec.JournaledOrder;
import com.flab.stocktradingengine.matching.disruptor.engine.MatchingEngine;
import com.flab.stocktradingengine.matching.disruptor.io.AeronOrderReceiver;
import com.flab.stocktradingengine.matching.worker.lifecycle.MatchingOrderReceiverLifecycle;
import com.flab.stocktradingengine.matching.worker.recovery.MatchingOrderIntakeReplayer;
import com.flab.stocktradingengine.matching.worker.recovery.StoredMatchingSnapshot;

import io.aeron.Aeron;
import io.aeron.CommonContext;
import io.aeron.Subscription;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchivingMediaDriver;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.codecs.SourceLocation;
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
 * MediaDriver + Archive가 한 프로세스에 묶인 조합)를 띄운다. I6까지는 녹화 대상이 이 인테이크
 * 스트림(2002)이 아니라 저널 스트림({@link MatchingJournalArchiveConfig})뿐이었다 — 이 클래스는
 * 녹화하지 않았다.</p>
 *
 * <h3>주문 인테이크도 REMOTE 녹화한다 (I2 U1)</h3>
 * <p>계좌가 매칭으로 보내다 실패하면(발신 큐 가득 참, Aeron offer 실패) 그 주문은 매칭에
 * 도착하지 않는다 — 이건 이 클래스가 못 막는다(발신측 문제, {@code AeronMatchingOrderSender}).
 * 이 클래스가 닫는 건 그 반대 gap이다: 매칭이 주문을 받긴 받았는데(Aeron 드라이버가 디스크에
 * 못 적기 전에) 링버퍼에 들어가 반영되기 전에 프로세스가 죽는 경우 — account-worker
 * {@code AccountFillIntakeConfig}가 체결 방향에 이미 확립한 것과 정확히 같은 패턴(대칭 적용,
 * {@code decision_records/i234-transport-loss-design.md} 후보1)을 주문 방향에 적용한다.
 * 발행자(계좌 샤드)가 여럿이면 recording이 갈라지는 문제는 account
 * {@code AccountFillReplayer}가 I1에서 만든 "발행자별 위치" 구조를 그대로 재사용한다(I2 U2b).</p>
 *
 * <h3>순서 (전달 유실 방지) — account {@code AccountJournalArchiveConfig} "순서" 절과 같은 이유</h3>
 * <p>①{@link #matchingOrderIntakeReplayedEntries}(이전 녹화 읽기) → ②
 * {@link #matchingOrderIntakeRecordingSubscriptionId}(새 녹화 시작, REMOTE) → ③
 * {@link #matchingOrderSubscription}(라이브 구독) 순서로 만들어지도록 각자 앞 단계 빈을 파라미터로
 * 받아 의존시킨다 — 새 구독자는 붙은 시점 이후 데이터만 보므로, 녹화가 라이브 구독보다 늦게
 * 시작되면 그 사이 반영된 주문이 녹화엔 없는 유실 창이 생긴다.</p>
 *
 * <p>빈 생성 순서(ArchivingMediaDriver→Aeron→Subscription)가 그대로 소멸 순서의 역방향이 되도록
 * {@code destroyMethod}만 지정한다 — Spring이 빈 의존 그래프를 보고 Subscription을 Aeron보다,
 * Aeron을 ArchivingMediaDriver보다 먼저 닫아준다.</p>
 */
@Configuration
public class MatchingOrderIntakeConfig {

    // 패키지 가시성 — 테스트(같은 패키지)가 프로덕션과 같은 채널로 발행하도록 이 기본값을 그대로
    // 참조한다. 실제 채널은 transport.matching-intake.channel 속성에서 해석된다(fork1 Unit 1).
    // 스트림 ID는 core AeronStreamIds.MATCHING_INTAKE로 공유한다.
    static final String DEFAULT_INTAKE_CHANNEL = "aeron:ipc";

    // Archive 제어 채널 — matching.worker.archive.control-channel 속성에서 해석된다(fork1 Unit 1
    // — 3-JVM 실행 시 account-worker와 포트가 겹치지 않게 워커별로 다른 값을 준다). 기본값은
    // account-worker AccountOrderIntakeConfig와 같은 관례값이다.
    private static final String DEFAULT_CONTROL_REQUEST_CHANNEL = "aeron:udp?endpoint=localhost:8010";
    private static final String CONTROL_RESPONSE_CHANNEL = "aeron:udp?endpoint=localhost:0";
    private static final String REPLICATION_CHANNEL = "aeron:udp?endpoint=localhost:0";

    /**
     * @param archiveDirConfig {@code matching.worker.archive-dir} — 저널 녹화(2c-1)·스냅샷
     *                         파일(2d-1b, {@link MatchingSnapshotConfig})을 같이 담는 durable
     *                         디렉터리. 비워두면(기본, 테스트) 매 기동마다 새 임시 디렉터리를
     *                         쓴다(격리). 값을 주면(운영, K8s PV 마운트 지점처럼 재시작 사이
     *                         살아남는 고정 경로) 그 경로를 그대로 쓴다.
     */
    @Bean
    public File matchingArchiveDir(@Value("${matching.worker.archive-dir:}") String archiveDirConfig) throws IOException {
        if (archiveDirConfig == null || archiveDirConfig.isBlank()) {
            return Files.createTempDirectory("matching-worker-archive-").toFile();
        }
        File archiveDir = new File(archiveDirConfig);
        Files.createDirectories(archiveDir.toPath());
        return archiveDir;
    }

    @Bean(destroyMethod = "close")
    public ArchivingMediaDriver archivingMediaDriver(
            File matchingArchiveDir,
            @Value("${matching.worker.archive.control-channel:" + DEFAULT_CONTROL_REQUEST_CHANNEL + "}") String controlRequestChannel) {
        String aeronDirectoryName = CommonContext.generateRandomDirName();

        return ArchivingMediaDriver.launch(
            new MediaDriver.Context().aeronDirectoryName(aeronDirectoryName)
                // Aeron 드라이버 통신용 임시 디렉터리(aeron-*) — Archive 저널·스냅샷 디렉터리와는
                // 다른 자리다(그쪽은 archiveDir, 재시작 넘어 보존해야 해 절대 안 건드린다). 이
                // 드라이버 디렉터리는 프로세스가 죽으면 쓸모없는데 기본값이 안 지워 무한히 쌓인다(I10).
                .dirDeleteOnStart(true)
                .dirDeleteOnShutdown(true),
            new Archive.Context()
                .controlChannel(controlRequestChannel)
                .replicationChannel(REPLICATION_CHANNEL)
                .deleteArchiveOnStart(false) // 저널은 재시작 넘어 보존해야 한다(2c-1)
                .archiveDir(matchingArchiveDir)
        );
    }

    @Bean(destroyMethod = "close")
    public Aeron aeron(ArchivingMediaDriver archivingMediaDriver) {
        return Aeron.connect(new Aeron.Context()
            .aeronDirectoryName(archivingMediaDriver.mediaDriver().aeronDirectoryName()));
    }

    @Bean(destroyMethod = "close")
    public AeronArchive aeronArchive(
            Aeron aeron,
            @Value("${matching.worker.archive.control-channel:" + DEFAULT_CONTROL_REQUEST_CHANNEL + "}") String controlRequestChannel) {
        return AeronArchive.connect(new AeronArchive.Context()
            .aeron(aeron)
            .ownsAeronClient(false) // Aeron 빈은 우리가 별도로 소유·소멸시킨다(위 aeron() 빈)
            .controlRequestChannel(controlRequestChannel)
            .controlResponseChannel(CONTROL_RESPONSE_CHANNEL));
    }

    /**
     * 매칭이 아직 한 번도 뜬 적이 없으면(카탈로그에 recording이 하나도 없음) 빈 리스트를 돌려준다
     * — 예외가 아니라 복구할 gap 자체가 없는 정상 상태다(account
     * {@code AccountFillReplayer.readFrom}과 같은 결, I2 U2b).
     *
     * <p>스냅샷이 있으면(2d-1b) 그 시점의 발행자(계좌 샤드)별 위치({@link
     * StoredMatchingSnapshot#orderIntakePosition()})부터 recording마다 읽는다 — 스냅샷이 없으면
     * (하위호환, 첫 기동) 모든 recording을 시작 위치부터 읽는다({@link MatchingOrderIntakeReplayer}가
     * 빈 맵을 그렇게 해석한다).</p>
     */
    @Bean
    public List<JournaledOrder> matchingOrderIntakeReplayedEntries(
            AeronArchive aeronArchive,
            Optional<StoredMatchingSnapshot> matchingLoadedSnapshot,
            @Value("${transport.matching-intake.channel:" + DEFAULT_INTAKE_CHANNEL + "}") String intakeChannel) {
        Map<Integer, Long> orderIntakePositions = matchingLoadedSnapshot
            .map(StoredMatchingSnapshot::orderIntakePosition)
            .orElse(Map.of());
        MatchingOrderIntakeReplayer replayer = new MatchingOrderIntakeReplayer(aeronArchive);
        return replayer.readFrom(intakeChannel, AeronStreamIds.MATCHING_INTAKE, orderIntakePositions);
    }

    /**
     * 주문 인테이크 스트림에 새 녹화를 시작한다(REMOTE, 클래스 javadoc "주문 인테이크도 REMOTE
     * 녹화한다" 참고). 반환값(Archive 구독 ID)은 안 쓴다 — 이 빈이 존재하는 이유는
     * {@link #matchingOrderSubscription}이 이 빈에 의존하게 만들어 생성 순서를 강제하는 것뿐이다.
     * {@code matchingOrderIntakeReplayedEntries}를 파라미터로 받는 이유도 같다 — 이전 녹화 리플레이가
     * 끝난 뒤에야 새 녹화를 시작해야 한다(account {@code AccountFillIntakeConfig}와 같은 이유).
     */
    @Bean
    public Long matchingOrderIntakeRecordingSubscriptionId(
            AeronArchive aeronArchive,
            List<JournaledOrder> matchingOrderIntakeReplayedEntries,
            @Value("${transport.matching-intake.channel:" + DEFAULT_INTAKE_CHANNEL + "}") String intakeChannel) {
        return aeronArchive.startRecording(intakeChannel, AeronStreamIds.MATCHING_INTAKE, SourceLocation.REMOTE);
    }

    @Bean(destroyMethod = "close")
    public Subscription matchingOrderSubscription(
            Aeron aeron,
            Long matchingOrderIntakeRecordingSubscriptionId,
            @Value("${transport.matching-intake.channel:" + DEFAULT_INTAKE_CHANNEL + "}") String intakeChannel) {
        return aeron.addSubscription(intakeChannel, AeronStreamIds.MATCHING_INTAKE);
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
