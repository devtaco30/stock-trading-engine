package com.flab.stocktradingengine.account.worker.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.math.BigDecimal;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

import com.flab.stocktradingengine.account.disruptor.domain.AccountState;
import com.flab.stocktradingengine.account.disruptor.engine.AccountEngine;
import com.flab.stocktradingengine.account.worker.AccountWorkerApplication;
import com.flab.stocktradingengine.account.worker.recovery.AccountSnapshotWriter;
import com.flab.stocktradingengine.account.worker.support.NoOpMatchingOrderSenderTestConfig;

/**
 * ADR-032 I5 U3 — 저널 Archive 세그먼트가 실제로 회수된(purge된) 뒤에도 재기동 복구가 정상
 * 동작하는지 확인한다(LLD {@code _i5_lld.md} 5절 U3). U1·U2는 "회수가 실제로 지우는지"를
 * 검증했다 — 이 테스트는 반대 방향, "회수가 필요한 구간까지 지워버리는 버그가 있으면 여기서
 * 깨진다"를 본다({@code AccountFillMultiSourceReplayRecoveryIntegrationTest}가 가장 가까운
 * 형태라 그 기동 패턴을 그대로 쓴다).
 *
 * <h3>왜 세그먼트·term 길이를 줄이나</h3>
 * <p>기본 세그먼트(128MB)로는 테스트 데이터로 세그먼트 경계를 못 넘어 회수를 불러도 지울 게
 * 없다 — "회수 뒤에도 복구된다"가 아니라 "아무것도 안 지워진 채 복구된다"를 증명하는 거짓
 * green이 난다(U1·U2에서 먼저 겪음). {@code AccountOrderIntakeConfig.archivingMediaDriver}에
 * 추가한 두 property(segment-file-length·ipc-term-buffer-length, U3 전용, 비우면 운영값 그대로)로
 * 둘 다 64KB로 줄인다 — Archive가 실제 세그먼트 길이를 {@code max(설정값, termBufferLength)}로
 * 강제하므로 둘 다 줄여야 세그먼트가 실제로 작아진다.
 *
 * <h3>어떻게 회수를 두 번 이상 트리거하나</h3>
 * <p>스냅샷 주기 N({@code AccountEventHandler.SNAPSHOT_INTERVAL_JOURNAL_ENTRIES}=10,000)은
 * 상수라 테스트에서 못 줄인다 — {@code AccountEngineSnapshotTriggerTest}(1-3)가 쓴 것과 같은
 * 트릭으로 같은 requestId를 N×2번 재전송한다. 첫 건만 accept, 나머지는 빠른 dup 경로를 타지만
 * journal-before-apply라 저널엔 매번 기록되고 handler의 appliedSeq도 매번 오른다 — 2×N건이면
 * 스냅샷이 두 번 찍히고(N건·2N건), 두 번째 스냅샷 때 첫 번째 스냅샷 경계까지 회수가 실행된다
 * (D1 "한 세대 여유").</p>
 *
 * <h3>⚠️함정 — {@code AccountState.seq()}로 dedup 진행을 재면 안 된다</h3>
 * <p>{@code AccountState.seq()}는 그 계좌에 실제로 상태 변경이 일어날 때만(tryReserve 등, dup
 * 요청은 저널 기록 이전에 걸러져 이 메서드 자체를 안 탄다) 오른다 — dup 재전송 N건을 다 처리해도
 * 계속 1에 머문다(직접 겪음, 30초 타임아웃으로 막혔다가 로그로 "요청 재전송 무시"가 정확히
 * N번 찍힌 걸 보고서야 처리 자체는 되고 있고 관찰 신호가 틀렸다는 걸 확인했다 — 세그먼트·term
 * 길이를 의심해 1MB까지 올려봐도 재현돼 그쪽이 원인이 아님도 같이 확인). 대신 {@link
 * AccountSnapshotWriter#durableSeq()}(handler의 전역 appliedSeq를 반영, N건마다 오름)를
 * 폴링해야 한다.</p>
 */
class AccountArchivePurgeRecoveryIntegrationTest {

    private static final String STOCK = "005930";
    private static final long TIMEOUT_NANOS = 30_000_000_000L;
    private static final long SNAPSHOT_INTERVAL = 10_000L;
    private static final int SEGMENT_AND_TERM_LENGTH = 64 * 1024; // Aeron 허용 최소 term 길이

    private static final BigDecimal EXPECTED_RESERVED_MARGIN = new BigDecimal("40000"); // 10주×10,000×0.40

    @TempDir
    private Path archiveDir;

    @Test
    void 저널_회수가_실제로_일어난_뒤에도_재기동하면_상태가_그대로_복원된다() {
        long journalRecordingId;
        int filesAfterFirstSnapshot;
        int filesAfterSecondSnapshot;

        ConfigurableApplicationContext run1 = launch();
        try {
            AccountEngine engine = run1.getBean(AccountEngine.class);
            AccountSnapshotWriter writer = run1.getBean(AccountSnapshotWriter.class);
            journalRecordingId = run1.getBean("accountJournalRecordingId", Long.class);

            engine.publishBuy(1L, STOCK, new BigDecimal("10000"), 10, "r1");
            awaitSeq(engine, 1L);
            assertThat(engine.accountState(1L).reservedMargin().compareTo(EXPECTED_RESERVED_MARGIN))
                .as("최초 매수 접수로 예약증거금이 반영돼야 한다").isZero();

            // 같은 requestId를 N번 재전송 — 첫 스냅샷 트리거(회수는 직전 경계가 없어 아직 안 지움).
            // durableSeq를 본다 — AccountState.seq()는 dedup 요청엔 안 오른다(클래스 javadoc 함정 참고).
            fillDummyRequests(engine, SNAPSHOT_INTERVAL);
            awaitDurableSeqAtLeast(writer, SNAPSHOT_INTERVAL);
            filesAfterFirstSnapshot = countSegmentFiles(journalRecordingId);
            assertThat(filesAfterFirstSnapshot).as("N건 분량 저널이 실제로 세그먼트 여러 개를 채워야 한다").isGreaterThanOrEqualTo(3);

            // 두 번째 N건 — 두 번째 스냅샷 트리거, 이번엔 첫 번째 스냅샷 경계까지 회수가 실행된다.
            fillDummyRequests(engine, SNAPSHOT_INTERVAL);
            awaitDurableSeqAtLeast(writer, 2 * SNAPSHOT_INTERVAL);
            filesAfterSecondSnapshot = awaitSegmentFilesBelow(journalRecordingId, 2 * filesAfterFirstSnapshot);

            assertThat(filesAfterSecondSnapshot)
                .as("회수가 실제로 일어났다면, 두 배 분량을 발행했어도 세그먼트 파일 수가 두 배까지 늘지 않아야 한다"
                    + "(회수 없이 그대로 쌓였다면 filesAfterFirstSnapshot의 대략 두 배가 나온다)")
                .isLessThan(2 * filesAfterFirstSnapshot);
        } finally {
            run1.close(); // graceful stop — 마지막 상태를 스냅샷으로 확정 짓는다
        }

        ConfigurableApplicationContext run2 = launch();
        try {
            AccountEngine engine = run2.getBean(AccountEngine.class);
            AccountState state = engine.accountState(1L);

            assertThat(state.reservedMargin().compareTo(EXPECTED_RESERVED_MARGIN))
                .as("저널 앞부분이 회수(삭제)된 뒤에도, 최근 스냅샷 기준으로 예약증거금이 정확히 복원돼야 한다")
                .isZero();
        } finally {
            run2.close();
        }
    }

    private ConfigurableApplicationContext launch() {
        return new SpringApplicationBuilder(AccountWorkerApplication.class)
            .web(WebApplicationType.NONE)
            .sources(NoOpMatchingOrderSenderTestConfig.class)
            .properties(
                "account-worker.seed-accounts[0].account-id=1",
                "account-worker.seed-accounts[0].balance=1000000",
                "account-worker.seed-accounts[0].margin-rate=0.40",
                "account.worker.archive-dir=" + archiveDir.toAbsolutePath(),
                "account.worker.archive.segment-file-length=" + SEGMENT_AND_TERM_LENGTH,
                "account.worker.archive.ipc-term-buffer-length=" + SEGMENT_AND_TERM_LENGTH)
            .run();
    }

    /** 같은 requestId를 count번 재전송한다 — 첫 건만 accept, 나머지는 빠른 dup 경로지만 저널엔 매번 남는다. */
    private void fillDummyRequests(AccountEngine engine, long count) {
        for (long i = 0; i < count; i++) {
            engine.publishBuy(1L, STOCK, new BigDecimal("10000"), 10, "r1");
        }
    }

    /** 최초 buy 접수(계좌 상태가 실제로 바뀌는 이벤트) 반영을 기다린다. */
    private void awaitSeq(AccountEngine engine, long accountId) {
        long deadline = System.nanoTime() + TIMEOUT_NANOS;
        while (engine.accountState(accountId).seq() < 1L) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("30초 안에 최초 buy 접수가 반영되지 않음");
            }
            Thread.yield();
        }
    }

    /**
     * 러닝 중 스냅샷이 durable하게(fsync 완료) 기록될 때까지 기다린다. dedup되는 재전송 N건이
     * 다 처리돼 handler의 appliedSeq가 목표에 도달해야 스냅샷이 트리거되므로, 그 처리 자체가
     * 끝났는지 확인하는 신호로도 쓴다.
     */
    private void awaitDurableSeqAtLeast(AccountSnapshotWriter writer, long expectedAppliedSeq) {
        long deadline = System.nanoTime() + TIMEOUT_NANOS;
        while (writer.durableSeq() < expectedAppliedSeq) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("30초 안에 durableSeq가 " + expectedAppliedSeq + "에 도달하지 않음, 현재 durableSeq="
                    + writer.durableSeq());
            }
            Thread.yield();
        }
    }

    private int countSegmentFiles(long recordingId) {
        File dir = archiveDir.toFile();
        String[] files = dir.list((d, name) -> name.startsWith(recordingId + "-") && name.endsWith(".rec"));
        return files == null ? 0 : files.length;
    }

    /** 회수는 스냅샷 쓰기 스레드가 비동기로 실행하므로, 세그먼트 파일 수가 기준 미만으로 내려갈 때까지 기다린다. */
    private int awaitSegmentFilesBelow(long recordingId, int threshold) {
        long deadline = System.nanoTime() + TIMEOUT_NANOS;
        int files;
        while ((files = countSegmentFiles(recordingId)) >= threshold) {
            if (System.nanoTime() > deadline) {
                return files; // 타임아웃이어도 실측값을 그대로 돌려준다 — 이어지는 assertion이 드러낸다.
            }
            Thread.yield();
        }
        return files;
    }
}
