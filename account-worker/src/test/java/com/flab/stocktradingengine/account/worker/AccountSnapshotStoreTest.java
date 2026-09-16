package com.flab.stocktradingengine.account.worker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.RandomAccessFile;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.flab.stocktradingengine.account.disruptor.snapshot.AccountSnapshot;
import com.flab.stocktradingengine.account.disruptor.snapshot.AccountStateSnapshot;
import com.flab.stocktradingengine.account.disruptor.snapshot.TradeIdGenerationSnapshot;
import com.flab.stocktradingengine.account.worker.lifecycle.AccountSnapshotLifecycle;
import com.flab.stocktradingengine.account.worker.recovery.AccountSnapshotFormatException;
import com.flab.stocktradingengine.account.worker.recovery.AccountSnapshotStore;
import com.flab.stocktradingengine.account.worker.recovery.StoredAccountSnapshot;

/**
 * 2d-2b — {@link AccountSnapshotStore}(archive-dir에 스냅샷 파일을 원자적으로 쓰고 읽는 순수
 * 파일 I/O) 단위 검증. Aeron Archive는 안 건드린다 — recordingId는 이미 정해진 값을 그대로
 * 받아서 저장·복원만 한다(해석은 {@link AccountSnapshotLifecycle} 책임). matching
 * {@code MatchingSnapshotStoreTest}와 같은 결.
 */
class AccountSnapshotStoreTest {

    @TempDir
    private Path archiveDir;

    private AccountSnapshotStore store() {
        return new AccountSnapshotStore(archiveDir.toFile());
    }

    @Test
    void 파일이_없으면_read는_빈_Optional() {
        assertThat(store().read()).isEmpty();
    }

    @Test
    void write_후_read하면_recordingId와_fillConsumedPosition과_스냅샷이_그대로_돌아온다() {
        AccountStateSnapshot account = new AccountStateSnapshot(
            1L, 3L, new BigDecimal("1000000"), new BigDecimal("0.40"),
            Map.of(), Map.of(), Map.of("005930", 5),
            List.of(new TradeIdGenerationSnapshot(0L, Set.of())), Set.of(),
            Set.of("r1"), BigDecimal.ZERO);
        AccountSnapshot snapshot = new AccountSnapshot(Map.of(1L, account), 3L, 555L);

        AccountSnapshotStore store = store();
        store.write(42L, Map.of(0, 777L), snapshot);

        Optional<StoredAccountSnapshot> found = store.read();
        assertThat(found).isPresent();
        assertThat(found.get().recordingId()).isEqualTo(42L);
        assertThat(found.get().fillConsumedPosition()).isEqualTo(Map.of(0, 777L));
        assertThat(found.get().snapshot()).isEqualTo(snapshot);
    }

    @Test
    void write_후_read하면_발행자_여럿의_위치_맵이_그대로_왕복한다() {
        AccountSnapshotStore store = store();
        Map<Integer, Long> fillPositions = Map.of(0, 100L, 1, 250L, 2, 999L);
        store.write(42L, fillPositions, new AccountSnapshot(Map.of(), 0L, 0L));

        Optional<StoredAccountSnapshot> found = store.read();
        assertThat(found).isPresent();
        assertThat(found.get().fillConsumedPosition()).isEqualTo(fillPositions);
    }

    @Test
    void 체결_위치가_아직_없으면_빈_맵으로_왕복한다() {
        AccountSnapshotStore store = store();
        store.write(42L, Map.of(), new AccountSnapshot(Map.of(), 0L, 0L));

        Optional<StoredAccountSnapshot> found = store.read();
        assertThat(found).isPresent();
        assertThat(found.get().fillConsumedPosition()).isEmpty();
    }

    @Test
    void write를_두_번_하면_최신_한_개만_남는다() {
        AccountSnapshotStore store = store();
        store.write(1L, Map.of(0, 10L), new AccountSnapshot(Map.of(), 0L, 100L));
        store.write(2L, Map.of(0, 20L), new AccountSnapshot(Map.of(), 0L, 200L));

        Optional<StoredAccountSnapshot> found = store.read();
        assertThat(found).isPresent();
        assertThat(found.get().recordingId()).isEqualTo(2L);
        assertThat(found.get().fillConsumedPosition()).isEqualTo(Map.of(0, 20L));
        assertThat(found.get().snapshot().journalPosition()).isEqualTo(200L);

        File[] filesInDir = archiveDir.toFile().listFiles((dir, name) -> name.contains("account-snapshot"));
        assertThat(filesInDir).as("임시 파일이 안 남고 최종 파일 하나만 남아야 한다").hasSize(1);
    }

    @Test
    void 형식_번호가_다른_파일을_읽으면_예외가_나고_메시지에_경로와_버전이_담긴다() throws Exception {
        AccountSnapshotStore store = store();
        store.write(1L, Map.of(0, 10L), new AccountSnapshot(Map.of(), 0L, 100L));

        File snapshotFile = new File(archiveDir.toFile(), "account-snapshot.dat");
        try (RandomAccessFile raf = new RandomAccessFile(snapshotFile, "rw")) {
            raf.seek(0);
            raf.writeByte(9); // 형식 번호(첫 바이트)를 존재하지 않는 값으로 덮어쓴다
        }

        assertThatThrownBy(store::read)
            .isInstanceOf(AccountSnapshotFormatException.class)
            .hasMessageContaining(snapshotFile.getPath())
            .hasMessageContaining("읽은 버전=9")
            .hasMessageContaining("기대 버전=2");
    }
}
