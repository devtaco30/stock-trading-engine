package com.flab.stocktradingengine.account.worker;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.flab.stocktradingengine.account.disruptor.snapshot.AccountSnapshot;
import com.flab.stocktradingengine.account.disruptor.snapshot.AccountStateSnapshot;
import com.flab.stocktradingengine.account.worker.lifecycle.AccountSnapshotLifecycle;
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
            Map.of(), Map.of(), Map.of("005930", 5), Set.of(), Set.of(),
            Map.of("r1", 10L), BigDecimal.ZERO);
        AccountSnapshot snapshot = new AccountSnapshot(Map.of(1L, account), 3L, 555L);

        AccountSnapshotStore store = store();
        store.write(42L, 777L, snapshot);

        Optional<StoredAccountSnapshot> found = store.read();
        assertThat(found).isPresent();
        assertThat(found.get().recordingId()).isEqualTo(42L);
        assertThat(found.get().fillConsumedPosition()).isEqualTo(777L);
        assertThat(found.get().snapshot()).isEqualTo(snapshot);
    }

    @Test
    void write를_두_번_하면_최신_한_개만_남는다() {
        AccountSnapshotStore store = store();
        store.write(1L, 10L, new AccountSnapshot(Map.of(), 0L, 100L));
        store.write(2L, 20L, new AccountSnapshot(Map.of(), 0L, 200L));

        Optional<StoredAccountSnapshot> found = store.read();
        assertThat(found).isPresent();
        assertThat(found.get().recordingId()).isEqualTo(2L);
        assertThat(found.get().fillConsumedPosition()).isEqualTo(20L);
        assertThat(found.get().snapshot().journalPosition()).isEqualTo(200L);

        File[] filesInDir = archiveDir.toFile().listFiles((dir, name) -> name.contains("account-snapshot"));
        assertThat(filesInDir).as("임시 파일이 안 남고 최종 파일 하나만 남아야 한다").hasSize(1);
    }
}
