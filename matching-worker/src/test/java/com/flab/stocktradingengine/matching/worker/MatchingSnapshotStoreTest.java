package com.flab.stocktradingengine.matching.worker;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.flab.stocktradingengine.matching.disruptor.snapshot.BookSnapshot;
import com.flab.stocktradingengine.matching.disruptor.snapshot.MatchingSnapshot;
import com.flab.stocktradingengine.matching.disruptor.snapshot.RestingOrder;
import com.flab.stocktradingengine.matching.worker.lifecycle.MatchingEngineLifecycle;
import com.flab.stocktradingengine.matching.worker.recovery.MatchingSnapshotStore;
import com.flab.stocktradingengine.matching.worker.recovery.StoredMatchingSnapshot;
import com.flab.stocktradingengine.trading.entity.OrderSide;

/**
 * 2d-1b — {@link MatchingSnapshotStore}(archive-dir에 스냅샷 파일을 원자적으로 쓰고 읽는 순수
 * 파일 I/O) 단위 검증. Aeron Archive는 안 건드린다 — recordingId는 이미 정해진 값을 그대로
 * 받아서 저장·복원만 한다(해석은 {@link MatchingEngineLifecycle} 책임).
 */
class MatchingSnapshotStoreTest {

    @TempDir
    private Path archiveDir;

    private MatchingSnapshotStore store() {
        return new MatchingSnapshotStore(archiveDir.toFile());
    }

    @Test
    void 파일이_없으면_read는_빈_Optional() {
        assertThat(store().read()).isEmpty();
    }

    @Test
    void write_후_read하면_recordingId와_스냅샷이_그대로_돌아온다() {
        RestingOrder order = new RestingOrder(1L, 100L, OrderSide.BUY, new BigDecimal("70000"), 10, 1L, 0, false);
        BookSnapshot book = new BookSnapshot(List.of(order), Map.of(9001L, 2L));
        MatchingSnapshot snapshot = new MatchingSnapshot(Map.of("005930", book), 555L);

        MatchingSnapshotStore store = store();
        store.write(42L, snapshot);

        Optional<StoredMatchingSnapshot> found = store.read();
        assertThat(found).isPresent();
        assertThat(found.get().recordingId()).isEqualTo(42L);
        assertThat(found.get().snapshot()).isEqualTo(snapshot);
    }

    @Test
    void write를_두_번_하면_최신_한_개만_남는다() {
        MatchingSnapshotStore store = store();
        store.write(1L, new MatchingSnapshot(Map.of(), 100L));
        store.write(2L, new MatchingSnapshot(Map.of(), 200L));

        Optional<StoredMatchingSnapshot> found = store.read();
        assertThat(found).isPresent();
        assertThat(found.get().recordingId()).isEqualTo(2L);
        assertThat(found.get().snapshot().journalPosition()).isEqualTo(200L);

        File[] filesInDir = archiveDir.toFile().listFiles((dir, name) -> name.contains("matching-snapshot"));
        assertThat(filesInDir).as("임시 파일이 안 남고 최종 파일 하나만 남아야 한다").hasSize(1);
    }
}
