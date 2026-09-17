package com.flab.stocktradingengine.account.worker.recovery;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** U3-b — 저장한 (recordingId, position)이 그대로 왕복하는지, 파일이 없으면 빈 값인지 검증한다. */
class OrderResultForwardPositionStoreTest {

    @TempDir
    private File archiveDir;

    @Test
    void 저장한_값이_그대로_왕복한다() {
        OrderResultForwardPositionStore store = new OrderResultForwardPositionStore(archiveDir);

        store.write(7L, 12345L);
        Optional<StoredOrderResultForwardPosition> read = store.read();

        assertTrue(read.isPresent());
        assertEquals(7L, read.get().recordingId());
        assertEquals(12345L, read.get().position());
    }

    @Test
    void 다시_쓰면_이전_값을_덮어쓴다() {
        OrderResultForwardPositionStore store = new OrderResultForwardPositionStore(archiveDir);

        store.write(1L, 100L);
        store.write(2L, 200L);
        Optional<StoredOrderResultForwardPosition> read = store.read();

        assertEquals(2L, read.get().recordingId());
        assertEquals(200L, read.get().position());
    }

    @Test
    void 파일이_없으면_빈_값을_돌려준다() {
        OrderResultForwardPositionStore store = new OrderResultForwardPositionStore(archiveDir);

        Optional<StoredOrderResultForwardPosition> read = store.read();

        assertTrue(read.isEmpty());
    }
}
