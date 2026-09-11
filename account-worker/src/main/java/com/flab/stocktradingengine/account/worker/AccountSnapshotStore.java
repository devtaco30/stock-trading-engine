package com.flab.stocktradingengine.account.worker;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.Optional;

import com.flab.stocktradingengine.account.disruptor.AccountSnapshot;
import com.flab.stocktradingengine.account.disruptor.AccountSnapshotCodec;

/**
 * 계좌 엔진 스냅샷(2d-2)을 archive-dir(저널과 같은 durable 디스크)에 파일로 저장·조회한다.
 * Aeron Archive는 건드리지 않는 순수 파일 I/O — recordingId 해석(현재 저널 녹화가 무엇인지)은
 * 이 클래스의 책임이 아니라 호출부({@link AccountSnapshotLifecycle})가 넘겨준다. matching
 * {@code MatchingSnapshotStore}와 같은 결.
 *
 * <h3>원자적 쓰기</h3>
 * <p>임시 파일에 다 쓴 뒤 최종 이름으로 rename한다({@link StandardCopyOption#ATOMIC_MOVE}) — 쓰는
 * 도중 프로세스가 죽어도 절반만 쓰인 파일이 최종 이름으로 남는 일이 없다. 최신 스냅샷 하나만
 * 유지한다(이전 스냅샷은 rename이 덮어쓴다).</p>
 *
 * <h3>파일 레이아웃</h3>
 * <pre>[recordingId:8][AccountSnapshotCodec가 인코딩한 스냅샷 바이트...]</pre>
 */
class AccountSnapshotStore {

    private static final String FILE_NAME = "account-snapshot.dat";
    private static final String TEMP_FILE_NAME = "account-snapshot.dat.tmp";

    private final File file;
    private final File tempFile;
    private final AccountSnapshotCodec codec = new AccountSnapshotCodec();

    AccountSnapshotStore(File archiveDir) {
        this.file = new File(archiveDir, FILE_NAME);
        this.tempFile = new File(archiveDir, TEMP_FILE_NAME);
    }

    void write(long recordingId, AccountSnapshot snapshot) {
        byte[] snapshotBytes = codec.encode(snapshot);
        ByteBuffer payload = ByteBuffer.allocate(Long.BYTES + snapshotBytes.length);
        payload.putLong(recordingId);
        payload.put(snapshotBytes);

        try {
            Path tempPath = tempFile.toPath();
            Files.write(tempPath, payload.array());
            Files.move(tempPath, file.toPath(),
                StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            throw new UncheckedIOException("계좌 스냅샷 파일 쓰기 실패: " + file, e);
        }
    }

    Optional<StoredAccountSnapshot> read() {
        if (!file.exists()) {
            return Optional.empty();
        }
        try {
            byte[] payload = Files.readAllBytes(file.toPath());
            ByteBuffer buffer = ByteBuffer.wrap(payload);
            long recordingId = buffer.getLong();
            byte[] snapshotBytes = Arrays.copyOfRange(payload, Long.BYTES, payload.length);
            AccountSnapshot snapshot = codec.decode(snapshotBytes);
            return Optional.of(new StoredAccountSnapshot(recordingId, snapshot));
        } catch (IOException e) {
            throw new UncheckedIOException("계좌 스냅샷 파일 읽기 실패: " + file, e);
        }
    }
}
