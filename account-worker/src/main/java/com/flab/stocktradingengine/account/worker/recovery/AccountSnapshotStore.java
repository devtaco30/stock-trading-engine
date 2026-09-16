package com.flab.stocktradingengine.account.worker.recovery;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import com.flab.stocktradingengine.account.disruptor.snapshot.AccountSnapshot;
import com.flab.stocktradingengine.account.disruptor.snapshot.AccountSnapshotCodec;
import com.flab.stocktradingengine.account.worker.lifecycle.AccountSnapshotLifecycle;

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
 * <h3>파일 레이아웃 (버전 2, ADR-032 I1 D2)</h3>
 * <pre>
 * [version:1 = 2][journalRecordingId:8][fillSourceCount:4]
 *   [(sessionId:4)(position:8)] × fillSourceCount
 *   [AccountSnapshotCodec가 인코딩한 스냅샷 바이트...]
 * </pre>
 * <p>버전 1(발행자별 체결 위치 맵이 없던 시절, {@code [recordingId:8][fillConsumedPosition:8]...})
 * 파일을 읽으면 첫 바이트가 2와 다르게 나와(옛 recordingId의 최상위 바이트) {@link
 * AccountSnapshotFormatException}이 난다 — 의도한 동작이다, 클래스 javadoc·그 예외 클래스
 * javadoc 참고.</p>
 */
public class AccountSnapshotStore {

    private static final String FILE_NAME = "account-snapshot.dat";
    private static final String TEMP_FILE_NAME = "account-snapshot.dat.tmp";
    private static final byte FORMAT_VERSION = 2;

    private final File file;
    private final File tempFile;
    private final AccountSnapshotCodec codec = new AccountSnapshotCodec();

    public AccountSnapshotStore(File archiveDir) {
        this.file = new File(archiveDir, FILE_NAME);
        this.tempFile = new File(archiveDir, TEMP_FILE_NAME);
    }

    public void write(long recordingId, Map<Integer, Long> fillPositions, AccountSnapshot snapshot) {
        write(recordingId, fillPositions, codec.encode(snapshot));
    }

    /**
     * 이미 인코딩된 스냅샷 바이트를 그대로 쓴다(1-3) — 러닝 중 스냅샷 쓰기 스레드는 소비자
     * 스레드가 이미 직렬화한 바이트를 받으므로 재인코딩하지 않는다({@code AccountSnapshotSink}
     * 계약과 맞물림).
     *
     * <h3>fsync (1-3)</h3>
     * <p>{@link FileChannel#force(boolean)}로 디스크에 실제로 박힌 뒤에야 이 메서드가 반환한다.
     * durable-before-prune(tradeId 세대 가지치기)의 전제가 "durable = OS 캐시가 아니라 디스크에
     * 실제로 쓰임"이라, rename만으로는 부족하다 — rename 자체는 원자적이지만 그 내용이 아직
     * 페이지 캐시에만 있을 수 있다.</p>
     */
    public void write(long recordingId, Map<Integer, Long> fillPositions, byte[] snapshotBytes) {
        int fillSourceCount = fillPositions.size();
        int fillSourcesBytes = fillSourceCount * (Integer.BYTES + Long.BYTES);
        ByteBuffer payload = ByteBuffer.allocate(
            Byte.BYTES + Long.BYTES + Integer.BYTES + fillSourcesBytes + snapshotBytes.length);
        payload.put(FORMAT_VERSION);
        payload.putLong(recordingId);
        payload.putInt(fillSourceCount);
        fillPositions.forEach((sessionId, position) -> {
            payload.putInt(sessionId);
            payload.putLong(position);
        });
        payload.put(snapshotBytes);
        payload.flip();

        try {
            Path tempPath = tempFile.toPath();
            try (FileChannel channel = FileChannel.open(tempPath,
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
                while (payload.hasRemaining()) {
                    channel.write(payload);
                }
                channel.force(true);
            }
            Files.move(tempPath, file.toPath(),
                StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            throw new UncheckedIOException("계좌 스냅샷 파일 쓰기 실패: " + file, e);
        }
    }

    public Optional<StoredAccountSnapshot> read() {
        if (!file.exists()) {
            return Optional.empty();
        }
        try {
            byte[] payload = Files.readAllBytes(file.toPath());
            ByteBuffer buffer = ByteBuffer.wrap(payload);
            byte version = buffer.get();
            if (version != FORMAT_VERSION) {
                throw new AccountSnapshotFormatException(file, version, FORMAT_VERSION);
            }
            long recordingId = buffer.getLong();
            int fillSourceCount = buffer.getInt();
            Map<Integer, Long> fillPositions = new LinkedHashMap<>();
            for (int i = 0; i < fillSourceCount; i++) {
                int sessionId = buffer.getInt();
                long position = buffer.getLong();
                fillPositions.put(sessionId, position);
            }
            int headerBytes = Byte.BYTES + Long.BYTES + Integer.BYTES + fillSourceCount * (Integer.BYTES + Long.BYTES);
            byte[] snapshotBytes = Arrays.copyOfRange(payload, headerBytes, payload.length);
            AccountSnapshot snapshot = codec.decode(snapshotBytes);
            return Optional.of(new StoredAccountSnapshot(recordingId, fillPositions, snapshot));
        } catch (IOException e) {
            throw new UncheckedIOException("계좌 스냅샷 파일 읽기 실패: " + file, e);
        }
    }
}
