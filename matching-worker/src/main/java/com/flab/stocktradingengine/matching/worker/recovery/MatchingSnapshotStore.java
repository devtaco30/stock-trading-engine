package com.flab.stocktradingengine.matching.worker.recovery;

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

import com.flab.stocktradingengine.matching.disruptor.snapshot.MatchingSnapshot;
import com.flab.stocktradingengine.matching.disruptor.snapshot.MatchingSnapshotCodec;
import com.flab.stocktradingengine.matching.worker.lifecycle.MatchingEngineLifecycle;

/**
 * 매칭 엔진 스냅샷(2d-1)을 archive-dir(저널과 같은 durable 디스크)에 파일로 저장·조회한다.
 * Aeron Archive는 건드리지 않는 순수 파일 I/O — recordingId 해석(현재 저널 녹화가 무엇인지)은
 * 이 클래스의 책임이 아니라 호출부({@link MatchingEngineLifecycle})가 넘겨준다. account
 * {@code AccountSnapshotStore}와 같은 결.
 *
 * <h3>원자적 쓰기</h3>
 * <p>임시 파일에 다 쓴 뒤 최종 이름으로 rename한다({@link StandardCopyOption#ATOMIC_MOVE}) — 쓰는
 * 도중 프로세스가 죽어도 절반만 쓰인 파일이 최종 이름으로 남는 일이 없다. 최신 스냅샷 하나만
 * 유지한다(이전 스냅샷은 rename이 덮어쓴다).</p>
 *
 * <h3>fsync (I6 U2)</h3>
 * <p>{@link FileChannel#force(boolean)}로 디스크에 실제로 박힌 뒤에야 {@link #write} 가 반환한다
 * (account-worker {@code AccountSnapshotStore}와 같은 이유). rename 자체는 원자적이지만 그 내용이
 * 아직 페이지 캐시에만 있을 수 있어 rename만으로는 "디스크에 확정됐다"는 보고가 거짓일 수 있다 —
 * 이 보고를 나중에 매칭 쪽 디스크 회수(저널 recording 정리)가 경계로 믿고 쓰게 되므로(I6 D3, LLD
 * §4) 여기서 실제로 디스크까지 박아둔다. 정상 종료 경로({@link MatchingSnapshotLifecycle}의 stop)도
 * 이 메서드를 그대로 쓰므로 종료가 그만큼(디스크에 박힐 때까지) 느려지는 게 의도한 대가다.</p>
 *
 * <h3>파일 레이아웃 (버전 1, I2 U1)</h3>
 * <pre>
 * [version:1 = 1][recordingId:8][orderIntakeSourceCount:4]
 *   [(sessionId:4)(position:8)] × orderIntakeSourceCount
 *   [MatchingSnapshotCodec가 인코딩한 스냅샷 바이트...]
 * </pre>
 * <p>이전(무버전, I6까지의 {@code [recordingId:8][스냅샷 바이트...]}) 파일을 읽으면 첫 바이트가
 * 1과 다르게 나와(옛 recordingId의 최상위 바이트) {@link MatchingSnapshotFormatException}이 난다 —
 * 의도한 동작이다, 클래스 javadoc·그 예외 클래스 javadoc 참고.</p>
 */
public class MatchingSnapshotStore {

    private static final String FILE_NAME = "matching-snapshot.dat";
    private static final String TEMP_FILE_NAME = "matching-snapshot.dat.tmp";
    private static final byte FORMAT_VERSION = 1;

    private final File file;
    private final File tempFile;
    private final MatchingSnapshotCodec codec = new MatchingSnapshotCodec();

    public MatchingSnapshotStore(File archiveDir) {
        this.file = new File(archiveDir, FILE_NAME);
        this.tempFile = new File(archiveDir, TEMP_FILE_NAME);
    }

    public void write(long recordingId, Map<Integer, Long> orderIntakePositions, MatchingSnapshot snapshot) {
        write(recordingId, orderIntakePositions, codec.encode(snapshot));
    }

    /**
     * 이미 인코딩된 스냅샷 바이트를 그대로 쓴다(I6 U2) — 러닝 중 스냅샷 쓰기 스레드는 소비자
     * 스레드가 이미 직렬화한 바이트를 받으므로 재인코딩하지 않는다({@code MatchingSnapshotSink}
     * 계약과 맞물림, account-worker {@code AccountSnapshotStore}와 같은 결).
     */
    public void write(long recordingId, Map<Integer, Long> orderIntakePositions, byte[] snapshotBytes) {
        int sourceCount = orderIntakePositions.size();
        int sourcesBytes = sourceCount * (Integer.BYTES + Long.BYTES);
        ByteBuffer payload = ByteBuffer.allocate(
            Byte.BYTES + Long.BYTES + Integer.BYTES + sourcesBytes + snapshotBytes.length);
        payload.put(FORMAT_VERSION);
        payload.putLong(recordingId);
        payload.putInt(sourceCount);
        orderIntakePositions.forEach((sessionId, position) -> {
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
            throw new UncheckedIOException("매칭 스냅샷 파일 쓰기 실패: " + file, e);
        }
    }

    public Optional<StoredMatchingSnapshot> read() {
        if (!file.exists()) {
            return Optional.empty();
        }
        try {
            byte[] payload = Files.readAllBytes(file.toPath());
            ByteBuffer buffer = ByteBuffer.wrap(payload);
            byte version = buffer.get();
            if (version != FORMAT_VERSION) {
                throw new MatchingSnapshotFormatException(file, version, FORMAT_VERSION);
            }
            long recordingId = buffer.getLong();
            int sourceCount = buffer.getInt();
            Map<Integer, Long> orderIntakePositions = new LinkedHashMap<>();
            for (int i = 0; i < sourceCount; i++) {
                int sessionId = buffer.getInt();
                long position = buffer.getLong();
                orderIntakePositions.put(sessionId, position);
            }
            int headerBytes = Byte.BYTES + Long.BYTES + Integer.BYTES + sourceCount * (Integer.BYTES + Long.BYTES);
            byte[] snapshotBytes = Arrays.copyOfRange(payload, headerBytes, payload.length);
            MatchingSnapshot snapshot = codec.decode(snapshotBytes);
            return Optional.of(new StoredMatchingSnapshot(recordingId, orderIntakePositions, snapshot));
        } catch (IOException e) {
            throw new UncheckedIOException("매칭 스냅샷 파일 읽기 실패: " + file, e);
        }
    }
}
