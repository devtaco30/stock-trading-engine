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
import java.util.Optional;

/**
 * U3-b — "Kafka로 마지막으로 보낸 주문 결과가 어느 recording의 어느 position까지인지"를
 * archive-dir(저널·스냅샷과 같은 durable 디스크)에 파일로 저장·조회한다. {@code AccountSnapshotStore}와
 * 같은 원자적 쓰기(임시 파일 → fsync → rename) 결.
 *
 * <h3>파일 레이아웃</h3>
 * <pre>[recordingId:8][position:8]</pre>
 */
public class OrderResultForwardPositionStore {

    private static final String FILE_NAME = "order-result-forward-position.dat";
    private static final String TEMP_FILE_NAME = "order-result-forward-position.dat.tmp";

    private final File file;
    private final File tempFile;

    public OrderResultForwardPositionStore(File archiveDir) {
        this.file = new File(archiveDir, FILE_NAME);
        this.tempFile = new File(archiveDir, TEMP_FILE_NAME);
    }

    /** {@code AccountSnapshotStore#write}와 같은 이유로 fsync까지 끝난 뒤에야 반환한다. */
    public void write(long recordingId, long position) {
        ByteBuffer payload = ByteBuffer.allocate(Long.BYTES + Long.BYTES);
        payload.putLong(recordingId);
        payload.putLong(position);
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
            throw new UncheckedIOException("주문 결과 forward position 파일 쓰기 실패: " + file, e);
        }
    }

    public Optional<StoredOrderResultForwardPosition> read() {
        if (!file.exists()) {
            return Optional.empty();
        }
        try {
            byte[] payload = Files.readAllBytes(file.toPath());
            ByteBuffer buffer = ByteBuffer.wrap(payload);
            long recordingId = buffer.getLong();
            long position = buffer.getLong();
            return Optional.of(new StoredOrderResultForwardPosition(recordingId, position));
        } catch (IOException e) {
            throw new UncheckedIOException("주문 결과 forward position 파일 읽기 실패: " + file, e);
        }
    }
}
