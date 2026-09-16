package com.flab.stocktradingengine.matching.worker.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

import com.flab.stocktradingengine.aeron.AeronStreamIds;
import com.flab.stocktradingengine.codec.EventType;
import com.flab.stocktradingengine.codec.FillCodec;
import com.flab.stocktradingengine.codec.FilledTrade;
import com.flab.stocktradingengine.codec.JournaledOrder;
import com.flab.stocktradingengine.codec.OrderCodec;
import com.flab.stocktradingengine.matching.disruptor.engine.MatchingEngine;
import com.flab.stocktradingengine.matching.disruptor.io.AeronOrderReceiver;
import com.flab.stocktradingengine.matching.worker.MatchingWorkerApplication;
import com.flab.stocktradingengine.trading.entity.OrderSide;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.logbuffer.FragmentHandler;

/**
 * I2 U3 — 이 트랙의 핵심 증명. 매칭이 주문을 Aeron으로 받아 인테이크 스트림에는 durable하게
 * 녹화됐지만, 링버퍼(매칭 엔진)에는 아직 들어가지 못한 채 죽는 상황을 만들고, 재기동 뒤 그
 * 주문이 호가창에 살아 있는지 + 이미 정상 반영된 다른 주문이 이중 반영되지 않는지 본다.
 *
 * <h3>"링버퍼에 들어가기 전에 죽는다"를 타이밍 경합 없이 결정적으로 만드는 방법</h3>
 * <p>이 스트림을 소비해 링버퍼에 넣는 경로는 {@link AeronOrderReceiver}의 폴링 스레드 하나뿐이다
 * — Archive REMOTE 녹화는 그 스레드가 폴링하는 것과 무관하게 Aeron 드라이버 레벨에서 이미지에
 * 직접 붙어 기록한다(I2 U1 javadoc). run1에서 정상 반영되는 주문({@link #NORMAL_ORDER_ID})을
 * 먼저 하나 보내(폴링 스레드가 살아있을 때) 반영을 확인한 뒤, {@link AeronOrderReceiver#close()}로
 * 그 폴링 스레드만 영구히 멈추고 목표 주문({@link #TARGET_ORDER_ID})을 보낸다 — 그 뒤 발행한
 * 주문은 녹화엔 반드시 남지만 매칭 엔진 링버퍼엔 절대 들어가지 못한다("먼저 도착하면 반영될 수도
 * 있는" 경합이 아니라 항상 같은 결과가 나온다).</p>
 *
 * <p>{@code matching.worker.graceful-snapshot-enabled=true}(운영 기본값)로 두 run을 모두 띄운다.
 * 꺼두면 스냅샷 파일이 안 남아 run2의 {@code matchingOrderIntakeReplayedEntries}가 빈 맵을 받아
 * recording을 처음부터 통째로 다시 읽는 경로를 타버린다 — "저장된 위치부터 이어 읽기"가 아니라
 * "처음부터 전부"로도 우연히 통과해버려 이 유닛의 핵심(위치 기준 재생)을 증명하지 못한다. 켜두면
 * run1 종료 시 정상 종료 스냅샷이 {@link #NORMAL_ORDER_ID}의 반영 위치까지만 담고(목표 주문은
 * 반영된 적이 없어 담길 수 없다), run2는 스냅샷 복원 → 위치 시드 → 그 위치부터 인테이크 재생이라는
 * 실제 운영 경로를 탄다.</p>
 *
 * <h3>이중 반영 검증({@link #NORMAL_ORDER_ID})</h3>
 * <p>{@link #NORMAL_ORDER_ID}는 저널에도 있고(정상 반영됐으니) 인테이크 녹화에도 있다 — 위치
 * 기준 재생이 틀리면(스냅샷의 위치를 무시하거나 잘못 계산하면) 저널 리플레이와 인테이크 리플레이
 * 양쪽에서 다시 반영돼 잔량이 두 배가 될 수 있다(I2 U2b LLD §4-1, 돈이 틀어지는 경로). run2에서
 * 원래 수량과 정확히 같은 매도 주문으로 상계해보고, 그 뒤 작은 탐지용 주문을 하나 더 보내
 * 아무것도 안 남았는지(체결이 또 나지 않는지) 확인한다.</p>
 */
class MatchingOrderIntakeRecoveryIntegrationTest {

    private static final String STOCK = "005930";
    private static final BigDecimal PRICE = new BigDecimal("10000");
    private static final int NORMAL_ORDER_QUANTITY = 10;
    private static final long NORMAL_ORDER_ID = 1L; // run1에서 폴링 스레드가 살아있을 때 정상 반영
    private static final long TARGET_ORDER_ID = 2L; // 폴링 스레드를 멈춘 뒤 발행 — 반영 전에 "죽는다"
    private static final long PROBE_MATCH_ORDER_ID = 3L; // run2 — NORMAL_ORDER_ID와 정확히 상계
    private static final long PROBE_LEFTOVER_ORDER_ID = 4L; // run2 — 잔량 탐지용
    private static final long TIMEOUT_NANOS = 5_000_000_000L;
    private static final long NO_ARRIVAL_TIMEOUT_NANOS = 300_000_000L;
    private static final int FRAGMENT_LIMIT = 10;

    private final OrderCodec orderCodec = new OrderCodec();
    private final FillCodec fillCodec = new FillCodec();

    @TempDir
    private Path archiveDir;

    @Test
    void 링버퍼_반영_전에_죽어도_재기동하면_주문이_호가창에_살아있고_이미_반영된_주문은_이중반영되지_않는다() {
        ConfigurableApplicationContext run1 = launch();
        try {
            Aeron aeron = run1.getBean(Aeron.class);
            AeronArchive archive = run1.getBean(AeronArchive.class);

            Publication publication = aeron.addPublication(
                MatchingOrderIntakeConfig.DEFAULT_INTAKE_CHANNEL, AeronStreamIds.MATCHING_INTAKE);
            try {
                awaitConnected(publication);
                long recordingId = awaitRecordingId(archive, publication.sessionId());

                MatchingEngine engine = run1.getBean(MatchingEngine.class);

                // 폴링 스레드가 살아있을 때 정상 반영되는 주문 — 저널·스냅샷 양쪽에 남긴다.
                send(publication, new JournaledOrder(EventType.PLACE, NORMAL_ORDER_ID, 100L, STOCK,
                    OrderSide.BUY, PRICE, NORMAL_ORDER_QUANTITY, Instant.now()));
                awaitContainsOrder(engine, NORMAL_ORDER_ID);

                AeronOrderReceiver receiver = run1.getBean(AeronOrderReceiver.class);
                receiver.close(); // 폴링 스레드를 영구히 멈춘다 — 이후 발행분은 링버퍼에 못 들어간다

                long positionBeforeTarget = archive.getRecordingPosition(recordingId);
                send(publication, new JournaledOrder(EventType.PLACE, TARGET_ORDER_ID, 200L, STOCK,
                    OrderSide.BUY, new BigDecimal("9000"), 5, Instant.now()));
                awaitPositionAdvance(archive, recordingId, positionBeforeTarget); // 녹화가 실제로 디스크에 남았다는 증거

                assertThat(engine.containsOrder(STOCK, TARGET_ORDER_ID))
                    .as("폴링 스레드를 멈췄으니 녹화는 됐어도 링버퍼엔 절대 못 들어가야 한다")
                    .isFalse();
            } finally {
                publication.close();
            }
        } finally {
            run1.close(); // "죽음" 시뮬레이션 — graceful-snapshot-enabled=true라 종료 시점 스냅샷이 남는다
        }

        ConfigurableApplicationContext run2 = launch();
        try {
            MatchingEngine engine = run2.getBean(MatchingEngine.class);
            assertThat(engine.containsOrder(STOCK, TARGET_ORDER_ID))
                .as("주문 인테이크 리플레이(I2 U2b)로 이 주문이 복구돼야 한다")
                .isTrue();

            Aeron aeron = run2.getBean(Aeron.class);
            try (Subscription fillSubscription = aeron.addSubscription(ShardRoutingConfig.DEFAULT_FILL_CHANNEL, AeronStreamIds.FILL)) {
                awaitConnected(fillSubscription);

                Publication publication = aeron.addPublication(
                    MatchingOrderIntakeConfig.DEFAULT_INTAKE_CHANNEL, AeronStreamIds.MATCHING_INTAKE);
                try {
                    awaitConnected(publication);

                    // 원래 수량과 정확히 같은 매도 주문으로 상계한다 — NORMAL_ORDER_ID가 저널·
                    // 인테이크 양쪽에서 이중 반영됐다면 잔량이 20이 돼 이 매도 10으로는 다 못 없앤다.
                    send(publication, new JournaledOrder(EventType.PLACE, PROBE_MATCH_ORDER_ID, 300L, STOCK,
                        OrderSide.SELL, PRICE, NORMAL_ORDER_QUANTITY, Instant.now()));
                    FilledTrade trade = awaitFill(fillSubscription);
                    assertThat(trade.buyOrderId()).isEqualTo(NORMAL_ORDER_ID);
                    assertThat(trade.filledQuantity())
                        .as("이중 반영됐다면 이 체결도 10으로 끝나겠지만 잔량이 남아 다음 탐지 주문에서 걸린다")
                        .isEqualTo(NORMAL_ORDER_QUANTITY);

                    // 잔량 탐지 — 위 체결 뒤에도 NORMAL_ORDER_ID가 남아있다면(이중 반영) 여기서 또 체결된다.
                    send(publication, new JournaledOrder(EventType.PLACE, PROBE_LEFTOVER_ORDER_ID, 300L, STOCK,
                        OrderSide.SELL, PRICE, 1, Instant.now()));
                    assertThat(awaitFillOrNothing(fillSubscription))
                        .as("이중 반영이 아니라면 NORMAL_ORDER_ID는 위에서 이미 정확히 상계돼 남은 잔량이 없어야 한다")
                        .isEmpty();
                } finally {
                    publication.close();
                }
            }
        } finally {
            run2.close();
        }
    }

    private ConfigurableApplicationContext launch() {
        return new SpringApplicationBuilder(MatchingWorkerApplication.class)
            .web(WebApplicationType.NONE)
            .properties(
                "matching.worker.archive-dir=" + archiveDir.toAbsolutePath(),
                "matching.worker.graceful-snapshot-enabled=true")
            .run();
    }

    /** 주문을 인코딩해 Aeron으로 발행한다. 백프레셔면 받아들여질 때까지 재시도한다. */
    private void send(Publication publication, JournaledOrder order) {
        UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(256));
        int length = orderCodec.encode(buffer, 0, order);

        long deadline = System.nanoTime() + TIMEOUT_NANOS;
        long result;
        do {
            result = publication.offer(buffer, 0, length);
            if (result > 0) {
                return;
            }
            if (System.nanoTime() > deadline) {
                throw new AssertionError("5초 안에 주문 발행 실패 — offer 반환=" + result);
            }
            Thread.yield();
        } while (true);
    }

    private void awaitConnected(Publication publication) {
        long deadline = System.nanoTime() + TIMEOUT_NANOS;
        while (!publication.isConnected()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("5초 안에 Publication이 연결되지 않음");
            }
            Thread.yield();
        }
    }

    private void awaitConnected(Subscription subscription) {
        long deadline = System.nanoTime() + TIMEOUT_NANOS;
        while (!subscription.isConnected()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("5초 안에 체결 구독이 연결되지 않음");
            }
            Thread.yield();
        }
    }

    private void awaitContainsOrder(MatchingEngine engine, long orderId) {
        long deadline = System.nanoTime() + TIMEOUT_NANOS;
        while (!engine.containsOrder(STOCK, orderId)) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("5초 안에 주문이 호가창에 반영되지 않음: orderId=" + orderId);
            }
            Thread.yield();
        }
    }

    private long awaitRecordingId(AeronArchive archive, int sessionId) {
        long deadline = System.nanoTime() + TIMEOUT_NANOS;
        long recordingId;
        while ((recordingId = findRecordingId(archive, sessionId)) == -1L) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("5초 안에 주문 인테이크 녹화가 시작되지 않음");
            }
            Thread.yield();
        }
        return recordingId;
    }

    private long findRecordingId(AeronArchive archive, int sessionId) {
        long[] found = {-1L};
        archive.listRecordingsForUri(0, 10,
            MatchingOrderIntakeConfig.DEFAULT_INTAKE_CHANNEL, AeronStreamIds.MATCHING_INTAKE,
            (controlSessionId, correlationId, recordingId, startTimestamp, stopTimestamp,
             startPosition, stopPosition, initialTermId, segmentFileLength, termBufferLength,
             mtuLength, foundSessionId, streamId, strippedChannel, originalChannel, sourceIdentity) -> {
                if (foundSessionId == sessionId) {
                    found[0] = recordingId;
                }
            });
        return found[0];
    }

    /** 녹화 위치가 기준값보다 늘어날 때까지 기다린 뒤, 늘어난 위치를 돌려준다. */
    private long awaitPositionAdvance(AeronArchive archive, long recordingId, long positionBefore) {
        long deadline = System.nanoTime() + TIMEOUT_NANOS;
        long position;
        while ((position = archive.getRecordingPosition(recordingId)) <= positionBefore) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("5초 안에 녹화 위치가 늘지 않음");
            }
            Thread.yield();
        }
        return position;
    }

    private FilledTrade awaitFill(Subscription subscription) {
        List<FilledTrade> received = new ArrayList<>();
        FragmentHandler handler = (buffer, offset, length, header) -> received.add(fillCodec.decode(buffer, offset));

        long deadline = System.nanoTime() + TIMEOUT_NANOS;
        while (received.isEmpty()) {
            subscription.poll(handler, FRAGMENT_LIMIT);
            if (System.nanoTime() > deadline) {
                throw new AssertionError("5초 안에 체결 스트림에 체결이 발행되지 않음");
            }
        }
        return received.get(0);
    }

    /** 짧은 시간 안에 체결이 오면 그 값을, 안 오면 빈 값을 돌려준다 — "체결이 없어야 정상"인 경우의 탐지용. */
    private Optional<FilledTrade> awaitFillOrNothing(Subscription subscription) {
        List<FilledTrade> received = new ArrayList<>();
        FragmentHandler handler = (buffer, offset, length, header) -> received.add(fillCodec.decode(buffer, offset));

        long deadline = System.nanoTime() + NO_ARRIVAL_TIMEOUT_NANOS;
        while (received.isEmpty() && System.nanoTime() <= deadline) {
            subscription.poll(handler, FRAGMENT_LIMIT);
        }
        return received.isEmpty() ? Optional.empty() : Optional.of(received.get(0));
    }
}
