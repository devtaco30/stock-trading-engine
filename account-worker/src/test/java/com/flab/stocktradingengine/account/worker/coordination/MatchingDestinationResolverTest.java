package com.flab.stocktradingengine.account.worker.coordination;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.logging.Handler;
import java.util.logging.LogRecord;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.flab.stocktradingengine.aeron.MatchingSlotTarget;
import com.flab.stocktradingengine.aeron.StockSlotHasher;

/**
 * 매칭 샤딩 U8 — 브로커 없이 {@link MockConsumer}로 {@link MatchingDestinationResolver}가 맵 토픽
 * {@code matching-shard-map}의 배정·tombstone·깨진 값을 종목 slot별로 반영하는지 확인한다.
 * 맵 토픽 발행자(매칭 pod)는 직접 흉내 낸다(레코드를 consumer에 넣는다).
 */
class MatchingDestinationResolverTest {

    private static final String MAP_TOPIC = MatchingDestinationResolver.MAP_TOPIC;
    private static final int STOCK_SLOT_COUNT = 64;
    private static final String ADDRESS_A = "aeron:udp?endpoint=localhost:20020";
    private static final String ADDRESS_B = "aeron:udp?endpoint=localhost:20021";
    private static final Set<String> ADDRESS_POOL = Set.of(ADDRESS_A, ADDRESS_B);

    private final StockSlotHasher stockSlotHasher = new StockSlotHasher(STOCK_SLOT_COUNT);
    private final Map<Integer, Long> nextOffsetByPartition = new HashMap<>();

    private SleepingMockConsumer consumer;
    private MatchingDestinationResolver resolver;

    /** MockConsumer.poll은 기다리지 않고 바로 돌아와 poll 스레드가 CPU를 태우므로 짧게 쉰다. */
    private static final class SleepingMockConsumer extends MockConsumer<Integer, String> {
        SleepingMockConsumer() {
            super(OffsetResetStrategy.EARLIEST);
        }

        @Override
        public synchronized ConsumerRecords<Integer, String> poll(Duration timeout) {
            ConsumerRecords<Integer, String> records = super.poll(timeout);
            try {
                Thread.sleep(5);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return records;
        }
    }

    @BeforeEach
    void setUp() {
        consumer = new SleepingMockConsumer();
        List<PartitionInfo> partitionInfos = new ArrayList<>();
        Map<TopicPartition, Long> zeroOffsets = new HashMap<>();
        for (int partition = 0; partition < STOCK_SLOT_COUNT; partition++) {
            partitionInfos.add(new PartitionInfo(MAP_TOPIC, partition, null, null, null));
            zeroOffsets.put(new TopicPartition(MAP_TOPIC, partition), 0L);
        }
        consumer.updatePartitions(MAP_TOPIC, partitionInfos);
        consumer.updateBeginningOffsets(zeroOffsets);
        consumer.updateEndOffsets(zeroOffsets);
        resolver = new MatchingDestinationResolver(consumer, stockSlotHasher, ADDRESS_POOL);
    }

    @AfterEach
    void tearDown() {
        resolver.close();
    }

    @Test
    void 같은_종목은_항상_같은_주소를_돌려준다() {
        String stockCode = "005930";
        resolver.start();
        publish(stockSlotHasher.slotFor(stockCode), ADDRESS_A);

        awaitEndpoint(stockCode, Optional.of(ADDRESS_A));
        for (int repeat = 0; repeat < 100; repeat++) {
            assertEquals(Optional.of(ADDRESS_A), resolver.endpointFor(stockCode));
        }
    }

    @Test
    void 맵_토픽에_없는_종목_slot은_empty를_돌려준다() {
        resolver.start();
        resolver.awaitInitialCatchUp(Duration.ofSeconds(10));

        assertEquals(Optional.empty(), resolver.endpointFor("005930"));
        assertEquals(Optional.empty(), resolver.targetFor("005930"));
    }

    @Test
    void tombstone을_받으면_그_종목_slot이_empty로_돌아간다() {
        String stockCode = "005930";
        int slot = stockSlotHasher.slotFor(stockCode);
        resolver.start();
        publish(slot, ADDRESS_A);
        awaitEndpoint(stockCode, Optional.of(ADDRESS_A));

        publish(slot, null);

        awaitEndpoint(stockCode, Optional.empty());
    }

    @Test
    void 형식이_깨진_값은_그_종목_slot만_empty이고_나머지는_유지한다() {
        String brokenStock = "005930";
        String healthyStock = stockInDifferentSlot(brokenStock);
        int brokenSlot = stockSlotHasher.slotFor(brokenStock);
        resolver.start();
        publish(brokenSlot, ADDRESS_A);
        publish(stockSlotHasher.slotFor(healthyStock), ADDRESS_B);
        awaitEndpoint(brokenStock, Optional.of(ADDRESS_A));

        publish(brokenSlot, "   ");
        awaitEndpoint(brokenStock, Optional.empty());
        assertEquals(Optional.of(ADDRESS_B), resolver.endpointFor(healthyStock));

        publish(brokenSlot, ADDRESS_A);
        awaitEndpoint(brokenStock, Optional.of(ADDRESS_A));
        publish(brokenSlot, ";moving");
        awaitEndpoint(brokenStock, Optional.empty());
        assertEquals(Optional.of(ADDRESS_B), resolver.endpointFor(healthyStock));
    }

    @Test
    void 범위_밖_키는_무시하고_로그를_남기며_나머지_배정은_계속_반영한다() {
        String stockCode = "005930";
        List<String> warnings = new ArrayList<>();
        java.util.logging.Logger julLogger = java.util.logging.Logger.getLogger(MatchingDestinationResolver.class.getName());
        Handler captureHandler = new Handler() {
            @Override
            public void publish(LogRecord logRecord) {
                synchronized (warnings) {
                    warnings.add(logRecord.getMessage());
                }
            }

            @Override
            public void flush() {
            }

            @Override
            public void close() {
            }
        };
        julLogger.addHandler(captureHandler);
        try {
            resolver.start();
            publishWithKey(STOCK_SLOT_COUNT, ADDRESS_A);
            publishWithKey(-1, ADDRESS_A);
            publish(stockSlotHasher.slotFor(stockCode), ADDRESS_B);

            awaitEndpoint(stockCode, Optional.of(ADDRESS_B));
            awaitCondition(() -> {
                synchronized (warnings) {
                    return warnings.stream().filter(message -> message.contains("범위 밖")).count() >= 2;
                }
            }, "범위 밖 키 경고 로그 2건");
        } finally {
            julLogger.removeHandler(captureHandler);
        }
    }

    @Test
    void 풀에_없는_주소가_올라오면_조회할_때_예외를_던진다() {
        String misconfiguredStock = "005930";
        String healthyStock = stockInDifferentSlot(misconfiguredStock);
        resolver.start();
        publish(stockSlotHasher.slotFor(misconfiguredStock), "aeron:udp?endpoint=localhost:29999");
        publish(stockSlotHasher.slotFor(healthyStock), ADDRESS_B);
        awaitEndpoint(healthyStock, Optional.of(ADDRESS_B));

        assertThrows(MatchingDestinationMisconfiguredException.class, () -> resolver.endpointFor(misconfiguredStock));
        assertThrows(MatchingDestinationMisconfiguredException.class, () -> resolver.targetFor(misconfiguredStock));
        assertEquals(Optional.of(ADDRESS_B), resolver.endpointFor(healthyStock));
    }

    @Test
    void 초기_읽기가_끝나면_그_전에_쌓인_배정이_이미_반영돼_있다() {
        String stockCode = "005930";
        int slot = stockSlotHasher.slotFor(stockCode);
        Map<TopicPartition, Long> endOffsets = new HashMap<>();
        for (int partitionNumber = 0; partitionNumber < STOCK_SLOT_COUNT; partitionNumber++) {
            endOffsets.put(new TopicPartition(MAP_TOPIC, partitionNumber), partitionNumber == slot ? 1L : 0L);
        }
        consumer.updateEndOffsets(endOffsets);
        consumer.schedulePollTask(() -> consumer.addRecord(new ConsumerRecord<>(MAP_TOPIC, slot, 0L, slot, ADDRESS_A)));

        resolver.start();
        resolver.awaitInitialCatchUp(Duration.ofSeconds(10));

        assertEquals(Optional.of(ADDRESS_A), resolver.endpointFor(stockCode));
    }

    @Test
    void 맵_토픽이_비어_있으면_초기_읽기를_기다리지_않고_바로_돌아온다() {
        resolver.start();

        assertTimeoutPreemptively(Duration.ofSeconds(3),
            () -> resolver.awaitInitialCatchUp(Duration.ofSeconds(10)));
    }

    @Test
    void 한_번에_받은_변경_둘은_읽는_쪽에_섞여_보이지_않고_이미_읽은_표는_바뀌지_않는다() throws Exception {
        String firstStock = "005930";
        String secondStock = stockInDifferentSlot(firstStock);
        int firstSlot = stockSlotHasher.slotFor(firstStock);
        int secondSlot = stockSlotHasher.slotFor(secondStock);
        resolver.start();
        schedulePairedPublish(firstSlot, secondSlot, ADDRESS_A);
        awaitEndpoint(firstStock, Optional.of(ADDRESS_A));

        MatchingSlotTarget[] tableBeforeUpdate = resolver.snapshot();
        MatchingSlotTarget firstBefore = tableBeforeUpdate[firstSlot];
        MatchingSlotTarget secondBefore = tableBeforeUpdate[secondSlot];

        AtomicBoolean stopReading = new AtomicBoolean(false);
        AtomicInteger mixedReads = new AtomicInteger(0);
        AtomicInteger consistentReads = new AtomicInteger(0);
        Thread reader = new Thread(() -> {
            while (!stopReading.get()) {
                MatchingSlotTarget[] table = resolver.snapshot();
                MatchingSlotTarget first = table[firstSlot];
                MatchingSlotTarget second = table[secondSlot];
                if (first == null || second == null || !first.endpoint().equals(second.endpoint())) {
                    mixedReads.incrementAndGet();
                } else {
                    consistentReads.incrementAndGet();
                }
            }
        }, "table-reader");
        reader.start();
        for (int flip = 0; flip < 200; flip++) {
            String address = flip % 2 == 0 ? ADDRESS_B : ADDRESS_A;
            schedulePairedPublish(firstSlot, secondSlot, address);
            Thread.sleep(2);
        }
        awaitEndpoint(firstStock, Optional.of(ADDRESS_A));
        stopReading.set(true);
        reader.join(5000);

        assertEquals(0, mixedReads.get(), "두 칸이 서로 다른 갱신에서 온 값으로 섞여 읽힘");
        assertTrue(consistentReads.get() > 0);
        assertEquals(ADDRESS_A, firstBefore.endpoint());
        assertEquals(ADDRESS_A, secondBefore.endpoint());
        assertNotSame(tableBeforeUpdate, resolver.snapshot());
        assertEquals(firstBefore, tableBeforeUpdate[firstSlot]);
    }

    @Test
    void 맵_값의_주소와_이동_중_표시를_읽고_다시_쓸_수_있다() {
        String channelWithParams = "aeron:udp?endpoint=localhost:20020|term-length=64k";

        MatchingSlotTarget steady = MatchingSlotTarget.parse(ADDRESS_A);
        MatchingSlotTarget moving = MatchingSlotTarget.parse(ADDRESS_A + ";moving");
        MatchingSlotTarget movingWithParams = MatchingSlotTarget.parse(channelWithParams + ";moving");

        assertEquals(new MatchingSlotTarget(ADDRESS_A, false), steady);
        assertEquals(new MatchingSlotTarget(ADDRESS_A, true), moving);
        assertEquals(new MatchingSlotTarget(channelWithParams, true), movingWithParams);
        assertEquals(ADDRESS_A, steady.encode());
        assertEquals(ADDRESS_A + ";moving", moving.encode());
        assertEquals(moving, MatchingSlotTarget.parse(moving.encode()));
        assertDoesNotThrow(() -> MatchingSlotTarget.parse(channelWithParams));
    }

    @Test
    void 이동_중_표시가_붙은_종목_slot은_주소와_이동_중_여부를_함께_돌려준다() {
        String stockCode = "005930";
        resolver.start();
        publish(stockSlotHasher.slotFor(stockCode), ADDRESS_A + ";moving");

        awaitEndpoint(stockCode, Optional.of(ADDRESS_A));
        assertEquals(Optional.of(new MatchingSlotTarget(ADDRESS_A, true)), resolver.targetFor(stockCode));
    }

    // ---- 헬퍼 ----

    private String stockInDifferentSlot(String stockCode) {
        int slot = stockSlotHasher.slotFor(stockCode);
        for (int candidate = 1; candidate < 1000; candidate++) {
            String candidateCode = String.format("%06d", candidate);
            if (stockSlotHasher.slotFor(candidateCode) != slot) {
                return candidateCode;
            }
        }
        throw new IllegalStateException("다른 종목 slot의 종목코드를 찾지 못했다");
    }

    private void publish(int slot, String value) {
        publishWithKey(slot, value);
    }

    /** 키가 곧 종목 slot이다. 레코드가 놓이는 파티션은 키와 무관하므로 범위 안의 파티션 하나에 넣는다. */
    private void publishWithKey(int slotKey, String value) {
        int partition = Math.floorMod(slotKey, STOCK_SLOT_COUNT);
        long offset = nextOffsetByPartition.merge(partition, 1L, Long::sum) - 1L;
        consumer.addRecord(new ConsumerRecord<>(MAP_TOPIC, partition, offset, slotKey, value));
    }

    /** 두 레코드를 같은 poll 안에 넣어 한 번에 받는 배치로 만든다(스케줄된 작업은 poll 안에서 먼저 실행된다). */
    private void schedulePairedPublish(int firstSlot, int secondSlot, String value) {
        consumer.schedulePollTask(() -> {
            publish(firstSlot, value);
            publish(secondSlot, value);
        });
    }

    private void awaitEndpoint(String stockCode, Optional<String> expected) {
        awaitCondition(() -> expected.equals(resolver.endpointFor(stockCode)),
            stockCode + " 의 주소가 " + expected + " 가 되기");
    }

    private void awaitCondition(BooleanSupplier condition, String description) {
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
        }
        throw new AssertionError("10초 안에 이루어지지 않음: " + description);
    }
}
