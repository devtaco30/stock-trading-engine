package com.flab.stocktradingengine.account.worker.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BooleanSupplier;

import org.agrona.DirectBuffer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.flab.stocktradingengine.account.disruptor.domain.AccountResultListener;
import com.flab.stocktradingengine.account.disruptor.domain.RejectReason;
import com.flab.stocktradingengine.account.disruptor.engine.AccountEngine;
import com.flab.stocktradingengine.account.worker.coordination.MatchingDestinationResolver;
import com.flab.stocktradingengine.aeron.StockSlotHasher;
import com.flab.stocktradingengine.codec.JournaledOrder;
import com.flab.stocktradingengine.codec.OrderCodec;

import io.aeron.Publication;

/**
 * 맵 토픽 읽기 thread가 초기 읽기 뒤에 예외로 끝나면 「resolver가 모든 종목 slot을 알 수 없음으로 돌려줌 →
 * sender가 주문을 보관 목록에 둠 → 보관 목록이 가득 차면 계좌 엔진이 MATCHING_HOLDING_FULL로 거부」가
 * 실제로 이어지는지 본다. resolver(MockConsumer로 읽기), {@link AeronMatchingOrderSender},
 * {@link AccountEngine}은 모두 실제 객체이고 Publication과 결과 리스너만 대역이다.
 */
class MatchingMapReadFailureHoldingIntegrationTest {

    private static final String MAP_TOPIC = MatchingDestinationResolver.MAP_TOPIC;
    private static final String ADDRESS = "aeron:udp?endpoint=localhost:20020";
    private static final String STOCK = "005930";
    private static final int STOCK_SLOT_COUNT = 64;
    private static final int HOLDING_CAPACITY = 2;
    private static final long ACCOUNT_ID = 1L;
    private static final int AWAIT_MILLIS = 5000;

    private final StockSlotHasher stockSlotHasher = new StockSlotHasher(STOCK_SLOT_COUNT);
    private final List<JournaledOrder> sent = new CopyOnWriteArrayList<>();

    private MatchingDestinationResolver resolver;
    private AeronMatchingOrderSender sender;
    private AccountEngine engine;

    /** MockConsumer.poll은 기다리지 않고 바로 돌아와 읽기 thread가 CPU를 태우므로 짧게 쉰다. */
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

    @AfterEach
    void tearDown() {
        if (engine != null) {
            engine.shutdown();
        }
        if (sender != null) {
            sender.close();
        }
        if (resolver != null) {
            resolver.close();
        }
    }

    @Test
    void 읽기_thread가_죽으면_주문이_보관되고_보관이_가득_차면_계좌_엔진이_MATCHING_HOLDING_FULL로_거부한다() throws Exception {
        SleepingMockConsumer consumer = new SleepingMockConsumer();
        int slot = stockSlotHasher.slotFor(STOCK);
        List<PartitionInfo> partitionInfos = new ArrayList<>();
        Map<TopicPartition, Long> beginOffsets = new HashMap<>();
        Map<TopicPartition, Long> endOffsets = new HashMap<>();
        for (int partition = 0; partition < STOCK_SLOT_COUNT; partition++) {
            partitionInfos.add(new PartitionInfo(MAP_TOPIC, partition, null, null, null));
            beginOffsets.put(new TopicPartition(MAP_TOPIC, partition), 0L);
            endOffsets.put(new TopicPartition(MAP_TOPIC, partition), partition == slot ? 1L : 0L);
        }
        consumer.updatePartitions(MAP_TOPIC, partitionInfos);
        consumer.updateBeginningOffsets(beginOffsets);
        consumer.updateEndOffsets(endOffsets);
        consumer.schedulePollTask(() -> consumer.addRecord(new ConsumerRecord<>(MAP_TOPIC, slot, 0L, slot, ADDRESS)));
        resolver = new MatchingDestinationResolver(consumer, stockSlotHasher, Set.of(ADDRESS));
        resolver.start();
        resolver.awaitInitialCatchUp(Duration.ofSeconds(10));

        Publication publication = mock(Publication.class);
        OrderCodec codec = new OrderCodec();
        when(publication.offer(any(DirectBuffer.class), anyInt(), anyInt())).thenAnswer(invocation -> {
            sent.add(codec.decode(invocation.getArgument(0), 0));
            return 100L;
        });
        sender = new AeronMatchingOrderSender(resolver, Map.of(ADDRESS, publication), stockSlotHasher, HOLDING_CAPACITY);
        sender.start();
        AccountResultListener listener = mock(AccountResultListener.class);
        engine = new AccountEngine(1024, 0L, sender, listener);
        engine.seed(ACCOUNT_ID, new BigDecimal("1000000"), new BigDecimal("0.40"));
        engine.start();

        // 읽기 thread가 살아 있는 동안에는 주문이 담당 주소로 나간다.
        engine.publishBuy(ACCOUNT_ID, STOCK, new BigDecimal("10000"), 10, "r1");
        await(() -> sent.size() == 1, "첫 주문이 담당 주소로 나가기");

        // 읽기 thread가 초기 읽기 뒤에 예외로 끝난다.
        consumer.schedulePollTask(() -> {
            throw new KafkaException("초기 읽기 뒤 브로커 오류");
        });
        await(() -> resolver.readFailure().isPresent(), "읽기 thread 종료 기록");

        // 담당을 모르는 상태와 같아져 새 주문 둘은 나가지 않고 보관된다.
        engine.publishBuy(ACCOUNT_ID, STOCK, new BigDecimal("10000"), 10, "r2");
        engine.publishBuy(ACCOUNT_ID, STOCK, new BigDecimal("10000"), 10, "r3");
        await(() -> sender.holdingCount(STOCK) == HOLDING_CAPACITY, "주문 둘이 보관 목록에 쌓이기");
        assertThat(sent).hasSize(1);
        assertThat(sender.hasHoldingRoom(STOCK)).isFalse();

        // 보관이 가득 찬 뒤의 주문은 예약 전에 거부된다.
        engine.publishBuy(ACCOUNT_ID, STOCK, new BigDecimal("10000"), 10, "r4");
        verify(listener, timeout(AWAIT_MILLIS)).onRejected(eq(ACCOUNT_ID), anyLong(), eq("r4"), eq(RejectReason.MATCHING_HOLDING_FULL));
        verify(listener, never()).onAccepted(anyLong(), anyLong(), eq("r4"), any());
        assertThat(sent).hasSize(1);
        assertThat(sender.holdingCount(STOCK)).isEqualTo(HOLDING_CAPACITY);
        // 접수된 주문 셋(r1~r3)의 예약증거금만 잡혀 있고 r4의 예약은 없다: 10주 × 10,000원 × 0.40 × 3건.
        assertThat(engine.accountState(ACCOUNT_ID).reservedMargin()).isEqualByComparingTo("120000");
    }

    private static void await(BooleanSupplier condition, String description) throws InterruptedException {
        long deadline = System.currentTimeMillis() + AWAIT_MILLIS;
        while (!condition.getAsBoolean()) {
            if (System.currentTimeMillis() > deadline) {
                throw new AssertionError("시간 안에 이루어지지 않음: " + description);
            }
            Thread.sleep(10);
        }
    }
}
