package com.flab.stocktradingengine.matching.worker.coordination;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntFunction;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.flab.stocktradingengine.aeron.MatchingSlotTarget;
import com.flab.stocktradingengine.aeron.StockSlotHasher;
import com.flab.stocktradingengine.matching.disruptor.engine.MatchingEngine;
import com.flab.stocktradingengine.matching.disruptor.io.SlotEventListener;
import com.flab.stocktradingengine.matching.disruptor.io.SlotEventRequests;
import com.lmax.disruptor.BlockingWaitStrategy;

/**
 * 이동으로 받는지 첫 배정인지의 판정 시점과, 담당이 켜지기 전에 회수된 종목 slot의 처리(보정 1·2). 판정은 배정 콜백이 아니라
 * 맵 읽기가 맵 토픽의 끝까지 따라온 뒤 별도 thread에서 한다. 이동으로 받는 도중 회수되면 맵을 쓰지 않아 다음 담당이 이전 담당의 이동 중
 * 값을 그대로 본다.
 */
class KafkaMatchingShardAssignmentDecisionTest {

    private static final String OWN = "aeron:udp?endpoint=localhost:20021";
    private static final String PREVIOUS_OWNER = "aeron:udp?endpoint=localhost:20020";
    private static final StockSlotHasher HASHER = new StockSlotHasher(64);

    private final SlotEventRequests requests = new SlotEventRequests();
    private final MockProducer<Integer, String> mapProducer =
        new MockProducer<>(true, new IntegerSerializer(), new StringSerializer());
    private final List<Integer> handoverRequested = new CopyOnWriteArrayList<>();
    private final List<Integer> handoverCancelled = new CopyOnWriteArrayList<>();
    private final List<Integer> queuedWhenRequested = new CopyOnWriteArrayList<>();
    private final List<Integer> cleanedSlots = new CopyOnWriteArrayList<>();
    private final List<Runnable> deferredDecisions = new ArrayList<>();
    private final AtomicReference<Optional<MatchingSlotTarget>> mapEntry = new AtomicReference<>(Optional.empty());
    private final AtomicReference<Boolean> caughtUp = new AtomicReference<>(true);
    private final List<String> order = new CopyOnWriteArrayList<>();
    private MatchingEngine engine;
    private KafkaMatchingShardAssignment assignment;
    private ConsumerRebalanceListener listener;
    private boolean deferDecisions;

    @BeforeEach
    void setUp() {
        engine = new MatchingEngine(1024, new BlockingWaitStrategy(), (stockCode, fill) -> { },
            new com.flab.stocktradingengine.matching.disruptor.journal.InMemoryJournal(), HASHER, new SlotEventListener() {
                @Override
                public void onSlotCleanedUp(int stockSlot) {
                    cleanedSlots.add(stockSlot);
                }
            });
        engine.start();
        MovedSlotRequests movedSlotRequests = new MovedSlotRequests() {
            @Override
            public void requestHandover(int stockSlot) {
                // 요청을 낼 때 이미 남아 있는 저널 요청(넘김) 수. 요청하기 전에 넘김이 남겨졌는지 본다.
                queuedWhenRequested.add(requests.drainTo(engine));
                handoverRequested.add(stockSlot);
            }

            @Override
            public void cancelHandover(int stockSlot) {
                handoverCancelled.add(stockSlot);
            }
        };
        IntFunction<Optional<MatchingSlotTarget>> lookup = slot -> {
            order.add("lookup");
            return mapEntry.get();
        };
        MapSync mapSync = () -> {
            order.add("sync");
            return caughtUp.get();
        };
        Executor executor = task -> {
            if (deferDecisions) {
                deferredDecisions.add(task);
            } else {
                task.run();
            }
        };
        assignment = new KafkaMatchingShardAssignment(mock(Consumer.class), mapProducer, mock(Admin.class), 64, OWN,
            "assignment-topic", "map-topic", requests, lookup, movedSlotRequests, mapSync, executor);
        listener = assignment.rebalanceListener();
    }

    @AfterEach
    void tearDown() {
        assignment.close();
        engine.shutdown();
    }

    @Test
    void 배정_콜백은_판정을_기다리지_않고_별도_thread에_맡긴다() {
        deferDecisions = true;
        mapEntry.set(Optional.of(new MatchingSlotTarget(PREVIOUS_OWNER, true)));

        listener.onPartitionsAssigned(partitions(5));

        assertThat(deferredDecisions).hasSize(1);
        assertThat(order).as("콜백 안에서는 맵도 따라잡기도 읽지 않는다").isEmpty();
        assertThat(handoverRequested).isEmpty();
    }

    @Test
    void 이동으로_받는_것으로_판정하면_호가창을_요청하기_전에_그_종목_slot의_넘김을_저널에_남긴다() {
        // 재시작 때 저널의 받음만 보고 담당으로 복원된 종목 slot이 실제로는 다른 pod에 배정된 뒤 이동해 오는 경우, 엔진이 담당으로 알고 있으면
        // 도착하는 호가창 적재를 거부한다. 호가창을 요청하는 중이면 담당이 아님이 확실하므로 요청하기 전에 넘김을 남긴다.
        mapEntry.set(Optional.of(new MatchingSlotTarget(PREVIOUS_OWNER, true)));

        listener.onPartitionsAssigned(partitions(5));

        assertThat(handoverRequested).containsExactly(5);
        assertThat(queuedWhenRequested).as("요청을 내는 시점에 넘김 요청이 이미 하나 남아 있다").containsExactly(1);
    }

    @Test
    void 맵을_읽기_전에_맵_읽기가_따라올_때까지_기다린다() {
        mapEntry.set(Optional.of(new MatchingSlotTarget(PREVIOUS_OWNER, true)));

        listener.onPartitionsAssigned(partitions(5));

        assertThat(order.get(0)).isEqualTo("sync");
        assertThat(order).contains("lookup");
        assertThat(handoverRequested).containsExactly(5);
    }

    @Test
    void 시간_안에_따라오지_못하면_첫_배정으로_가정하지_않고_담당을_켜지_않으며_맵도_쓰지_않는다() throws Exception {
        caughtUp.set(false);

        listener.onPartitionsAssigned(partitions(5));

        assertThat(order).as("따라오지 못했으면 맵을 읽지 않는다").containsExactly("sync");
        assertThat(handoverRequested).isEmpty();
        assertThat(mapProducer.history()).isEmpty();
        assertThat(requests.drainTo(engine)).as("받음 요청도 남기지 않는다").isZero();
        assertThat(assignment.test(5)).isFalse();
    }

    @Test
    void 판정을_기다리는_동안_회수된_종목_slot은_아무것도_하지_않는다() {
        deferDecisions = true;
        listener.onPartitionsAssigned(partitions(5));

        listener.onPartitionsRevoked(partitions(5));
        deferredDecisions.forEach(Runnable::run);

        assertThat(handoverRequested).isEmpty();
        assertThat(requests.drainTo(engine)).isZero();
        assertThat(mapProducer.history()).isEmpty();
    }

    @Test
    void 호가창을_기다리는_도중_회수되면_맵을_쓰지_않고_진행_중_요청을_취소하며_적재한_복사본을_정리한다() throws Exception {
        mapEntry.set(Optional.of(new MatchingSlotTarget(PREVIOUS_OWNER, true)));
        listener.onPartitionsAssigned(partitions(5));

        listener.onPartitionsRevoked(partitions(5));

        assertThat(mapProducer.history()).as("맵의 이전 담당 값(주소;moving)을 덮어쓰지 않는다").isEmpty();
        assertThat(handoverCancelled).containsExactly(5);
        requests.drainTo(engine);
        awaitCleaned(5);
        assertThat(cleanedSlots).containsExactly(5);
    }

    @Test
    void 받음_요청을_남긴_뒤_담당이_켜지기_전에_회수되면_맵을_쓰지_않고_넘김_항목으로_저널을_맞춘다() throws Exception {
        mapEntry.set(Optional.of(new MatchingSlotTarget(PREVIOUS_OWNER, true)));
        listener.onPartitionsAssigned(partitions(5));
        assignment.acquireAfterLoad(5);

        listener.onPartitionsRevoked(partitions(5));

        assertThat(mapProducer.history()).isEmpty();
        assertThat(handoverCancelled).containsExactly(5);
        assertThat(assignment.test(5)).isFalse();
        requests.drainTo(engine);
        awaitJournal(3);
        assertThat(engine.journal().entries()).extracting(entry -> entry.type().name())
            .containsExactly("SLOT_RELEASED", "SLOT_ACQUIRED", "SLOT_RELEASED");
        assignment.confirmAcquired(5);
        assertThat(assignment.test(5)).as("회수된 뒤 늦게 온 기록 확인이 담당을 켜지 않는다").isFalse();
    }

    @Test
    void 이미_담당인_종목_slot의_회수는_예전처럼_이동_중_표시를_쓴다() throws Exception {
        mapEntry.set(Optional.empty());
        listener.onPartitionsAssigned(partitions(5));
        requests.drainTo(engine);
        assignment.confirmAcquired(5);

        listener.onPartitionsRevoked(partitions(5));

        assertThat(mapProducer.history()).extracting(record -> record.value()).contains(OWN + ";moving");
        assertThat(handoverCancelled).isEmpty();
    }

    private void awaitCleaned(int count) throws InterruptedException {
        long deadline = System.nanoTime() + 3_000_000_000L;
        while (cleanedSlots.isEmpty() && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
    }

    private void awaitJournal(int size) throws InterruptedException {
        long deadline = System.nanoTime() + 3_000_000_000L;
        while (engine.journal().entries().size() < size && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        assertThat(engine.journal().entries().size()).isGreaterThanOrEqualTo(size);
    }

    private static List<TopicPartition> partitions(int... slots) {
        return java.util.Arrays.stream(slots).mapToObj(slot -> new TopicPartition("assignment-topic", slot)).toList();
    }
}
