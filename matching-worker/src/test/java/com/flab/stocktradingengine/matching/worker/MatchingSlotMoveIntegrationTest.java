package com.flab.stocktradingengine.matching.worker;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BooleanSupplier;
import java.util.function.IntConsumer;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import org.agrona.concurrent.UnsafeBuffer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.RemoveMembersFromConsumerGroupOptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;

import com.flab.stocktradingengine.aeron.AccountDestinationResolver;
import com.flab.stocktradingengine.aeron.AeronStreamIds;
import com.flab.stocktradingengine.aeron.MatchingSlotTarget;
import com.flab.stocktradingengine.aeron.StockSlotHasher;
import com.flab.stocktradingengine.codec.EventType;
import com.flab.stocktradingengine.codec.JournaledOrder;
import com.flab.stocktradingengine.codec.OrderCodec;
import com.flab.stocktradingengine.matching.disruptor.engine.MatchingEngine;
import com.flab.stocktradingengine.matching.disruptor.io.MatchListener;
import com.flab.stocktradingengine.matching.disruptor.io.SlotEventListener;
import com.flab.stocktradingengine.matching.worker.config.RejectReplyPublications;
import com.flab.stocktradingengine.matching.worker.coordination.KafkaMatchingShardAssignment;
import com.flab.stocktradingengine.matching.worker.coordination.MatchingSlotMapReader;
import com.flab.stocktradingengine.matching.worker.handover.HandoverBookStore;
import com.flab.stocktradingengine.matching.worker.messaging.RejectReplyPublisher;
import com.flab.stocktradingengine.matching.worker.testsupport.MapTopicHistory;
import com.flab.stocktradingengine.trading.entity.OrderSide;
import com.flab.stocktradingengine.trading.matching.FillResult;
import com.flab.stocktradingengine.trading.matching.OrderBook;

import io.aeron.Aeron;
import io.aeron.Publication;

/**
 * 종목 slot 이동의 통합 시험(U15·U16 전체 경로). 한 JVM에서 matching-worker 컨텍스트 둘(이전 담당 A, 새 담당 B: 신원 이름·주문 입구
 * 포트·Archive 제어 포트·Aeron 디렉터리·호가창 이동 주소가 모두 다르다)을 실제 Kafka, Aeron, Archive로 기동한다.
 * A가 종목 slot 64개를 모두 맡아 매도 호가를 쌓고, B가 조인해 협력 배정으로 일부 종목 slot이 옮겨 가는 동안과 그 뒤를 단언한다.
 *
 * <p>브로커 주소는 환경변수 KAFKA_BOOTSTRAP(기본 localhost:9092)이다. 조정 토픽과 그룹은 앱이 고정 이름을 쓰므로 시험 앞뒤로 비운다.
 * 시간 값은 이 한 대 로컬(임베디드 Archive, 단일 브로커)에서 잰 값이며 운영 값이 아니다.</p>
 */
class MatchingSlotMoveIntegrationTest {

    private static final String BOOTSTRAP = System.getenv().getOrDefault("KAFKA_BOOTSTRAP", "localhost:9092");
    private static final StockSlotHasher HASHER = new StockSlotHasher(64);
    private static final int PORT_A = 20750;
    private static final int PORT_B = 20751;
    private static final String CHANNEL_A = "aeron:udp?endpoint=localhost:" + PORT_A;
    private static final String CHANNEL_B = "aeron:udp?endpoint=localhost:" + PORT_B;
    private static final String GROUP_ID = KafkaMatchingShardAssignment.GROUP_ID;
    private static final long WAIT_NANOS = 120_000_000_000L;

    private final OrderCodec codec = new OrderCodec();
    private final java.util.concurrent.atomic.AtomicLong probeIdCounter = new java.util.concurrent.atomic.AtomicLong(8_000L);
    private ConfigurableApplicationContext podA;
    private ConfigurableApplicationContext podB;

    @TempDir
    Path dirA;
    @TempDir
    Path dirB;
    @TempDir
    Path dirW;

    // ---- 시험용 기록 장치 -------------------------------------------------------------------------------------

    record Event(long atNanos, String kind, int stockSlot) {
    }

    /** 컨텍스트 하나(매칭 pod 하나)에서 일어난 일을 시각과 함께 기록한다. */
    static final class PodRecorder {
        final List<Event> events = new CopyOnWriteArrayList<>();
        final List<FillResult> fills = new CopyOnWriteArrayList<>();
        final List<JournaledOrder> rejected = new CopyOnWriteArrayList<>();
        // 넘김 이벤트가 내보낸 종목별 호가창 바이트를 종목 slot별로 모아 둔다(시험이 보관소에서 지웠다가 되돌릴 때 쓴다).
        final Map<Integer, List<byte[]>> handedOverBytes = new java.util.concurrent.ConcurrentHashMap<>();
        // 넘김 이벤트를 처리한 직후 불리는 시험용 훅(엔진 thread에서 불린다).
        volatile IntConsumer afterHandedOver;

        void record(String kind, int stockSlot) {
            events.add(new Event(System.nanoTime(), kind, stockSlot));
        }

        Event first(String kind, int stockSlot) {
            return events.stream().filter(e -> e.kind().equals(kind) && e.stockSlot() == stockSlot).findFirst().orElse(null);
        }
    }

    /** 앱이 만든 SlotEventListener와 엔진에 주입되는 MatchListener를 같은 동작 그대로 감싸 기록한다. */
    static final class RecordingPostProcessor implements BeanPostProcessor {
        final PodRecorder recorder = new PodRecorder();

        @Override
        public Object postProcessAfterInitialization(Object bean, String beanName) {
            if (bean instanceof SlotEventListener delegate && beanName.equals("matchingSlotEventListener")) {
                return new SlotEventListener() {
                    @Override
                    public void onSlotAcquiredJournaled(int stockSlot) {
                        delegate.onSlotAcquiredJournaled(stockSlot);
                        recorder.record("acquired", stockSlot);
                    }

                    @Override
                    public void onSlotHandedOver(int stockSlot, List<byte[]> bookBytesPerStock) {
                        delegate.onSlotHandedOver(stockSlot, bookBytesPerStock);
                        recorder.handedOverBytes.put(stockSlot, bookBytesPerStock);
                        recorder.record("handedOver", stockSlot);
                        IntConsumer hook = recorder.afterHandedOver;
                        if (hook != null) {
                            hook.accept(stockSlot);
                        }
                    }

                    @Override
                    public void onBooksLoadedDurable(long transferId, int stockSlot) {
                        recorder.record("loadedDurable", stockSlot);
                        delegate.onBooksLoadedDurable(transferId, stockSlot);
                    }

                    @Override
                    public void onSlotCleanedUp(int stockSlot) {
                        delegate.onSlotCleanedUp(stockSlot);
                        recorder.record("cleanedUp", stockSlot);
                    }
                };
            }
            if (bean instanceof MatchListener delegate && beanName.equals("orderbookRecordingMatchListener")) {
                return new MatchListener() {
                    @Override
                    public void onFill(String stockCode, FillResult fill) {
                        recorder.fills.add(fill);
                        delegate.onFill(stockCode, fill);
                    }

                    @Override
                    public void onFill(String stockCode, FillResult fill, long emittedAtNanos, long aggressorOriginNanos) {
                        recorder.fills.add(fill);
                        delegate.onFill(stockCode, fill, emittedAtNanos, aggressorOriginNanos);
                    }

                    @Override
                    public void onBookChanged(String stockCode, OrderBook book) {
                        delegate.onBookChanged(stockCode, book);
                    }
                };
            }
            return bean;
        }
    }

    /** 담당이 아니라 거부한 주문을 기록한다(원래 동작은 그대로 이어서 한다). */
    static final class RecordingRejectPublisher extends RejectReplyPublisher {
        private final PodRecorder recorder;

        RecordingRejectPublisher(AccountDestinationResolver resolver, RejectReplyPublications publications, PodRecorder recorder) {
            super(resolver, publications.byEndpoint());
            this.recorder = recorder;
        }

        @Override
        public void onNotOwned(JournaledOrder order) {
            recorder.rejected.add(order);
            super.onNotOwned(order);
        }
    }

    /** 맵 읽기가 맵 토픽의 새 레코드를 받지 못하게 붙잡아 두는 장치(이전 담당의 이동 중 값이 늦게 도착하는 상황을 만든다). */
    static final class DelayControl {
        final AtomicBoolean hold = new AtomicBoolean();
        // 맵 읽기의 시작 단계(초기 읽기 기다림, 30초 한도)가 끝난 뒤에 붙잡기 시작하도록, 맵 읽기가 만들어진 뒤 이 시각부터 효력이 생긴다.
        volatile long activeFromNanos = Long.MAX_VALUE;

        boolean holding() {
            return hold.get() && System.nanoTime() >= activeFromNanos;
        }
    }

    /** 다음에 만들어지는 컨텍스트 하나의 맵 읽기에만 적용한다. */
    static volatile DelayControl delayForNextPod;

    @SuppressWarnings("unchecked")
    static org.apache.kafka.clients.consumer.Consumer<Integer, String> delaying(
            org.apache.kafka.clients.consumer.Consumer<Integer, String> real, DelayControl control) {
        return (org.apache.kafka.clients.consumer.Consumer<Integer, String>) Proxy.newProxyInstance(
            org.apache.kafka.clients.consumer.Consumer.class.getClassLoader(),
            new Class<?>[] {org.apache.kafka.clients.consumer.Consumer.class},
            (proxy, method, args) -> {
                if (method.getName().equals("poll") && control.holding()) {
                    Thread.sleep(20);
                    return ConsumerRecords.empty();
                }
                try {
                    return method.invoke(real, args);
                } catch (InvocationTargetException e) {
                    throw e.getCause();
                }
            });
    }

    @TestConfiguration
    static class PodRecording {
        @Bean
        MatchingSlotMapReader matchingSlotMapReader(
                StockSlotHasher stockSlotHasher, @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {
            Properties props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            org.apache.kafka.clients.consumer.Consumer<Integer, String> real = new KafkaConsumer<>(props);
            DelayControl control = delayForNextPod;
            delayForNextPod = null;
            if (control != null) {
                control.activeFromNanos = System.nanoTime() + 2_500_000_000L;
            }
            return new MatchingSlotMapReader(control == null ? real : delaying(real, control),
                stockSlotHasher.stockSlotCount(), KafkaMatchingShardAssignment.MAP_TOPIC);
        }

        @Bean
        static RecordingPostProcessor recordingPostProcessor() {
            return new RecordingPostProcessor();
        }

        @Bean(initMethod = "start", destroyMethod = "close")
        RejectReplyPublisher rejectReplyPublisher(
                AccountDestinationResolver resolver, RejectReplyPublications publications, RecordingPostProcessor processor) {
            return new RecordingRejectPublisher(resolver, publications, processor.recorder);
        }
    }

    // ---- 시험 --------------------------------------------------------------------------------------------------

    @BeforeEach
    void cleanKafkaState() throws Exception {
        clearKafkaState();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (podB != null) {
            podB.close();
        }
        if (podA != null) {
            podA.close();
        }
        clearKafkaState();
    }

    @Test
    void 둘째_pod가_조인하면_옮겨_가는_종목_slot의_호가창이_요청과_응답으로_넘어가고_맵_전환_뒤_이전_담당이_지운다() throws Exception {
        // (1) 첫 pod만 기동해 종목 slot 64개를 모두 맡고, 여러 종목 slot에 매도 호가를 쌓는다.
        podA = startPod("slotmove-0", PORT_A, 8021, dirA);
        PodRecorder recA = podA.getBean(RecordingPostProcessor.class).recorder;
        KafkaMatchingShardAssignment assignmentA = podA.getBean(KafkaMatchingShardAssignment.class);
        MatchingEngine engineA = podA.getBean(MatchingEngine.class);
        MatchingSlotMapReader mapReaderA = podA.getBean(MatchingSlotMapReader.class);
        HandoverBookStore storeA = podA.getBean(HandoverBookStore.class);
        awaitUntil(() -> assignmentA.currentSlotsSnapshot().size() == 64, "A가 종목 slot 64개를 맡지 못함");

        List<String> stocks = stocksInDistinctSlots(24);
        Map<String, Long> firstSellIdByStock = new HashMap<>();
        Publication toA = podA.getBean(Aeron.class).addPublication(CHANNEL_A, AeronStreamIds.MATCHING_INTAKE);
        awaitUntil(toA::isConnected, "A의 주문 입구에 연결되지 않음");
        int sessionA = toA.sessionId();
        long orderId = 1L;
        for (String stock : stocks) {
            firstSellIdByStock.put(stock, orderId);
            send(toA, sell(orderId++, stock, 10_000, 5));
            send(toA, sell(orderId++, stock, 10_100, 7));
        }
        long lastPosition = toA.position();
        awaitUntil(() -> Long.valueOf(lastPosition).equals(engineA.lastAppliedOrderIntakePositions().get(sessionA)),
            "A가 주문을 모두 반영하지 못함");

        // 맵 전환 시각을 잡으려고 B가 뜨기 전에 A의 맵 읽기에 관찰자를 건다.
        Map<Integer, Long> mapSwitchedAt = new java.util.concurrent.ConcurrentHashMap<>();
        mapReaderA.addListener((slot, target) -> {
            if (target.isPresent() && !target.get().moving() && target.get().endpoint().equals(CHANNEL_B)) {
                mapSwitchedAt.putIfAbsent(slot, System.nanoTime());
            }
        });

        // (2) 둘째 pod를 조인시킨다. B의 맵 읽기를 붙잡아, 이전 담당(A)이 쓴 이동 중 값이 B에 늦게 도착하는 상황을 만든다.
        DelayControl delay = new DelayControl();
        delay.hold.set(true);
        delayForNextPod = delay;
        long joinStartedAt = System.nanoTime();
        podB = startPod("slotmove-1", PORT_B, 8022, dirB);
        long podBUpAt = System.nanoTime();
        PodRecorder recB = podB.getBean(RecordingPostProcessor.class).recorder;
        KafkaMatchingShardAssignment assignmentB = podB.getBean(KafkaMatchingShardAssignment.class);
        Set<Integer> orderSlots = new HashSet<>();
        stocks.forEach(stock -> orderSlots.add(HASHER.slotFor(stock)));
        Map<Integer, String> stockBySlot = new HashMap<>();
        stocks.forEach(stock -> stockBySlot.put(HASHER.slotFor(stock), stock));

        // 이동 도중 도착하는 주문: A가 회수했고 B가 아직 켜지지 않은 구간(B의 맵 읽기를 붙잡아 둔 동안)에 보낸다.
        Set<Integer> probedInWindow = new HashSet<>();
        Map<Integer, Long> probeIdBySlot = new HashMap<>();
        long probeOrderId = 8_000L;
        awaitUntil(() -> {
            // B가 배정을 받아 판정을 기다리는 상태가 될 때까지 기다리며, 그 사이 A가 회수한 종목 slot에 주문을 보낸다.
            Set<Integer> ownedA = assignmentA.currentSlotsSnapshot();
            for (int slot : orderSlots) {
                if (!ownedA.contains(slot) && !probedInWindow.contains(slot) && recA.first("handedOver", slot) != null) {
                    probedInWindow.add(slot);
                    probeIdBySlot.put(slot, probeIdCounter.incrementAndGet());
                    send(toA, sell(probeIdBySlot.get(slot), stockBySlot.get(slot), 10_200, 3));
                }
            }
            return !assignmentB.slotsAwaitingDecision().isEmpty();
        }, "B가 배정을 받아 판정을 기다리는 상태가 되지 않음");

        // 보정 1: 맵 읽기가 이전 담당의 이동 중 값을 아직 못 받은 동안 B는 첫 배정으로 가정하지 않는다 — 담당을 켜지 않고, 맵에 자기 주소를 올리지 않는다.
        Set<Integer> decidingSlots = assignmentB.slotsAwaitingDecision();
        assertThat(assignmentB.currentSlotsSnapshot()).as("판정을 기다리는 동안 담당이 아니다").isEmpty();
        assertThat(recB.events.stream().filter(e -> e.kind().equals("acquired") || e.kind().equals("loadedDurable")).toList())
            .as("판정을 기다리는 동안 받음도 적재도 없다").isEmpty();
        assertThat(mapSwitchedAt).as("판정을 기다리는 동안 맵에 B 주소가 올라가지 않았다").isEmpty();
        for (int slot : decidingSlots) {
            assertThat(mapReaderA.targetFor(slot).map(MatchingSlotTarget::moving).orElse(false))
                .as("A가 쓴 이동 중 값이 맵에 남아 있다: 종목 slot=" + slot).isTrue();
        }
        long heldAt = System.nanoTime();
        delay.hold.set(false);

        long deadline = System.nanoTime() + WAIT_NANOS;
        while (System.nanoTime() < deadline) {
            Set<Integer> ownedA = assignmentA.currentSlotsSnapshot();
            Set<Integer> ownedB = assignmentB.currentSlotsSnapshot();
            boolean allMovedCleaned = !ownedB.isEmpty() && ownedA.size() + ownedB.size() == 64
                && ownedB.stream().allMatch(slot -> recA.first("cleanedUp", slot) != null);
            if (allMovedCleaned) {
                break;
            }
            Thread.sleep(2);
        }
        Set<Integer> movedSlots = assignmentB.currentSlotsSnapshot();
        assertThat(movedSlots).as("B가 옮겨 받은 종목 slot이 있어야 한다").isNotEmpty();
        assertThat(assignmentA.currentSlotsSnapshot().size() + movedSlots.size()).isEqualTo(64);
        assertThat(movedSlots).as("A가 아직 맡은 종목 slot과 겹치면 안 된다")
            .doesNotContainAnyElementsOf(assignmentA.currentSlotsSnapshot());

        // (3) 옮겨진 종목 slot 중 주문이 쌓인 종목을 고른다.
        int movedOrderSlot = movedSlots.stream().filter(orderSlots::contains).sorted().findFirst().orElseThrow(
            () -> new AssertionError("옮겨진 종목 slot 중 주문을 쌓은 종목이 없음: moved=" + movedSlots + " orderSlots=" + orderSlots));
        String movedStock = stockBySlot.get(movedOrderSlot);
        long sellOne = firstSellIdByStock.get(movedStock);

        // 단언 1: 시점 순서. B의 적재 확정 → 담당 켜짐(저널·Archive 기록 확인) → 맵 전환 → A의 정리.
        Event loaded = recB.first("loadedDurable", movedOrderSlot);
        Event acquired = recB.first("acquired", movedOrderSlot);
        Long switched = mapSwitchedAt.get(movedOrderSlot);
        Event cleaned = recA.first("cleanedUp", movedOrderSlot);
        Event handedOver = recA.first("handedOver", movedOrderSlot);
        assertThat(handedOver).as("A가 넘김 이벤트를 처리함").isNotNull();
        assertThat(loaded).as("B가 적재 확정을 알림").isNotNull();
        assertThat(acquired).as("B가 받음 항목 기록 확인 뒤 담당을 켬").isNotNull();
        assertThat(switched).as("맵이 B 주소의 평상시 값으로 바뀜").isNotNull();
        assertThat(cleaned).as("A가 맵 전환을 보고 정리함").isNotNull();
        assertThat(handedOver.atNanos()).isLessThanOrEqualTo(loaded.atNanos());
        assertThat(loaded.atNanos()).isLessThanOrEqualTo(acquired.atNanos());
        assertThat(acquired.atNanos()).isLessThanOrEqualTo(switched);
        assertThat(switched).isLessThanOrEqualTo(cleaned.atNanos());

        // 단언 2: A는 그 종목 slot의 호가창과 보관을 지웠다.
        assertThat(storeA.get(movedOrderSlot)).isEmpty();
        assertThat(engineA.containsOrder(movedStock, sellOne)).as("A의 호가창에서 지워짐").isFalse();
        assertThat(mapReaderA.targetFor(movedOrderSlot)).contains(new MatchingSlotTarget(CHANNEL_B, false));

        // 단언 3: B에 같은 호가창이 적재되었다 — 교차하는 매수를 보내면 A에 쌓았던 가격과 수량 그대로 체결된다.
        Publication toB = podB.getBean(Aeron.class).addPublication(CHANNEL_B, AeronStreamIds.MATCHING_INTAKE);
        awaitUntil(toB::isConnected, "B의 주문 입구에 연결되지 않음");
        send(toB, buy(9_001L, movedStock, 10_100, 12));
        awaitUntil(() -> recB.fills.stream().filter(fill -> fill.buyOrderId() == 9_001L).count() >= 2, "B에서 체결 둘이 나오지 않음");
        List<FillResult> fillsOfBuy = recB.fills.stream().filter(fill -> fill.buyOrderId() == 9_001L).toList();
        assertThat(fillsOfBuy).extracting(FillResult::sellOrderId).containsExactly(sellOne, sellOne + 1);
        assertThat(fillsOfBuy).extracting(FillResult::filledQuantity).containsExactly(5, 7);
        assertThat(fillsOfBuy.stream().map(FillResult::matchPrice).map(BigDecimal::intValueExact).toList())
            .containsExactly(10_000, 10_100);

        // 단언 4: 이동 도중(또는 회수 뒤) 이전 담당에 도착한 그 종목 slot의 주문은 거부 회신으로 돌아가고 호가창에 들어가지 않는다.
        long rejectedBefore = recA.rejected.size();
        long lateProbeId = 8_900L;
        send(toA, sell(lateProbeId, movedStock, 10_300, 2));
        awaitUntil(() -> recA.rejected.stream().anyMatch(order -> order.orderId() == lateProbeId), "회수 뒤 도착한 주문이 거부되지 않음");
        assertThat(engineA.containsOrder(movedStock, lateProbeId)).isFalse();
        assertThat(probeIdBySlot).as("B가 판정을 기다리는 동안 A가 회수한 종목 slot에 주문을 보냈다").isNotEmpty();
        awaitUntil(() -> probeIdBySlot.values().stream().allMatch(
                id -> recA.rejected.stream().anyMatch(order -> order.orderId() == id)),
            "이동 도중 도착한 주문이 모두 거부되지 않음");
        for (long id : probeIdBySlot.values()) {
            assertThat(engineA.containsOrder(stockBySlot.get(probeIdBySlot.entrySet().stream()
                .filter(entry -> entry.getValue() == id).findFirst().orElseThrow().getKey()), id)).isFalse();
        }
        boolean windowProbeRejected = probeIdBySlot.entrySet().stream().anyMatch(entry -> entry.getKey() == movedOrderSlot
            && recA.rejected.stream().anyMatch(order -> order.orderId() == entry.getValue()));

        long ms = 1_000_000L;
        System.out.println("SLOT-MOVE-TIMING (한 대 로컬, 임베디드 Archive·단일 브로커 값이며 운영 값이 아니다) 옮겨진 종목 slot 수=" + movedSlots.size()
            + " 선택 종목 slot=" + movedOrderSlot
            + " | B 컨텍스트 기동까지(ms)=" + (podBUpAt - joinStartedAt) / ms
            + " | 붙잡은 맵 읽기를 풀기까지(조인 시작 기준, ms)=" + (heldAt - joinStartedAt) / ms
            + " | 풀고 나서 B 적재 확정까지(ms)=" + (loaded.atNanos() - heldAt) / ms
            + " | 조인 시작→A 넘김 처리(ms)=" + (handedOver.atNanos() - joinStartedAt) / ms
            + " | 넘김→B 적재 확정(ms)=" + (loaded.atNanos() - handedOver.atNanos()) / ms
            + " | 적재 확정→B 담당 켜짐(ms)=" + (acquired.atNanos() - loaded.atNanos()) / ms
            + " | 담당 켜짐→A가 맵 전환을 봄(ms)=" + (switched - acquired.atNanos()) / ms
            + " | 맵 전환→A 정리(ms)=" + (cleaned.atNanos() - switched) / ms
            + " | 넘김→정리 전체(ms)=" + (cleaned.atNanos() - handedOver.atNanos()) / ms
            + " | 이동 도중 구간에 보낸 주문 거부 확인=" + windowProbeRejected + " 구간에서 보낸 종목 slot 수=" + probedInWindow.size()
            + " | 회수 뒤 보낸 주문 거부 수 증가=" + (recA.rejected.size() - rejectedBefore));
    }

    /** 이전 담당 A를 띄우고 여러 종목 slot에 매도 호가를 쌓은 상태. */
    private record PodASetup(PodRecorder recorder, KafkaMatchingShardAssignment assignment, MatchingEngine engine,
                            MatchingSlotMapReader mapReader, HandoverBookStore store, Publication toA,
                            List<String> stocks, Map<String, Long> firstSellIdByStock) {
    }

    private PodASetup startPodAWithOrders() throws Exception {
        podA = startPod("slotmove-0", PORT_A, 8021, dirA);
        PodRecorder recA = podA.getBean(RecordingPostProcessor.class).recorder;
        KafkaMatchingShardAssignment assignmentA = podA.getBean(KafkaMatchingShardAssignment.class);
        MatchingEngine engineA = podA.getBean(MatchingEngine.class);
        awaitUntil(() -> assignmentA.currentSlotsSnapshot().size() == 64, "A가 종목 slot 64개를 맡지 못함");
        List<String> stocks = stocksInDistinctSlots(24);
        Map<String, Long> firstSellIdByStock = new HashMap<>();
        Publication toA = podA.getBean(Aeron.class).addPublication(CHANNEL_A, AeronStreamIds.MATCHING_INTAKE);
        awaitUntil(toA::isConnected, "A의 주문 입구에 연결되지 않음");
        int sessionA = toA.sessionId();
        long orderId = 1L;
        for (String stock : stocks) {
            firstSellIdByStock.put(stock, orderId);
            send(toA, sell(orderId++, stock, 10_000, 5));
            send(toA, sell(orderId++, stock, 10_100, 7));
        }
        long lastPosition = toA.position();
        awaitUntil(() -> Long.valueOf(lastPosition).equals(engineA.lastAppliedOrderIntakePositions().get(sessionA)),
            "A가 주문을 모두 반영하지 못함");
        return new PodASetup(recA, assignmentA, engineA, podA.getBean(MatchingSlotMapReader.class),
            podA.getBean(HandoverBookStore.class), toA, stocks, firstSellIdByStock);
    }

    @Test
    void 호가창_적재_전에_회수되면_맵을_덮어쓰지_않아_다음_담당이_이전_담당에게_다시_요청해_이동을_마친다() throws Exception {
        PodASetup a = startPodAWithOrders();
        PodRecorder recA = a.recorder();
        // A가 넘김을 처리한 직후 보관한 바이트를 지워, B의 요청에 응답하지 못하게 한다(B가 호가창을 기다리는 상태를 만든다).
        recA.afterHandedOver = a.store()::remove;

        podB = startPod("slotmove-1", PORT_B, 8022, dirB);
        KafkaMatchingShardAssignment assignmentB = podB.getBean(KafkaMatchingShardAssignment.class);
        awaitUntil(() -> !assignmentB.slotsAwaitingLoad().isEmpty(), "B가 호가창을 기다리는 상태가 되지 않음");
        Thread.sleep(500); // 옮겨 오는 종목 slot이 모두 B에 배정될 때까지
        Set<Integer> waitingSlots = assignmentB.slotsAwaitingLoad();
        assertThat(assignmentB.currentSlotsSnapshot()).as("호가창을 기다리는 동안 B는 담당이 아니다").isEmpty();
        for (int slot : waitingSlots) {
            assertThat(a.mapReader().targetFor(slot)).as("호가창을 기다리는 동안 맵은 A의 이동 중 값이다: " + slot)
                .contains(new MatchingSlotTarget(CHANNEL_A, true));
        }

        // B를 닫는다. 담당이 되기 전에 회수되므로 맵을 쓰지 않아야 한다.
        podB.close();
        podB = null;
        Thread.sleep(700);
        for (int slot : waitingSlots) {
            assertThat(a.mapReader().targetFor(slot)).as("회수 뒤에도 맵은 A의 이동 중 값 그대로다(B 주소로 덮이지 않음): " + slot)
                .contains(new MatchingSlotTarget(CHANNEL_A, true));
        }

        // A의 보관을 되돌리고, 같은 신원의 새 pod(W)를 띄워 같은 종목 slot을 다시 배정받게 한다. W는 맵의 A 이동 중 값을 보고 A에 요청한다.
        recA.afterHandedOver = null;
        waitingSlots.forEach(slot -> a.store().put(slot, recA.handedOverBytes.get(slot)));
        podB = startPod("slotmove-1", PORT_B, 8023, dirW);
        PodRecorder recW = podB.getBean(RecordingPostProcessor.class).recorder;
        KafkaMatchingShardAssignment assignmentW = podB.getBean(KafkaMatchingShardAssignment.class);
        awaitUntil(() -> assignmentW.currentSlotsSnapshot().containsAll(waitingSlots)
            && waitingSlots.stream().allMatch(slot -> recA.first("cleanedUp", slot) != null),
            "W가 종목 slot을 넘겨받아 A가 정리하는 데 이르지 못함: W=" + assignmentW.currentSlotsSnapshot() + " 기다리던=" + waitingSlots);

        int slot = waitingSlots.stream().filter(candidate -> a.stocks().stream().anyMatch(stock -> HASHER.slotFor(stock) == candidate))
            .sorted().findFirst().orElseThrow(() -> new AssertionError("기다리던 종목 slot 중 주문을 쌓은 종목이 없음"));
        String movedStock = a.stocks().stream().filter(stock -> HASHER.slotFor(stock) == slot).findFirst().orElseThrow();
        long sellOne = a.firstSellIdByStock().get(movedStock);
        assertThat(a.mapReader().targetFor(slot)).contains(new MatchingSlotTarget(CHANNEL_B, false));
        assertThat(a.store().get(slot)).isEmpty();
        assertThat(recW.first("loadedDurable", slot)).isNotNull();

        Publication toW = podB.getBean(Aeron.class).addPublication(CHANNEL_B, AeronStreamIds.MATCHING_INTAKE);
        awaitUntil(toW::isConnected, "W의 주문 입구에 연결되지 않음");
        send(toW, buy(9_501L, movedStock, 10_100, 12));
        awaitUntil(() -> recW.fills.stream().filter(fill -> fill.buyOrderId() == 9_501L).count() >= 2, "W에서 체결 둘이 나오지 않음");
        assertThat(recW.fills.stream().filter(fill -> fill.buyOrderId() == 9_501L).map(FillResult::sellOrderId).toList())
            .containsExactly(sellOne, sellOne + 1);
    }

    /**
     * U17a. 운영 종료 경로(Spring 컨텍스트 종료가 배정 생명주기의 stop()을 거쳐 close()를 부른다)로 컨슈머를 닫으면, 맡고 있던 종목 slot
     * 전체에 회수 콜백이 불려 맵 토픽에 {@code 주소;moving}이 한 번씩 올라온다. close() 주석이 적은 동작이고 지금까지 시험이 없었다.
     * 이 시험이 실패하면(맵 값이 평상시에 머물면) 운영 종료 때 맵이 이동 중으로 바뀌지 않아 다른 pod의 판정이 달라진다.
     */
    @Test
    void 컨텍스트를_닫으면_맡은_종목_slot_전체의_맵에_이동_중_표시가_올라온다() {
        podA = startPod("slotmove-0", PORT_A, 8021, dirA);
        KafkaMatchingShardAssignment assignmentA = podA.getBean(KafkaMatchingShardAssignment.class);
        awaitUntil(() -> assignmentA.currentSlotsSnapshot().size() == 64, "A가 종목 slot 64개를 맡지 못함");
        Map<Integer, List<String>> before = awaitMapTopic(history -> history.size() == 64
            && history.values().stream().allMatch(values -> values.equals(List.of(CHANNEL_A))));
        assertThat(before).as("닫기 전에는 종목 slot마다 평상시 값 하나뿐이다").hasSize(64);

        podA.close();
        podA = null;

        String moving = new MatchingSlotTarget(CHANNEL_A, true).encode();
        Map<Integer, List<String>> after = awaitMapTopic(history -> history.size() == 64
            && history.values().stream().allMatch(values -> values.size() >= 2 && values.get(values.size() - 1).equals(moving)));
        for (int slot = 0; slot < 64; slot++) {
            assertThat(after.get(slot)).as("종목 slot=" + slot).containsExactly(CHANNEL_A, moving);
        }
    }

    // ---- 도우미 ------------------------------------------------------------------------------------------------

    private ConfigurableApplicationContext startPod(String instanceId, int port, int archiveControlPort, Path archiveDir) {
        return new SpringApplicationBuilder(MatchingWorkerApplication.class, PodRecording.class)
            .web(WebApplicationType.NONE)
            .properties(
                "matching.worker.archive-dir=" + archiveDir.toAbsolutePath(),
                "matching.worker.archive.control-channel=aeron:udp?endpoint=localhost:" + archiveControlPort,
                "matching-shard.coordination.enabled=true",
                "matching-worker.instance-id=" + instanceId,
                "transport.matching-intake.channel=aeron:udp?endpoint=localhost:" + port,
                "matching-shard.handover.endpoints[0]=" + CHANNEL_A,
                "matching-shard.handover.endpoints[1]=" + CHANNEL_B,
                "spring.main.allow-bean-definition-overriding=true")
            // application.yml의 spring.kafka.bootstrap-servers를 덮어야 해서 명령행 인자(가장 높은 우선순위)로 준다.
            .run("--spring.kafka.bootstrap-servers=" + BOOTSTRAP);
    }

    private List<String> stocksInDistinctSlots(int count) {
        List<String> stocks = new ArrayList<>();
        Set<Integer> usedSlots = new HashSet<>();
        for (int code = 1; stocks.size() < count && code < 10_000; code++) {
            String candidate = String.format("%06d", code);
            if (usedSlots.add(HASHER.slotFor(candidate))) {
                stocks.add(candidate);
            }
        }
        return stocks;
    }

    private JournaledOrder sell(long orderId, String stock, int price, int quantity) {
        return new JournaledOrder(EventType.PLACE, orderId, 1_000L + orderId, stock, OrderSide.SELL,
            new BigDecimal(price), quantity, Instant.now());
    }

    private JournaledOrder buy(long orderId, String stock, int price, int quantity) {
        return new JournaledOrder(EventType.PLACE, orderId, 2_000L + orderId, stock, OrderSide.BUY,
            new BigDecimal(price), quantity, Instant.now());
    }

    private void send(Publication publication, JournaledOrder order) {
        UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(256));
        int length = codec.encode(buffer, 0, order);
        awaitUntil(() -> publication.offer(buffer, 0, length) > 0, "주문 발행 실패: " + order.orderId());
    }

    /** 맵 토픽을 처음부터 다시 읽는 일을 조건이 참이 될 때까지 되풀이한다(닫힌 pod 안의 맵 읽기를 쓸 수 없어 별도 consumer로 읽는다). */
    private Map<Integer, List<String>> awaitMapTopic(java.util.function.Predicate<Map<Integer, List<String>>> condition) {
        long deadline = System.nanoTime() + 30_000_000_000L;
        Map<Integer, List<String>> history = MapTopicHistory.read(BOOTSTRAP, KafkaMatchingShardAssignment.MAP_TOPIC);
        while (!condition.test(history)) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("30초 안에 맵 토픽이 기대한 상태가 되지 않음: " + history);
            }
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError("맵 토픽 대기 중 인터럽트됨", e);
            }
            history = MapTopicHistory.read(BOOTSTRAP, KafkaMatchingShardAssignment.MAP_TOPIC);
        }
        return history;
    }

    private static void awaitUntil(BooleanSupplier condition, String failureMessage) {
        long deadline = System.nanoTime() + WAIT_NANOS;
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("120초 안에 조건이 충족되지 않음: " + failureMessage);
            }
            try {
                Thread.sleep(2);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError(failureMessage, e);
            }
        }
    }

    /** 고정 이름의 조정 토픽·맵 토픽을 지우고 그룹의 정적 멤버를 모두 내보낸다(이전 시험이나 실행이 남긴 상태를 없앤다). */
    private void clearKafkaState() throws Exception {
        try (Admin admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP))) {
            try {
                admin.removeMembersFromConsumerGroup(GROUP_ID, new RemoveMembersFromConsumerGroupOptions()).all().get();
            } catch (Exception ignored) {
                // 그룹이 없으면 할 일이 없다.
            }
            try {
                admin.deleteTopics(List.of(KafkaMatchingShardAssignment.ASSIGNMENT_TOPIC, KafkaMatchingShardAssignment.MAP_TOPIC))
                    .all().get();
            } catch (Exception ignored) {
                // 토픽이 없으면 할 일이 없다.
            }
            long deadline = System.nanoTime() + 20_000_000_000L;
            while (System.nanoTime() < deadline) {
                Set<String> names = admin.listTopics().names().get();
                if (!names.contains(KafkaMatchingShardAssignment.ASSIGNMENT_TOPIC)
                    && !names.contains(KafkaMatchingShardAssignment.MAP_TOPIC)) {
                    return;
                }
                Thread.sleep(200);
            }
        }
    }
}
