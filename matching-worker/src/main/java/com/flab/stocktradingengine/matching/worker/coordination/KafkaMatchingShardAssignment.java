package com.flab.stocktradingengine.matching.worker.coordination;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.IntFunction;
import java.util.function.IntPredicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import com.flab.stocktradingengine.aeron.MatchingSlotTarget;
import com.flab.stocktradingengine.matching.disruptor.io.SlotEventRequests;

/**
 * 매칭 shard가 담당할 종목 slot을 Kafka 컨슈머 그룹 배정으로 받는다. 계좌 워커의
 * {@code KafkaShardAssignment}와 같은 방식이다: 조정용 토픽 {@link #ASSIGNMENT_TOPIC}에는 메시지를
 * 한 건도 보내지 않고, 이 매칭 shard가 그룹 {@link #GROUP_ID}에서 배정받은 파티션 번호 집합을 그대로
 * 담당 종목 slot 집합으로 쓴다.
 *
 * <h3>계좌 쪽과 다른 점</h3>
 * <ul>
 * <li>맵 토픽 {@link #MAP_TOPIC}의 값이 주문 입구 주소 하나다(계좌 쪽은 주문 주소와 체결 주소 둘).</li>
 * <li>첫 배정 이후 처음 배정에 없던 종목 slot을 거부하는 안전망을 두지 않는다. 증설로 종목 slot이
 *     옮겨 오는 것이 정상 경로이기 때문이다.</li>
 * <li>컨슈머가 {@link CooperativeStickyAssignor}로 배정을 받는다({@link #consumerProperties}). 지정하지
 *     않으면 기본값(RangeAssignor 우선)이 적용되어 새 멤버가 조인할 때 기존 멤버의 revoke에 64개
 *     전부가 온다. 이 경우 옮겨지지 않는 종목 slot까지 이동 중으로 취급되어 이동 절차가 성립하지
 *     않는다.</li>
 * </ul>
 *
 * <p>기동 직후부터 첫 배정을 받을 때까지 {@link #test}는 모든 종목 slot에서 false다.</p>
 *
 * <h3>담당이 켜지는 시점</h3>
 * <p>요청 통로({@link SlotEventRequests})를 받은 구성에서는 배정 콜백이 종목 slot을 바로 켜지 않는다. 받음 요청만 남기고,
 * 엔진이 받음 이벤트를 저널에 기록한 뒤 {@link #confirmAcquired}를 불러야 켜진다. 켜진 뒤에 들어온 주문은 저널에서 항상
 * 받음 항목 뒤에 있어서, 복구가 그 주문을 걸러내지 않는다(11-12절). 회수 콜백은 즉시 끄고 넘김 요청을 남긴다.
 * 요청 통로가 없는 구성은 배정 즉시 켠다.</p>
 */
public final class KafkaMatchingShardAssignment implements IntPredicate {

    private static final Logger log = System.getLogger(KafkaMatchingShardAssignment.class.getName());

    public static final String ASSIGNMENT_TOPIC = "matching-shard-assignment";
    public static final String GROUP_ID = "matching-shard-owners";
    public static final String MAP_TOPIC = "matching-shard-map";

    private final Consumer<String, String> consumer;
    private final Producer<Integer, String> mapProducer;
    private final Admin adminClient;
    private final int stockSlotCount;
    private final String ownOrderEndpoint;
    private final String assignmentTopic;
    private final String mapTopic;
    // 받음 요청을 남기는 통로. null이면 배정 콜백이 종목 slot을 바로 켠다.
    private final SlotEventRequests slotEventRequests;
    // 받음 요청을 남겼고 저널 기록 확인을 기다리는 종목 slot. 그 사이 회수되면 빠진다.
    private final java.util.Set<Integer> awaitingJournalConfirm = ConcurrentHashMap.newKeySet();
    // 맵을 읽어 이동으로 받는 종목 slot인지 판단하는 함수와 이전 담당에게 호가창을 요청하는 쪽. 둘 다 null이면 이동 처리를 하지 않는다.
    private final IntFunction<Optional<MatchingSlotTarget>> mapLookup;
    private final MovedSlotRequests movedSlotRequests;
    // 맵 읽기가 맵 토픽의 끝까지 따라올 때까지 기다리는 함수와 판정을 맡는 executor. mapSync가 null이면 기다리지 않고, executor가 null이면 콜백 thread에서 바로 판정한다.
    private final MapSync mapSync;
    private final java.util.concurrent.Executor decisionExecutor;
    // 배정을 받았고 이동으로 받는지 첫 배정인지 판정을 기다리는 종목 slot. 그 사이 회수되면 빠진다.
    private final java.util.Set<Integer> awaitingDecision = ConcurrentHashMap.newKeySet();
    // 이동으로 받는 종목 slot 중 호가창 적재가 끝나 받음 요청을 남겼고 담당이 켜지기를 기다리는 것. 그 사이 회수되면 맵을 쓰지 않는다.
    private final java.util.Set<Integer> movedInAcquiring = ConcurrentHashMap.newKeySet();
    // 이동으로 받기로 하고 호가창 적재와 스냅샷 확정을 기다리는 종목 slot. 그 사이 회수되면 빠진다.
    private final java.util.Set<Integer> awaitingLoad = ConcurrentHashMap.newKeySet();
    // 적재가 끝나 담당 표시가 켜지기를 기다리는, 이동으로 받은 종목 slot. 켜진 직후 맵을 자기 주소의 평상시 값으로 바꾼다(U16a).
    private final java.util.Set<Integer> awaitingMapSwitch = ConcurrentHashMap.newKeySet();
    // 종목 slot별 맵 쓰기 세대. 회수될 때마다 올려, 다시 보내려던 옛 쓰기가 나중에 이동 중 표시를 덮어쓰지 못하게 한다.
    private final java.util.concurrent.atomic.AtomicIntegerArray mapWriteGeneration;
    private volatile long mapWriteRetryMillis = 1_000L;
    // 콜백(회수·첫 배정)이 자기 맵 쓰기의 완료를 기다리는 한도. 넘기면 ERROR 로그를 남기고 콜백은 돌아간다(쓰기 thread는 계속 재시도한다).
    // 콜백은 Kafka poll 안에서 불리므로 오래 막히면 max.poll.interval.ms를 넘겨 그룹에서 쫓겨날 수 있다(11-8절 마감 D).
    private volatile long mapWriteDeadlineMillis = 30_000L;
    // 맵 쓰기는 전부 이 queue로 보내 쓰기 전용 thread 하나가 넣은 순서대로 처리한다. 순서가 queue 순서로 고정되고, 세대 확인은 그 thread가
    // 쓰기 직전에 한다. 호출 thread(Kafka 콜백, 주문 수신 thread)는 브로커 응답을 기다리지 않거나(기다려야 하면 완료 future로 기다린다) 락 안에서
    // send하지 않는다.
    private final java.util.concurrent.BlockingQueue<MapWrite> mapWriteQueue = new java.util.concurrent.LinkedBlockingQueue<>();
    private volatile Thread mapWriteThread;

    private record MapWrite(int stockSlot, String value, int generation, java.util.concurrent.CompletableFuture<Void> done) {
    }

    // test()가 주문마다 불린다. 배열 참조를 volatile로 통째로 교체해 박싱 없이 인덱스 조회만 한다.
    private volatile boolean[] ownedSlotFlags;
    private volatile boolean running;
    private Thread pollThread;

    public KafkaMatchingShardAssignment(Consumer<String, String> consumer, Producer<Integer, String> mapProducer,
                                        Admin adminClient, int stockSlotCount, String ownOrderEndpoint) {
        this(consumer, mapProducer, adminClient, stockSlotCount, ownOrderEndpoint, ASSIGNMENT_TOPIC, MAP_TOPIC);
    }

    /** 토픽 이름을 바꿀 수 있는 생성자. 서로 다른 시험이 같은 브로커를 쓸 때 토픽을 분리하는 데 쓴다. */
    public KafkaMatchingShardAssignment(Consumer<String, String> consumer, Producer<Integer, String> mapProducer,
                                        Admin adminClient, int stockSlotCount, String ownOrderEndpoint,
                                        String assignmentTopic, String mapTopic) {
        this(consumer, mapProducer, adminClient, stockSlotCount, ownOrderEndpoint, assignmentTopic, mapTopic, null);
    }

    /** 받음·넘김 요청 통로를 받는다. 종목 slot 담당은 {@link #confirmAcquired}가 와야 켜진다. */
    public KafkaMatchingShardAssignment(Consumer<String, String> consumer, Producer<Integer, String> mapProducer,
                                        Admin adminClient, int stockSlotCount, String ownOrderEndpoint,
                                        String assignmentTopic, String mapTopic, SlotEventRequests slotEventRequests) {
        this(consumer, mapProducer, adminClient, stockSlotCount, ownOrderEndpoint, assignmentTopic, mapTopic,
            slotEventRequests, null, null);
    }

    /**
     * 이동으로 받는 종목 slot을 처리하는 구성. 배정 콜백이 맵({@code mapLookup})에서 그 종목 slot의 항목을 읽어 {@code 주소;moving}이면
     * 이동으로 받는 것으로 본다.
     */
    public KafkaMatchingShardAssignment(Consumer<String, String> consumer, Producer<Integer, String> mapProducer,
                                        Admin adminClient, int stockSlotCount, String ownOrderEndpoint,
                                        String assignmentTopic, String mapTopic, SlotEventRequests slotEventRequests,
                                        IntFunction<Optional<MatchingSlotTarget>> mapLookup,
                                        MovedSlotRequests movedSlotRequests) {
        this(consumer, mapProducer, adminClient, stockSlotCount, ownOrderEndpoint, assignmentTopic, mapTopic,
            slotEventRequests, mapLookup, movedSlotRequests, null, null);
    }

    /**
     * @param mapSync 판정 전에 맵 읽기가 맵 토픽의 끝까지 따라올 때까지 기다리는 함수. 시간 안에 못 따라오면 false를 돌려주고, 그러면 첫
     *     배정으로 가정하지 않고 담당을 켜지 않은 채 ERROR 로그를 남긴다. null이면 기다리지 않는다.
     * @param decisionExecutor 판정을 실행할 executor. 배정 콜백은 판정 요청만 남기고 돌아간다(콜백 시간 상한에 들어가지 않는다).
     *     null이면 콜백 thread에서 바로 판정한다.
     */
    public KafkaMatchingShardAssignment(Consumer<String, String> consumer, Producer<Integer, String> mapProducer,
                                        Admin adminClient, int stockSlotCount, String ownOrderEndpoint,
                                        String assignmentTopic, String mapTopic, SlotEventRequests slotEventRequests,
                                        IntFunction<Optional<MatchingSlotTarget>> mapLookup,
                                        MovedSlotRequests movedSlotRequests, MapSync mapSync,
                                        java.util.concurrent.Executor decisionExecutor) {
        this.mapSync = mapSync;
        this.decisionExecutor = decisionExecutor;
        this.mapLookup = mapLookup;
        this.movedSlotRequests = movedSlotRequests;
        this.slotEventRequests = slotEventRequests;
        this.consumer = consumer;
        this.mapProducer = mapProducer;
        this.adminClient = adminClient;
        this.stockSlotCount = stockSlotCount;
        this.ownOrderEndpoint = ownOrderEndpoint;
        this.assignmentTopic = assignmentTopic;
        this.mapTopic = mapTopic;
        this.ownedSlotFlags = new boolean[stockSlotCount];
        this.mapWriteGeneration = new java.util.concurrent.atomic.AtomicIntegerArray(stockSlotCount);
    }

    /**
     * 이 컨슈머의 설정. {@code group.instance.id}를 주어 정적 멤버십으로 조인하고(재시작해도 같은
     * 신원이면 세션 시간 안에 같은 종목 slot을 돌려받는다), 배정 방식을
     * {@link CooperativeStickyAssignor}로 명시한다.
     */
    public static Properties consumerProperties(String bootstrapServers, String groupId, String groupInstanceId,
                                                int sessionTimeoutMs) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, groupInstanceId);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, sessionTimeoutMs);
        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        return props;
    }

    @Override
    public boolean test(int slot) {
        boolean[] flags = ownedSlotFlags;
        return slot >= 0 && slot < flags.length && flags[slot];
    }

    /**
     * 종목 slot의 받음 이벤트가 저널에 기록되었다는 확인. 엔진의 저널 핸들러 thread가 부른다. 그 사이 회수된 종목 slot은
     * 켜지 않는다.
     */
    public void confirmAcquired(int stockSlot) {
        boolean switchMapAfterAcquire;
        int generation;
        synchronized (this) {
            if (!awaitingJournalConfirm.remove(stockSlot)) {
                return;
            }
            boolean[] updated = ownedSlotFlags.clone();
            updated[stockSlot] = true;
            ownedSlotFlags = updated;
            switchMapAfterAcquire = awaitingMapSwitch.remove(stockSlot);
            movedInAcquiring.remove(stockSlot);
            generation = mapWriteGeneration.get(stockSlot);
        }
        if (switchMapAfterAcquire) {
            log.log(Level.INFO, "[호가창 이동] 담당 켜짐: 종목 slot=" + stockSlot);
            // 응답을 기다리지 않는다(호출 thread는 받음 항목의 기록 확인을 폴링하는 주문 수신 thread다).
            enqueueMapWrite(stockSlot, new MatchingSlotTarget(ownOrderEndpoint, false).encode(), generation);
        }
    }

    /** 맵 쓰기를 queue에 넣고 완료 future를 돌려준다. 쓰기 전용 thread는 처음 쓸 때 시작한다. */
    private java.util.concurrent.CompletableFuture<Void> enqueueMapWrite(int stockSlot, String value, int generation) {
        java.util.concurrent.CompletableFuture<Void> done = new java.util.concurrent.CompletableFuture<>();
        startMapWriteThreadIfNeeded();
        mapWriteQueue.add(new MapWrite(stockSlot, value, generation, done));
        return done;
    }

    private synchronized void startMapWriteThreadIfNeeded() {
        if (mapWriteThread == null) {
            Thread thread = new Thread(this::mapWriteLoop, "matching-shard-map-writer");
            thread.setDaemon(true);
            mapWriteThread = thread;
            thread.start();
        }
    }

    /**
     * 쓰기 전용 thread. queue에서 하나씩 꺼내, 보내기 직전에 그 종목 slot의 세대가 쓰기를 넣을 때와 같은지 확인한다(회수되어 세대가 바뀌었으면
     * 보내지 않고 끝낸다). 브로커 응답을 기다리고, 실패하면 ERROR 로그를 남기고 {@code mapWriteRetryMillis} 뒤에 같은 쓰기를 다시 보낸다 —
     * 뒤 쓰기는 앞 쓰기가 끝날 때까지 기다리므로 순서가 바뀌지 않는다.
     */
    private void mapWriteLoop() {
        while (!Thread.currentThread().isInterrupted()) {
            MapWrite write;
            try {
                write = mapWriteQueue.take();
            } catch (InterruptedException e) {
                return;
            }
            try {
                while (true) {
                    if (mapWriteGeneration.get(write.stockSlot()) != write.generation()) {
                        write.done().complete(null);
                        break;
                    }
                    try {
                        mapProducer.send(new ProducerRecord<>(mapTopic, write.stockSlot(), write.value())).get();
                        write.done().complete(null);
                        break;
                    } catch (ExecutionException e) {
                        log.log(Level.ERROR, "[매칭] 맵 쓰기 실패, 다시 보냄: 종목 slot=" + write.stockSlot() + " 값=" + write.value()
                            + " 원인=" + e.getCause());
                        Thread.sleep(mapWriteRetryMillis);
                    } catch (RuntimeException e) {
                        // 보내는 호출 자체가 던진 예외도 같은 간격으로 다시 보낸다. 담당 표시를 켠 뒤의 쓰기가 사라지면 맵이 이전 담당의 이동 중
                        // 값으로 남아 이 종목 slot의 주문이 보관만 된다.
                        log.log(Level.ERROR, "[매칭] 맵 쓰기가 예외로 끝나 다시 보냄: 종목 slot=" + write.stockSlot() + " 값=" + write.value()
                            + " 원인=" + e);
                        Thread.sleep(mapWriteRetryMillis);
                    }
                }
            } catch (InterruptedException e) {
                write.done().completeExceptionally(e);
                return;
            }
        }
    }

    /**
     * 콜백이 맵 쓰기 완료를 기다리는 한도(설정 {@code matching-shard.rebalance.map-write-deadline-ms}, 기본 30초는 근거 없는 임의 값이며
     * max.poll.interval.ms 기본값 300초보다 충분히 작게 잡았다).
     */
    public void setMapWriteDeadlineMillis(long millis) {
        this.mapWriteDeadlineMillis = millis;
    }

    /** 패키지 가시성 — 시험이 다시 보내기 간격을 줄인다. */
    void setMapWriteRetryMillis(long millis) {
        this.mapWriteRetryMillis = millis;
    }

    /** 배정을 받았고 이동으로 받는지 첫 배정인지 판정을 기다리는 종목 slot의 사본(점검용). */
    public Set<Integer> slotsAwaitingDecision() {
        return Set.copyOf(awaitingDecision);
    }

    /** 이동으로 받기로 하고 이전 담당의 호가창 적재와 스냅샷 확정을 기다리는 종목 slot의 사본(점검용). */
    public Set<Integer> slotsAwaitingLoad() {
        return Set.copyOf(awaitingLoad);
    }

    /** 이동으로 받는 도중 판정이나 호가창 적재를 기다리며 담당이 켜지지 않은 종목 slot의 수. 포기한 이동은 자동으로 풀리지 않으므로 0이 아니면 확인이 필요하다. */
    public int stuckSlotCount() {
        return awaitingDecision.size() + awaitingLoad.size();
    }

    /** 그 종목 slot이 이동으로 받기로 하고 호가창 적재와 스냅샷 확정을 기다리는 중인지. */
    public boolean isAwaitingLoad(int stockSlot) {
        return awaitingLoad.contains(stockSlot);
    }

    public Set<Integer> currentSlotsSnapshot() {
        boolean[] flags = ownedSlotFlags;
        return IntStream.range(0, flags.length).filter(slot -> flags[slot]).boxed()
            .collect(Collectors.toUnmodifiableSet());
    }

    /**
     * 조정 토픽과 맵 토픽이 있는지 확인하고 없으면 만든다. 맵을 읽는 쪽({@link MatchingSlotMapReader})이 토픽이 생긴 뒤에 읽기 시작할
     * 수 있도록 {@link #start}와 따로 부를 수 있다. 몇 번을 불러도 같다.
     */
    public void prepareTopics() {
        ensureAssignmentTopicPartitionCount();
        ensureMapTopicExists();
    }

    public void start() {
        prepareTopics();
        running = true;
        pollThread = new Thread(this::pollLoop, "matching-shard-assignment-poll");
        pollThread.setDaemon(true);
        pollThread.start();
    }

    public void close() {
        running = false;
        consumer.wakeup();
        try {
            if (pollThread != null) {
                pollThread.join(Duration.ofSeconds(5).toMillis());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        // 컨슈머를 닫으면 맡은 종목 slot 전체에 회수 콜백이 불려 이동 중 표시를 쓰고 기다린다. 쓰기 전용 thread는 그 뒤에 멈춘다.
        consumer.close();
        Thread writer = mapWriteThread;
        if (writer != null) {
            writer.interrupt();
        }
        mapWriteQueue.forEach(write -> write.done().completeExceptionally(new IllegalStateException("닫힘")));
        mapProducer.close();
        adminClient.close();
    }

    private void pollLoop() {
        consumer.subscribe(List.of(assignmentTopic), rebalanceListener());
        try {
            while (running) {
                consumer.poll(Duration.ofMillis(500));
            }
        } catch (WakeupException e) {
            // close()가 poll을 깨운 것이다.
        }
    }

    ConsumerRebalanceListener rebalanceListener() {
        return new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                List<TopicPartition> writeMoving = new java.util.ArrayList<>();
                List<Integer> cancel = new java.util.ArrayList<>();
                List<Integer> cleanup = new java.util.ArrayList<>();
                List<Integer> release = new java.util.ArrayList<>();
                synchronized (KafkaMatchingShardAssignment.this) {
                    boolean[] updated = ownedSlotFlags.clone();
                    for (TopicPartition partition : partitions) {
                        int slot = partition.partition();
                        updated[slot] = false;
                        mapWriteGeneration.incrementAndGet(slot);
                        boolean wasDeciding = awaitingDecision.remove(slot);
                        boolean wasLoading = awaitingLoad.remove(slot);
                        boolean wasAcquiringMovedIn = movedInAcquiring.remove(slot);
                        awaitingJournalConfirm.remove(slot);
                        awaitingMapSwitch.remove(slot);
                        if (wasDeciding) {
                            continue; // 판정 전에 회수되었다. 한 일이 없다.
                        }
                        if (wasLoading || wasAcquiringMovedIn) {
                            // 이동으로 받는 도중 담당이 되기 전에 회수되었다. 맵은 이전 담당의 「주소;moving」 그대로 두어 다음 담당이 이전 담당에게
                            // 요청하게 한다(자기 주소의 이동 중 값으로 덮으면 호가창 없는 이 shard에 요청이 간다). 진행 중이던 요청의 응답은 버리고,
                            // 적재가 끝나 있었다면 그 호가창은 복사본이므로 정리로 지운다. 받음 항목을 이미 요청했다면 저널이 맞도록 넘김 항목도 남긴다.
                            cancel.add(slot);
                            cleanup.add(slot);
                            if (wasAcquiringMovedIn) {
                                release.add(slot);
                            }
                        } else {
                            release.add(slot);
                            writeMoving.add(partition);
                        }
                    }
                    ownedSlotFlags = updated;
                }
                if (movedSlotRequests != null) {
                    cancel.forEach(movedSlotRequests::cancelHandover);
                }
                if (slotEventRequests != null) {
                    cleanup.forEach(slotEventRequests::requestCleanup);
                    release.forEach(slotEventRequests::requestRelease);
                }
                publishMapEntries(writeMoving, new MatchingSlotTarget(ownOrderEndpoint, true).encode());
                log.log(Level.WARNING, "[매칭] 담당 종목 slot 회수: " + slotNumbers(partitions)
                    + " 맵에 이동 중 표시=" + slotNumbers(writeMoving) + " 남은 담당=" + currentSlotsSnapshot().size() + "개");
            }

            /**
             * 세션 시간을 넘겨 그룹에서 쫓겨난 뒤 돌아와 받는다. 그 종목 slot은 이미 다른 pod에 배정되어 그 pod가 맵에 자기 주소를 올렸을 수 있다. 기본
             * 구현은 회수 콜백을 그대로 불러 맵에 이동 중 표시를 써 새 담당의 값을 덮으므로, 맵에는 쓰지 않고 담당 표시와 대기 상태만 끈다. 호가창은
             * 지우지 않고 넘김 이벤트도 남기지 않는다(그 뒤 처리는 재시작 규칙의 몫이다).
             */
            @Override
            public void onPartitionsLost(Collection<TopicPartition> partitions) {
                List<Integer> cancel = new java.util.ArrayList<>();
                synchronized (KafkaMatchingShardAssignment.this) {
                    boolean[] updated = ownedSlotFlags.clone();
                    for (TopicPartition partition : partitions) {
                        int slot = partition.partition();
                        updated[slot] = false;
                        mapWriteGeneration.incrementAndGet(slot);
                        awaitingDecision.remove(slot);
                        boolean wasLoading = awaitingLoad.remove(slot);
                        boolean wasAcquiringMovedIn = movedInAcquiring.remove(slot);
                        awaitingJournalConfirm.remove(slot);
                        awaitingMapSwitch.remove(slot);
                        if (wasLoading || wasAcquiringMovedIn) {
                            cancel.add(slot);
                        }
                    }
                    ownedSlotFlags = updated;
                }
                if (movedSlotRequests != null) {
                    cancel.forEach(movedSlotRequests::cancelHandover);
                }
                log.log(Level.WARNING, "[매칭] 종목 slot을 잃음(세션 만료로 이미 다른 pod에 배정되었을 수 있음) — 맵에는 쓰지 않고 담당 표시만 끔: "
                    + slotNumbers(partitions));
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                List<Integer> slots = partitions.stream().map(TopicPartition::partition).toList();
                if (mapLookup == null || movedSlotRequests == null || slotEventRequests == null) {
                    assignWithoutMoveSupport(partitions);
                    return;
                }
                // 판정은 맵 읽기가 맵 토픽의 끝까지 따라온 뒤에 한다. 이전 담당의 이동 중 값이 아직 읽는 쪽에 도착하지 않았는데 항목 없음으로
                // 보고 첫 배정으로 가정하면 빈 호가창으로 담당을 켜고 평상시 값을 올려 이전 담당이 진짜 호가창을 지우게 된다. 콜백은 판정 요청만
                // 남기고 돌아간다.
                awaitingDecision.addAll(slots);
                Runnable decision = () -> decide(slots);
                if (decisionExecutor == null) {
                    decision.run();
                } else {
                    decisionExecutor.execute(decision);
                }
            }
        };
    }

    /** 이동 처리를 쓰지 않는 구성(요청 통로나 맵 읽기를 받지 않은 구성)의 배정: 모든 종목 slot을 첫 배정으로 본다. */
    private void assignWithoutMoveSupport(Collection<TopicPartition> partitions) {
        if (slotEventRequests == null) {
            synchronized (this) {
                boolean[] updated = ownedSlotFlags.clone();
                partitions.forEach(p -> updated[p.partition()] = true);
                ownedSlotFlags = updated;
            }
        } else {
            partitions.forEach(p -> {
                awaitingJournalConfirm.add(p.partition());
                slotEventRequests.requestAcquire(p.partition());
            });
        }
        if (!partitions.isEmpty()) {
            publishMapEntries(partitions, ownOrderEndpoint);
        }
        log.log(Level.INFO, "[매칭] 담당 종목 slot 배정: 신규=" + slotNumbers(partitions) + " 전체 담당=" + currentSlotsSnapshot().size() + "개");
    }

    private enum Kind { MOVED_IN, SELF_RESTART, FIRST }

    /** 판정 thread에서 돈다. 맵 읽기가 따라온 뒤 종목 slot마다 이동으로 받는지, 재시작인지, 첫 배정인지 가른다. */
    private void decide(List<Integer> slots) {
        if (mapSync != null && !mapSync.awaitCaughtUp()) {
            log.log(Level.ERROR, "[매칭] 맵 읽기가 시간 안에 맵 토픽의 끝까지 따라오지 못해 이동으로 받는지 판정하지 못함 — "
                + "첫 배정으로 가정하지 않고 담당을 켜지 않은 채 둔다: 종목 slot=" + slots);
            return;
        }
        List<Integer> toRequest = new java.util.ArrayList<>();
        List<Integer> toAcquire = new java.util.ArrayList<>();
        List<TopicPartition> firstAssigned = new java.util.ArrayList<>();
        synchronized (this) {
            for (int slot : slots) {
                if (!awaitingDecision.remove(slot)) {
                    continue; // 판정 전에 회수되었다.
                }
                switch (classify(slot)) {
                    case MOVED_IN -> {
                        // 이전 담당이 넘기는 중인 종목 slot이다. 맵은 호가창 적재가 끝날 때까지 이동 중으로 남기므로 자기 주소를 올리지 않고,
                        // 호가창을 요청하며, 적재와 스냅샷 확정이 끝나기 전에는 담당 표시를 켜지 않는다.
                        awaitingLoad.add(slot);
                        toRequest.add(slot);
                    }
                    case SELF_RESTART -> {
                        // 이전 담당이 자기 자신이다(재시작). 복원한 호가창을 그대로 쓰고, 담당이 켜진 뒤에 맵을 평상시 값으로 되돌린다.
                        awaitingMapSwitch.add(slot);
                        awaitingJournalConfirm.add(slot);
                        toAcquire.add(slot);
                    }
                    case FIRST -> {
                        awaitingJournalConfirm.add(slot);
                        toAcquire.add(slot);
                        firstAssigned.add(new TopicPartition(assignmentTopic, slot));
                    }
                }
            }
        }
        toRequest.forEach(movedSlotRequests::requestHandover);
        toAcquire.forEach(slotEventRequests::requestAcquire);
        if (!firstAssigned.isEmpty()) {
            publishMapEntries(firstAssigned, ownOrderEndpoint);
        }
        log.log(Level.INFO, "[매칭] 담당 종목 slot 배정 판정: 이동으로 받는 중=" + toRequest + " 재시작=" + toAcquire
            + " 전체 담당=" + currentSlotsSnapshot().size() + "개");
    }

    /** 맵의 그 종목 slot 항목으로 가른다. 이동 중이 아닌 다른 pod의 항목이나 항목 없음은 첫 배정으로 본다. */
    private Kind classify(int stockSlot) {
        Optional<MatchingSlotTarget> entry = mapLookup.apply(stockSlot);
        if (entry.isEmpty()) {
            return Kind.FIRST;
        }
        MatchingSlotTarget target = entry.get();
        if (target.moving()) {
            return target.endpoint().equals(ownOrderEndpoint) ? Kind.SELF_RESTART : Kind.MOVED_IN;
        }
        if (!target.endpoint().equals(ownOrderEndpoint)) {
            // 이전 담당이 회수 콜백 없이 사라진 경우다. 장애 복구 범위라 이 단위에서 정하지 않고 첫 배정처럼 처리한다.
            log.log(Level.WARNING, "[매칭] 맵에 다른 pod의 항목이 남은 채 종목 slot을 배정받음(장애 복구 범위): 종목 slot="
                + stockSlot + " 항목=" + target);
        }
        return Kind.FIRST;
    }

    /**
     * 이동으로 받은 종목 slot의 호가창 적재와 스냅샷 확정이 끝났다는 신호. 이때 받음 요청을 남기고 저널 기록 확인이 오면
     * ({@link #confirmAcquired}) 담당이 켜진다. 그 사이 회수된 종목 slot은 요청을 남기지 않는다.
     */
    public void acquireAfterLoad(int stockSlot) {
        synchronized (this) {
            if (!awaitingLoad.remove(stockSlot)) {
                return;
            }
            movedInAcquiring.add(stockSlot);
            awaitingMapSwitch.add(stockSlot);
            awaitingJournalConfirm.add(stockSlot);
        }
        slotEventRequests.requestAcquire(stockSlot);
    }

    private void publishMapEntries(Collection<TopicPartition> partitions, String value) {
        List<java.util.concurrent.CompletableFuture<Void>> writes = new java.util.ArrayList<>();
        for (TopicPartition partition : partitions) {
            writes.add(enqueueMapWrite(partition.partition(), value, mapWriteGeneration.get(partition.partition())));
        }
        // 모든 쓰기의 완료를 기다리는 총 시간에 한도를 둔다. 넘기면 콜백은 돌아가고 쓰기 thread가 queue 순서대로 계속 재시도한다.
        long deadline = System.nanoTime() + mapWriteDeadlineMillis * 1_000_000L;
        for (java.util.concurrent.CompletableFuture<Void> write : writes) {
            try {
                write.get(Math.max(0L, deadline - System.nanoTime()), java.util.concurrent.TimeUnit.NANOSECONDS);
            } catch (java.util.concurrent.TimeoutException e) {
                log.log(Level.ERROR, "[매칭] 맵 쓰기 완료를 " + mapWriteDeadlineMillis + "ms 안에 확인하지 못해 콜백을 돌려보냄 — 쓰기는 계속 재시도한다: 값="
                    + value);
                return;
            } catch (ExecutionException e) {
                throw new IllegalStateException(mapTopic + " 발행 실패: 값=" + value, e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(mapTopic + " 발행 중 인터럽트됨", e);
            }
        }
    }

    private List<Integer> slotNumbers(Collection<TopicPartition> partitions) {
        return partitions.stream().map(TopicPartition::partition).toList();
    }

    /** 토픽이 없으면 종목 slot 개수와 같은 파티션 수로 만든다. 있는데 파티션 수가 다르면 기동을 막는다. */
    private void ensureAssignmentTopicPartitionCount() {
        try {
            Map<String, TopicDescription> described = adminClient.describeTopics(List.of(assignmentTopic)).allTopicNames().get();
            int actual = described.get(assignmentTopic).partitions().size();
            if (actual != stockSlotCount) {
                throw new IllegalStateException(
                    "조정 토픽 " + assignmentTopic + "의 파티션 수(" + actual + ")가 종목 slot 개수(" + stockSlotCount
                        + ")와 다릅니다 — 설정이 어긋났습니다");
            }
        } catch (ExecutionException e) {
            if (e.getCause() instanceof UnknownTopicOrPartitionException) {
                createAssignmentTopic();
                return;
            }
            throw new IllegalStateException("조정 토픽 조회 실패: " + assignmentTopic, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("조정 토픽 조회 중 인터럽트됨", e);
        }
    }

    private void createAssignmentTopic() {
        try {
            adminClient.createTopics(List.of(new NewTopic(assignmentTopic, stockSlotCount, (short) 1))).all().get();
            log.log(Level.INFO, "[매칭] 조정 토픽 생성: " + assignmentTopic + " partitions=" + stockSlotCount);
        } catch (ExecutionException e) {
            if (!(e.getCause() instanceof TopicExistsException)) {
                throw new IllegalStateException("조정 토픽 생성 실패: " + assignmentTopic, e);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("조정 토픽 생성 중 인터럽트됨", e);
        }
    }

    private void ensureMapTopicExists() {
        try {
            adminClient.describeTopics(List.of(mapTopic)).allTopicNames().get();
        } catch (ExecutionException e) {
            if (e.getCause() instanceof UnknownTopicOrPartitionException) {
                createMapTopic();
                return;
            }
            throw new IllegalStateException(mapTopic + " 조회 실패", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(mapTopic + " 조회 중 인터럽트됨", e);
        }
    }

    /** 맵 토픽은 최신 값만 있으면 되는 KV 저장소라 compacted로, 파티션 1개로 만든다(계좌 쪽과 같다). */
    private void createMapTopic() {
        try {
            NewTopic topic = new NewTopic(mapTopic, 1, (short) 1).configs(Map.of("cleanup.policy", "compact"));
            adminClient.createTopics(List.of(topic)).all().get();
            log.log(Level.INFO, "[매칭] " + mapTopic + " 토픽 생성(compacted)");
        } catch (ExecutionException e) {
            if (!(e.getCause() instanceof TopicExistsException)) {
                throw new IllegalStateException(mapTopic + " 생성 실패", e);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(mapTopic + " 생성 중 인터럽트됨", e);
        }
    }
}
