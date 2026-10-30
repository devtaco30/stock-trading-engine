package com.flab.stocktradingengine.account.worker.coordination;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

import com.flab.stocktradingengine.aeron.MatchingSlotTarget;
import com.flab.stocktradingengine.aeron.StockSlotHasher;

/**
 * 매칭 샤딩 U8 — 종목코드로 담당 매칭 pod의 주문 입구 주소를 찾는다. 매칭 pod들이
 * {@code matching-shard-map}(종목 slot 번호 → 그 종목 slot을 담당하는 pod의 주소, compacted)에
 * 발행한 배정을 계좌 shard가 구독해 "지금 이 종목 slot은 누가 담당인가"를 실시간으로 안다.
 * core의 {@code AssignmentDestinationResolver}(계좌 방향)를 본뜬 복사본이다 — 키가 종목코드
 * 문자열이고 값이 주소 하나(+이동 중 표시)라 모양이 달라 일반화하지 않았다(설계서 2-5절).
 *
 * <h3>컨슈머 그룹이 아니라 전체 구독이다</h3>
 * <p>계좌 shard마다 표 전체가 있어야 라우팅할 수 있으므로 그룹 없이 {@code assign}으로 모든 파티션을
 * 직접 맡아 처음부터({@code seekToBeginning}) 계속 읽는다.</p>
 *
 * <h3>핫패스 — 락 없는 배열 통째 교체</h3>
 * <p>{@link #endpointFor}·{@link #targetFor}는 sender thread가 주문마다 부른다. 갱신할 때 배열을
 * 통째로 복사해 갈아끼우고(poll thread 하나만 쓴다) 읽는 쪽은 volatile 참조 한 번 읽고 인덱스
 * 조회 하나로 끝난다. 한 poll에서 받은 변경은 배열 교체 한 번으로 함께 보인다(설계서 11-1절).</p>
 *
 * <h3>주소 풀 검증은 조회 시점에 한다</h3>
 * <p>맵 토픽의 주소가 풀에 없으면 설정 어긋남이다. poll thread에서 던지면 호출한 쪽에 닿지 않으므로
 * 값은 표에 두고, 그 종목 slot을 조회하는 sender thread에서
 * {@link MatchingDestinationMisconfiguredException}을 던진다.</p>
 */
public final class MatchingDestinationResolver implements MatchingTargetLookup {

    private static final Logger log = System.getLogger(MatchingDestinationResolver.class.getName());

    public static final String MAP_TOPIC = "matching-shard-map";

    private volatile Consumer<Integer, String> consumer;
    // 읽기 thread가 예외로 끝난 뒤 새 consumer를 만들어 처음부터 다시 읽게 하는 공급자와 시도 간격. 공급자가 없으면 복구하지 않는다.
    private final Supplier<Consumer<Integer, String>> consumerFactory;
    private final long recoveryIntervalMillis;
    private final StockSlotHasher stockSlotHasher;
    private final Set<String> addressPool;

    /** 표와 그 표를 읽는 view를 한 객체로 묶어 한 번에 교체한다 — sender thread의 매 회차 호출이 객체를 새로 만들지 않는다. */
    private record TableState(MatchingSlotTarget[] table, MatchingTargetView view) {
    }

    private volatile TableState tableState;
    private volatile boolean running;
    private Thread pollThread;
    private final CountDownLatch initialCatchUpLatch = new CountDownLatch(1);
    // 초기 읽기를 마치기 전에 읽기 thread가 예외로 끝났으면 그 예외. awaitInitialCatchUp이 실패로 알린다.
    private volatile Throwable initialCatchUpFailure;
    // 읽기 thread가 예외로 끝난 뒤의 원인(초기 읽기 전후 모두). 값이 있으면 표가 더는 갱신되지 않으므로
    // view()는 모든 종목 slot을 「알 수 없음」으로 돌려준다.
    private volatile Throwable readFailureCause;
    private static final MatchingTargetView UNKNOWN_VIEW = stockCode -> Optional.empty();

    /**
     * 표 크기는 {@link StockSlotHasher}가 들고 있는 종목 slot 개수 하나만 쓴다 — 계산에 쓰는 개수와
     * 담아 두는 개수가 따로 주어지면 서로 어긋날 수 있다.
     *
     * @param addressPool 계좌 워커 설정에 적힌 매칭 pod 주소 풀(설계서 2-8절)
     */
    public MatchingDestinationResolver(Consumer<Integer, String> consumer, StockSlotHasher stockSlotHasher,
                                       Set<String> addressPool) {
        this(consumer, stockSlotHasher, addressPool, null, 0L);
    }

    /**
     * 초기 읽기를 마친 뒤 읽기 thread가 예외로 끝나면 {@code recoveryIntervalMillis}마다 {@code consumerFactory}로 새 consumer를
     * 만들어 맵 토픽을 처음부터 다시 읽는다. 다 읽을 때까지는 담당을 모르는 상태로 두고(보관), 다 읽으면 읽기 실패 기록을 지운다.
     */
    public MatchingDestinationResolver(Consumer<Integer, String> consumer, StockSlotHasher stockSlotHasher,
                                       Set<String> addressPool, Supplier<Consumer<Integer, String>> consumerFactory,
                                       long recoveryIntervalMillis) {
        this.consumerFactory = consumerFactory;
        this.recoveryIntervalMillis = recoveryIntervalMillis;
        this.consumer = consumer;
        this.stockSlotHasher = stockSlotHasher;
        this.addressPool = Set.copyOf(addressPool);
        this.tableState = newTableState(new MatchingSlotTarget[stockSlotHasher.stockSlotCount()]);
    }

    /** 담당 pod의 주소. 이동 중인 종목 slot도 주소는 돌려준다 — 보낼지 보관할지는 호출한 쪽이 {@link #targetFor}로 판단한다. */
    public Optional<String> endpointFor(String stockCode) {
        return targetFor(stockCode).map(MatchingSlotTarget::endpoint);
    }

    public Optional<MatchingSlotTarget> targetFor(String stockCode) {
        return view().targetFor(stockCode);
    }

    /** 지금 표의 모습을 고정해 돌려준다. 이 view로 조회하는 동안 갱신이 있어도 view의 내용은 그대로다. */
    @Override
    public MatchingTargetView view() {
        if (readFailureCause != null) {
            // 갱신이 멈춘 옛 표로 계속 보내면 주문이 옛 담당 pod로 간다 — 담당을 모르는 상태와 같은 경로(보관 목록,
            // 가득 차면 MATCHING_HOLDING_FULL 거부)로 닫는다.
            return UNKNOWN_VIEW;
        }
        return tableState.view();
    }

    /** 읽기 thread가 예외로 끝났으면 그 원인. 상태를 조회하는 접근자다. */
    public Optional<Throwable> readFailure() {
        return Optional.ofNullable(readFailureCause);
    }

    private TableState newTableState(MatchingSlotTarget[] table) {
        return new TableState(table, stockCode -> targetIn(table, stockCode));
    }

    private Optional<MatchingSlotTarget> targetIn(MatchingSlotTarget[] table, String stockCode) {
        int slot = stockSlotHasher.slotFor(stockCode);
        if (slot < 0 || slot >= table.length) {
            return Optional.empty();
        }
        MatchingSlotTarget target = table[slot];
        if (target == null) {
            return Optional.empty();
        }
        if (!addressPool.contains(target.endpoint())) {
            throw new MatchingDestinationMisconfiguredException(
                MAP_TOPIC + "가 주소 풀에 없는 매칭 pod 주소를 가리킨다: 종목코드=" + stockCode + " 종목 slot=" + slot
                    + " 주소=" + target.endpoint() + " 풀=" + addressPool);
        }
        return Optional.of(target);
    }

    /** 테스트용 — 지금 표의 참조를 그대로 돌려준다. 읽은 뒤에는 바뀌지 않는다(갱신은 새 배열로 교체). */
    MatchingSlotTarget[] snapshot() {
        return tableState.table();
    }

    public void start() {
        List<PartitionInfo> partitionInfos = consumer.partitionsFor(MAP_TOPIC);
        if (partitionInfos == null || partitionInfos.isEmpty()) {
            // 맵 토픽이 아직 없거나 파티션이 없다. 빈 목록으로 읽기를 시작하면 첫 poll에서 「다 읽었다」로 판정돼
            // 배정을 하나도 모른 채 주문 접수가 열린다 — 기동을 중단한다.
            throw new IllegalStateException(MAP_TOPIC + "의 파티션을 찾지 못했습니다(토픽이 없거나 파티션이 없음) — "
                + "매칭 shard가 맵 토픽을 만든 뒤에 계좌 shard를 기동해야 합니다");
        }
        List<TopicPartition> partitions = partitionInfos.stream()
            .map(info -> new TopicPartition(MAP_TOPIC, info.partition()))
            .toList();
        consumer.assign(partitions);
        consumer.seekToBeginning(partitions);
        // 지금까지 쌓인 기록을 다 읽었다고 판정할 기준선. 이 값 이후에 도착하는 레코드는 "아직
        // 못 읽은 과거"가 아니라 "그때부터 새로 일어난 배정 변경"이라 초기 읽기 판정에 안 쓴다.
        Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);
        running = true;
        pollThread = new Thread(() -> pollLoop(partitions, endOffsets), "matching-shard-map-poll");
        pollThread.setDaemon(true);
        pollThread.start();
    }

    /**
     * 지금까지 {@code matching-shard-map}에 쌓인 기록을 전부 읽을 때까지 호출 스레드를 기다리게
     * 한다 — 계좌 shard가 "아직 아무 배정도 모르는 채로" 주문을 받기 시작하는 것을 막는 용도다.
     * 토픽이 비어 있으면 곧바로 반환한다.
     *
     * @throws IllegalStateException timeout 안에 못 끝나거나(브로커 문제 등), 초기 읽기를 마치기 전에
     *                               읽기 thread가 예외로 끝났으면(원인 예외를 cause로 담는다)
     */
    public void awaitInitialCatchUp(Duration timeout) {
        try {
            if (!initialCatchUpLatch.await(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
                throw new IllegalStateException(
                    MAP_TOPIC + " 초기 읽기가 " + timeout + " 안에 끝나지 않았습니다");
            }
            Throwable failure = initialCatchUpFailure;
            if (failure != null) {
                throw new IllegalStateException(MAP_TOPIC + " 초기 읽기 중 읽기 thread가 예외로 끝났습니다", failure);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(MAP_TOPIC + " 초기 읽기 대기 중 인터럽트됨", e);
        }
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
        consumer.close();
    }

    private void pollLoop(List<TopicPartition> partitions, Map<TopicPartition, Long> endOffsets) {
        boolean caughtUp = false;
        try {
            while (running) {
                try {
                    while (running) {
                        ConsumerRecords<Integer, String> records = consumer.poll(Duration.ofMillis(500));
                        if (!records.isEmpty()) {
                            applyRecords(records);
                        }
                        if (!caughtUp && isCaughtUp(partitions, endOffsets)) {
                            caughtUp = true;
                            initialCatchUpLatch.countDown();
                        }
                    }
                } catch (WakeupException e) {
                    // close()가 의도적으로 poll을 깨운 것 — 정상 종료 경로.
                    return;
                } catch (RuntimeException e) {
                    log.log(Level.ERROR, "[목적지] " + MAP_TOPIC + " 읽기 thread가 예외로 끝났습니다 — 이후 모든 종목 slot의 담당을 "
                        + "알 수 없음으로 돌려줍니다", e);
                    readFailureCause = e;
                    if (!caughtUp) {
                        // 초기 읽기를 마치기 전이면 대기 중인 호출자가 정상 반환하지 않고 실패로 알게 한다.
                        initialCatchUpFailure = e;
                        return;
                    }
                    if (consumerFactory == null || !recoverReading()) {
                        return;
                    }
                }
            }
        } finally {
            // 정상 종료 경로에서도 대기 중인 awaitInitialCatchUp 호출자가 영원히 안 깨는 일이 없게 한다.
            initialCatchUpLatch.countDown();
        }
    }

    /**
     * 간격을 두고 새 consumer를 만들어 맵 토픽을 처음부터 끝까지 다시 읽는다. 읽는 동안 표는 빈 표에서 새로 쌓고, 다 읽은 뒤에야 읽기 실패 기록을
     * 지워 view가 다시 값을 돌려주게 한다. 시도가 실패하면 ERROR를 남기고 같은 간격으로 다시 시도한다. close()로 멈추면 false를 돌려준다.
     */
    private boolean recoverReading() {
        while (running) {
            if (!sleepWhileRunning(recoveryIntervalMillis)) {
                return false;
            }
            try {
                Consumer<Integer, String> old = consumer;
                try {
                    old.close(Duration.ofSeconds(1));
                } catch (RuntimeException closeFailure) {
                    log.log(Level.WARNING, "[목적지] 옛 consumer를 닫지 못했다(무시하고 새로 만든다): " + closeFailure.getMessage());
                }
                Consumer<Integer, String> fresh = consumerFactory.get();
                consumer = fresh;
                List<PartitionInfo> partitionInfos = fresh.partitionsFor(MAP_TOPIC);
                if (partitionInfos == null || partitionInfos.isEmpty()) {
                    throw new IllegalStateException(MAP_TOPIC + "의 파티션을 찾지 못했습니다");
                }
                List<TopicPartition> partitions = partitionInfos.stream()
                    .map(info -> new TopicPartition(MAP_TOPIC, info.partition()))
                    .toList();
                fresh.assign(partitions);
                fresh.seekToBeginning(partitions);
                Map<TopicPartition, Long> ends = fresh.endOffsets(partitions);
                tableState = newTableState(new MatchingSlotTarget[stockSlotHasher.stockSlotCount()]);
                while (running && !isCaughtUp(partitions, ends)) {
                    ConsumerRecords<Integer, String> records = fresh.poll(Duration.ofMillis(500));
                    if (!records.isEmpty()) {
                        applyRecords(records);
                    }
                }
                if (!running) {
                    return false;
                }
                readFailureCause = null;
                log.log(Level.INFO, "[목적지] " + MAP_TOPIC + " 읽기를 처음부터 다시 읽어 재개했습니다");
                return true;
            } catch (WakeupException e) {
                return false;
            } catch (RuntimeException e) {
                log.log(Level.ERROR, "[목적지] " + MAP_TOPIC + " 읽기 복구 시도가 실패했습니다 — " + recoveryIntervalMillis
                    + "ms 뒤 다시 시도합니다", e);
            }
        }
        return false;
    }

    /** close()가 오래 기다리지 않게 짧게 나눠 쉰다. 쉬는 도중 멈추면 false. */
    private boolean sleepWhileRunning(long millis) {
        long deadline = System.nanoTime() + millis * 1_000_000L;
        while (running && System.nanoTime() < deadline) {
            try {
                Thread.sleep(Math.min(20L, Math.max(1L, millis)));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return running;
    }

    /** consumer.position()은 이 poll 스레드에서만 부른다 — KafkaConsumer는 여러 스레드에서 동시에 쓸 수 없다. */
    private boolean isCaughtUp(List<TopicPartition> partitions, Map<TopicPartition, Long> endOffsets) {
        for (TopicPartition partition : partitions) {
            if (consumer.position(partition) < endOffsets.get(partition)) {
                return false;
            }
        }
        return true;
    }

    private void applyRecords(ConsumerRecords<Integer, String> records) {
        MatchingSlotTarget[] updated = tableState.table().clone();
        for (ConsumerRecord<Integer, String> record : records) {
            int slot = record.key();
            if (slot < 0 || slot >= updated.length) {
                log.log(Level.WARNING, "[목적지] " + MAP_TOPIC + "에서 범위 밖 종목 slot을 받음: slot=" + slot
                    + " stockSlotCount=" + updated.length);
                continue;
            }
            // value가 null이면 tombstone(그 pod가 종목 slot을 회수당함) — 목적지 없음으로 되돌린다.
            if (record.value() == null) {
                updated[slot] = null;
                continue;
            }
            try {
                MatchingSlotTarget parsed = MatchingSlotTarget.parse(record.value());
                updated[slot] = parsed;
                if (parsed.moving()) {
                    // 이동 중 값은 종목 slot이 옮겨질 때 slot마다 한 번 올라오므로 slot 수 이내의 줄만 남는다. 시험이 「이동 중 표시 구간」을
                    // Kafka 기록 시각이 아니라 이 계좌 shard가 표시를 읽은 시각으로 다시 잴 때 쓴다.
                    log.log(Level.INFO, "[목적지] 이동 중 표시를 읽음: slot=" + slot + " 시각(epoch ms)=" + System.currentTimeMillis());
                }
            } catch (IllegalArgumentException e) {
                // 형식이 깨진 값 하나 때문에 나머지 배정까지 잃지 않는다. 그 종목 slot만 목적지 없음으로
                // 둔다.
                log.log(Level.WARNING, "[목적지] " + MAP_TOPIC + "에서 읽을 수 없는 값: slot=" + slot
                    + " value=" + record.value() + " — 이 종목 slot은 목적지 없음으로 둔다");
                updated[slot] = null;
            }
        }
        tableState = newTableState(updated);
        log.log(Level.INFO, "[목적지] " + MAP_TOPIC + " " + records.count() + "건 반영");
    }
}
