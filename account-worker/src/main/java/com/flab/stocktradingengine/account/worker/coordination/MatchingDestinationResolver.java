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

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

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
public final class MatchingDestinationResolver {

    private static final Logger log = System.getLogger(MatchingDestinationResolver.class.getName());

    public static final String MAP_TOPIC = "matching-shard-map";

    private final Consumer<Integer, String> consumer;
    private final StockSlotHasher stockSlotHasher;
    private final Set<String> addressPool;

    private volatile MatchingSlotTarget[] slotToTargets;
    private volatile boolean running;
    private Thread pollThread;
    private final CountDownLatch initialCatchUpLatch = new CountDownLatch(1);

    /**
     * 표 크기는 {@link StockSlotHasher}가 들고 있는 종목 slot 개수 하나만 쓴다 — 계산에 쓰는 개수와
     * 담아 두는 개수가 따로 주어지면 서로 어긋날 수 있다.
     *
     * @param addressPool 계좌 워커 설정에 적힌 매칭 pod 주소 풀(설계서 2-8절)
     */
    public MatchingDestinationResolver(Consumer<Integer, String> consumer, StockSlotHasher stockSlotHasher,
                                       Set<String> addressPool) {
        this.consumer = consumer;
        this.stockSlotHasher = stockSlotHasher;
        this.addressPool = Set.copyOf(addressPool);
        this.slotToTargets = new MatchingSlotTarget[stockSlotHasher.stockSlotCount()];
    }

    /** 담당 pod의 주소. 이동 중인 종목 slot도 주소는 돌려준다 — 보낼지 보관할지는 호출한 쪽이 {@link #targetFor}로 판단한다. */
    public Optional<String> endpointFor(String stockCode) {
        return targetFor(stockCode).map(MatchingSlotTarget::endpoint);
    }

    public Optional<MatchingSlotTarget> targetFor(String stockCode) {
        int slot = stockSlotHasher.slotFor(stockCode);
        MatchingSlotTarget[] table = slotToTargets;
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
        return slotToTargets;
    }

    public void start() {
        List<PartitionInfo> partitionInfos = consumer.partitionsFor(MAP_TOPIC);
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
     * @throws IllegalStateException timeout 안에 못 끝나면(브로커 문제 등)
     */
    public void awaitInitialCatchUp(Duration timeout) {
        try {
            if (!initialCatchUpLatch.await(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
                throw new IllegalStateException(
                    MAP_TOPIC + " 초기 읽기가 " + timeout + " 안에 끝나지 않았습니다");
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
        } finally {
            // 정상 종료 경로에서도 대기 중인 awaitInitialCatchUp 호출자가 영원히 안 깨는 일이 없게 한다.
            initialCatchUpLatch.countDown();
        }
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
        MatchingSlotTarget[] updated = slotToTargets.clone();
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
                updated[slot] = MatchingSlotTarget.parse(record.value());
            } catch (IllegalArgumentException e) {
                // 형식이 깨진 값 하나 때문에 나머지 배정까지 잃지 않는다. 그 종목 slot만 목적지 없음으로
                // 둔다.
                log.log(Level.WARNING, "[목적지] " + MAP_TOPIC + "에서 읽을 수 없는 값: slot=" + slot
                    + " value=" + record.value() + " — 이 종목 slot은 목적지 없음으로 둔다");
                updated[slot] = null;
            }
        }
        slotToTargets = updated;
        log.log(Level.INFO, "[목적지] " + MAP_TOPIC + " " + records.count() + "건 반영");
    }
}
