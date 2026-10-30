package com.flab.stocktradingengine.matching.worker.coordination;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import com.flab.stocktradingengine.aeron.MatchingSlotTarget;

/** 맵 토픽의 값을 종목 slot별 {@link MatchingSlotTarget}으로 읽는다(11-13절 2번). */
class MatchingSlotMapReaderTest {

    private static final String TOPIC = "matching-shard-map";
    private static final String X = "aeron:udp?endpoint=localhost:20020";

    private final MatchingSlotMapReader reader = new MatchingSlotMapReader(mock(Consumer.class), 64, TOPIC);

    @Test
    void 평상시_값과_이동_중_값을_읽는다() {
        reader.applyRecords(records(record(3, X), record(5, X + ";moving")));

        assertThat(reader.targetFor(3)).contains(new MatchingSlotTarget(X, false));
        assertThat(reader.targetFor(5)).contains(new MatchingSlotTarget(X, true));
        assertThat(reader.targetFor(4)).isEmpty();
    }

    @Test
    void 같은_종목_slot의_나중_값이_앞선_값을_대체한다() {
        reader.applyRecords(records(record(5, X), record(5, X + ";moving")));

        assertThat(reader.targetFor(5)).contains(new MatchingSlotTarget(X, true));
    }

    @Test
    void 값이_null이면_항목이_없다() {
        reader.applyRecords(records(record(5, X)));
        reader.applyRecords(records(record(5, null)));

        assertThat(reader.targetFor(5)).isEmpty();
    }

    @Test
    void 읽을_수_없는_값과_범위_밖_종목_slot은_그_항목만_건너뛴다() {
        reader.applyRecords(records(record(3, X), record(5, "  "), record(99, X)));

        assertThat(reader.targetFor(3)).contains(new MatchingSlotTarget(X, false));
        assertThat(reader.targetFor(5)).isEmpty();
        assertThat(reader.targetFor(99)).isEmpty();
    }

    @Test
    void 반영한_항목마다_관찰자에게_종목_slot과_새_값을_알린다() {
        List<String> seen = new java.util.ArrayList<>();
        reader.addListener((slot, target) -> seen.add(slot + ":" + target.map(MatchingSlotTarget::encode).orElse("없음")));

        reader.applyRecords(records(record(3, X), record(5, X + ";moving"), record(3, null)));

        assertThat(seen).containsExactly("3:" + X, "5:" + X + ";moving", "3:없음");
    }

    @Test
    void 읽는_thread가_예외로_끝나면_초기_읽기_대기가_실패로_끝난다() {
        Consumer<Integer, String> failing = mock(Consumer.class);
        TopicPartition partition = new TopicPartition(TOPIC, 0);
        org.mockito.Mockito.when(failing.partitionsFor(TOPIC))
            .thenReturn(List.of(new org.apache.kafka.common.PartitionInfo(TOPIC, 0, null, null, null)));
        org.mockito.Mockito.when(failing.endOffsets(List.of(partition))).thenReturn(Map.of(partition, 10L));
        org.mockito.Mockito.when(failing.poll(org.mockito.ArgumentMatchers.any(java.time.Duration.class)))
            .thenThrow(new IllegalStateException("브로커 연결 실패"));
        MatchingSlotMapReader failingReader = new MatchingSlotMapReader(failing, 64, TOPIC);

        failingReader.start();

        org.assertj.core.api.Assertions.assertThatThrownBy(() -> failingReader.awaitInitialCatchUp(java.time.Duration.ofSeconds(3)))
            .isInstanceOf(IllegalStateException.class).hasMessageContaining("비정상 종료");
    }

    @Test
    void 읽는_thread가_예외로_끝나면_낡은_값을_돌려주지_않고_알_수_없음으로_본다() throws Exception {
        Consumer<Integer, String> failing = mock(Consumer.class);
        TopicPartition partition = new TopicPartition(TOPIC, 0);
        org.mockito.Mockito.when(failing.partitionsFor(TOPIC))
            .thenReturn(List.of(new org.apache.kafka.common.PartitionInfo(TOPIC, 0, null, null, null)));
        org.mockito.Mockito.when(failing.endOffsets(List.of(partition))).thenReturn(Map.of(partition, 10L));
        org.mockito.Mockito.when(failing.poll(org.mockito.ArgumentMatchers.any(java.time.Duration.class)))
            .thenThrow(new IllegalStateException("브로커 연결 실패"));
        MatchingSlotMapReader failingReader = new MatchingSlotMapReader(failing, 64, TOPIC);
        failingReader.setRestartIntervalMillis(10_000L); // 이 시험에서는 다시 시작하기 전의 상태를 본다
        failingReader.applyRecords(records(record(3, X + ";moving")));
        assertThat(failingReader.targetFor(3)).isPresent();

        failingReader.start();
        awaitUntil(() -> !failingReader.isHealthy());

        assertThat(failingReader.targetFor(3)).as("죽은 읽기가 마지막으로 읽은 낡은 값을 돌려주면 이동 중이 아니라는 잘못된 판정이 나온다").isEmpty();
    }

    @Test
    void 읽는_thread가_예외로_끝나도_간격을_두고_다시_시작해_정상으로_돌아오면_읽은_값이_다시_보인다() throws Exception {
        Consumer<Integer, String> flaky = mock(Consumer.class);
        TopicPartition partition = new TopicPartition(TOPIC, 0);
        org.mockito.Mockito.when(flaky.partitionsFor(TOPIC))
            .thenReturn(List.of(new org.apache.kafka.common.PartitionInfo(TOPIC, 0, null, null, null)));
        org.mockito.Mockito.when(flaky.endOffsets(List.of(partition))).thenReturn(Map.of(partition, 0L));
        org.mockito.Mockito.when(flaky.position(partition)).thenReturn(0L);
        org.mockito.Mockito.when(flaky.poll(org.mockito.ArgumentMatchers.any(java.time.Duration.class)))
            .thenThrow(new IllegalStateException("일시적 브로커 오류"))
            .thenReturn(ConsumerRecords.empty());
        MatchingSlotMapReader flakyReader = new MatchingSlotMapReader(flaky, 64, TOPIC);
        flakyReader.setRestartIntervalMillis(300L);
        flakyReader.applyRecords(records(record(3, X + ";moving")));

        flakyReader.start();
        awaitUntil(() -> !flakyReader.isHealthy()); // 먼저 멈춘 상태가 관찰된다
        assertThat(flakyReader.targetFor(3)).isEmpty();
        awaitUntil(() -> flakyReader.isHealthy() && flakyReader.targetFor(3).isPresent());

        flakyReader.close();
    }

    @Test
    void 다시_시작을_요청하면_멈춘_상태가_되었다가_간격이_지나_정상으로_돌아온다() throws Exception {
        Consumer<Integer, String> consumer = mock(Consumer.class);
        TopicPartition partition = new TopicPartition(TOPIC, 0);
        org.mockito.Mockito.when(consumer.partitionsFor(TOPIC))
            .thenReturn(List.of(new org.apache.kafka.common.PartitionInfo(TOPIC, 0, null, null, null)));
        org.mockito.Mockito.when(consumer.endOffsets(List.of(partition))).thenReturn(Map.of(partition, 0L));
        org.mockito.Mockito.when(consumer.position(partition)).thenReturn(0L);
        org.mockito.Mockito.when(consumer.poll(org.mockito.ArgumentMatchers.any(java.time.Duration.class)))
            .thenReturn(ConsumerRecords.empty());
        MatchingSlotMapReader quietReader = new MatchingSlotMapReader(consumer, 64, TOPIC);
        quietReader.setRestartIntervalMillis(300L);
        quietReader.applyRecords(records(record(3, X + ";moving")));
        quietReader.start();
        awaitUntil(quietReader::isHealthy);

        quietReader.requestRestart("끝 위치를 따라오지 못함");

        awaitUntil(() -> !quietReader.isHealthy());
        assertThat(quietReader.targetFor(3)).isEmpty();
        awaitUntil(quietReader::isHealthy);
        assertThat(quietReader.targetFor(3)).isPresent();
        quietReader.close();
    }

    @Test
    void 쉬는_동안_다시_온_재시작_요청은_이미_반영된_것으로_보고_다시_쉬지_않는다() throws Exception {
        Consumer<Integer, String> consumer = mock(Consumer.class);
        TopicPartition partition = new TopicPartition(TOPIC, 0);
        org.mockito.Mockito.when(consumer.partitionsFor(TOPIC))
            .thenReturn(List.of(new org.apache.kafka.common.PartitionInfo(TOPIC, 0, null, null, null)));
        org.mockito.Mockito.when(consumer.endOffsets(List.of(partition))).thenReturn(Map.of(partition, 0L));
        org.mockito.Mockito.when(consumer.position(partition)).thenReturn(0L);
        org.mockito.Mockito.when(consumer.poll(org.mockito.ArgumentMatchers.any(java.time.Duration.class)))
            .thenReturn(ConsumerRecords.empty());
        MatchingSlotMapReader quietReader = new MatchingSlotMapReader(consumer, 64, TOPIC);
        quietReader.setRestartIntervalMillis(300L);
        quietReader.start();
        awaitUntil(quietReader::isHealthy);
        long requestedAt = System.nanoTime();
        quietReader.requestRestart("첫 요청");
        awaitUntil(() -> !quietReader.isHealthy());

        quietReader.requestRestart("쉬는 동안 다시 온 요청");
        awaitUntil(quietReader::isHealthy);
        long unhealthyMillis = (System.nanoTime() - requestedAt) / 1_000_000L;

        assertThat(unhealthyMillis).as("쉬는 간격(300ms) 한 번만 멈춘다. 이미 반영한 요청 때문에 다시 쉬면 600ms를 넘는다").isLessThan(500L);
        quietReader.close();
    }

    private static final String Y = "aeron:udp?endpoint=localhost:20030";

    /** 위치 0에서 시작해 한 번 poll로 주어진 기록을 모두 읽으면 끝 위치(기록 수)에 닿는 가짜 consumer. */
    @SuppressWarnings("unchecked")
    private static Consumer<Integer, String> replayConsumer(ConsumerRecords<Integer, String> firstPoll, long endOffset) {
        Consumer<Integer, String> consumer = mock(Consumer.class);
        TopicPartition partition = new TopicPartition(TOPIC, 0);
        org.mockito.Mockito.when(consumer.partitionsFor(TOPIC))
            .thenReturn(List.of(new org.apache.kafka.common.PartitionInfo(TOPIC, 0, null, null, null)));
        org.mockito.Mockito.when(consumer.endOffsets(List.of(partition))).thenReturn(Map.of(partition, endOffset));
        java.util.concurrent.atomic.AtomicBoolean polled = new java.util.concurrent.atomic.AtomicBoolean();
        org.mockito.Mockito.when(consumer.position(partition)).thenAnswer(invocation -> polled.get() ? endOffset : 0L);
        org.mockito.Mockito.when(consumer.poll(org.mockito.ArgumentMatchers.any(java.time.Duration.class)))
            .thenAnswer(invocation -> {
                if (polled.compareAndSet(false, true)) {
                    return firstPoll;
                }
                return ConsumerRecords.empty();
            });
        return consumer;
    }

    @Test
    void 다시_시작하면_옛_consumer를_닫고_새로_만들어_처음부터_다시_읽는다() throws Exception {
        Consumer<Integer, String> first = replayConsumer(ConsumerRecords.empty(), 0L);
        Consumer<Integer, String> second = replayConsumer(records(record(3, X)), 1L);
        java.util.Iterator<Consumer<Integer, String>> consumers = List.of(first, second).iterator();
        MatchingSlotMapReader supplied = new MatchingSlotMapReader(consumers::next, 64, TOPIC);
        supplied.setRestartIntervalMillis(100L);
        supplied.start();
        awaitUntil(supplied::isHealthy);

        supplied.requestRestart("끝 위치를 따라오지 못함");

        awaitUntil(() -> !supplied.isHealthy());
        awaitUntil(() -> supplied.isHealthy() && supplied.targetFor(3).isPresent());
        org.mockito.Mockito.verify(first).close();
        org.mockito.Mockito.verify(second).seekToBeginning(List.of(new TopicPartition(TOPIC, 0)));
        assertThat(supplied.targetFor(3)).contains(new MatchingSlotTarget(X, false));
        supplied.close();
    }

    @Test
    void 다시_읽는_동안에는_관찰자를_부르지_않고_따라온_뒤_값이_달라진_slot만_한_번씩_알린다() throws Exception {
        Consumer<Integer, String> first = replayConsumer(ConsumerRecords.empty(), 0L);
        // 처음부터 다시 읽으면 slot 5는 X, X;moving, Y를 차례로 재생하지만 최종 값은 이미 알고 있던 Y와 같다. slot 3만 새로 생긴다.
        Consumer<Integer, String> second = replayConsumer(
            records(record(5, X), record(5, X + ";moving"), record(5, Y), record(3, X)), 4L);
        java.util.Iterator<Consumer<Integer, String>> consumers = List.of(first, second).iterator();
        MatchingSlotMapReader supplied = new MatchingSlotMapReader(consumers::next, 64, TOPIC);
        supplied.setRestartIntervalMillis(100L);
        supplied.applyRecords(records(record(5, Y)));
        List<String> seen = new java.util.concurrent.CopyOnWriteArrayList<>();
        supplied.addListener((slot, target) -> seen.add(slot + ":" + target.map(MatchingSlotTarget::encode).orElse("없음")));
        supplied.start();
        awaitUntil(supplied::isHealthy);

        supplied.requestRestart("끝 위치를 따라오지 못함");

        awaitUntil(() -> !supplied.isHealthy());
        awaitUntil(() -> supplied.isHealthy() && supplied.targetFor(3).isPresent());
        assertThat(seen).containsExactly("3:" + X);
        supplied.close();
    }

    @Test
    void 다시_읽는_중_새_consumer_생성이_예외면_낡은_값을_막은_채_간격_뒤_다시_만든다() throws Exception {
        Consumer<Integer, String> first = replayConsumer(ConsumerRecords.empty(), 0L);
        Consumer<Integer, String> second = replayConsumer(records(record(3, X)), 1L);
        java.util.concurrent.atomic.AtomicInteger calls = new java.util.concurrent.atomic.AtomicInteger();
        MatchingSlotMapReader supplied = new MatchingSlotMapReader(() -> {
            int call = calls.incrementAndGet();
            if (call == 1) {
                return first;
            }
            if (call == 2) {
                throw new IllegalStateException("브로커에 연결할 수 없음");
            }
            return second;
        }, 64, TOPIC);
        supplied.setRestartIntervalMillis(100L);
        supplied.applyRecords(records(record(3, Y)));
        supplied.start();
        awaitUntil(supplied::isHealthy);

        supplied.requestRestart("끝 위치를 따라오지 못함");

        awaitUntil(() -> calls.get() >= 2);
        assertThat(supplied.isHealthy()).as("새 consumer 생성이 실패한 동안 정상으로 보이면 안 된다").isFalse();
        assertThat(supplied.targetFor(3)).as("낡은 값을 돌려주지 않는다").isEmpty();
        awaitUntil(() -> supplied.isHealthy() && supplied.targetFor(3).isPresent());
        assertThat(calls.get()).isEqualTo(3);
        assertThat(supplied.targetFor(3)).contains(new MatchingSlotTarget(X, false));
        supplied.close();
    }

    @Test
    void 옛_중간_값이_다시_재생돼도_보관본에_정리가_요청되지_않는다() throws Exception {
        // slot 5는 X(이 pod)가 Y에게 넘겨 보관했다가 도로 받았다. 맵의 최종 값은 X이고, 토픽에는 옛 중간 값 Y가 아직 남아 있다.
        com.flab.stocktradingengine.matching.worker.handover.HandoverBookStore store =
            new com.flab.stocktradingengine.matching.worker.handover.HandoverBookStore();
        store.put(5, List.of(new byte[] {1}));
        List<Integer> cleanupRequests = new java.util.concurrent.CopyOnWriteArrayList<>();
        com.flab.stocktradingengine.matching.worker.handover.HandoverCleanupTrigger trigger =
            new com.flab.stocktradingengine.matching.worker.handover.HandoverCleanupTrigger(X, store, cleanupRequests::add);
        Consumer<Integer, String> first = replayConsumer(ConsumerRecords.empty(), 0L);
        Consumer<Integer, String> second = replayConsumer(records(record(5, Y), record(5, X)), 2L);
        java.util.Iterator<Consumer<Integer, String>> consumers = List.of(first, second).iterator();
        MatchingSlotMapReader supplied = new MatchingSlotMapReader(consumers::next, 64, TOPIC);
        supplied.setRestartIntervalMillis(100L);
        supplied.applyRecords(records(record(5, X)));
        supplied.addListener(trigger::onMapChange);
        supplied.start();
        awaitUntil(supplied::isHealthy);

        supplied.requestRestart("끝 위치를 따라오지 못함");

        awaitUntil(() -> !supplied.isHealthy());
        awaitUntil(supplied::isHealthy);
        assertThat(supplied.targetFor(5)).contains(new MatchingSlotTarget(X, false));
        assertThat(cleanupRequests).isEmpty();
        supplied.close();
    }

    private static void awaitUntil(java.util.function.BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + 3_000_000_000L;
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        assertThat(condition.getAsBoolean()).as("3초 안에 조건이 충족되지 않음").isTrue();
    }

    @Test
    void 읽기를_정상으로_닫으면_초기_읽기_대기는_실패하지_않는다() {
        Consumer<Integer, String> consumer = mock(Consumer.class);
        TopicPartition partition = new TopicPartition(TOPIC, 0);
        org.mockito.Mockito.when(consumer.partitionsFor(TOPIC))
            .thenReturn(List.of(new org.apache.kafka.common.PartitionInfo(TOPIC, 0, null, null, null)));
        org.mockito.Mockito.when(consumer.endOffsets(List.of(partition))).thenReturn(Map.of(partition, 0L));
        org.mockito.Mockito.when(consumer.poll(org.mockito.ArgumentMatchers.any(java.time.Duration.class)))
            .thenReturn(ConsumerRecords.empty());
        org.mockito.Mockito.when(consumer.position(partition)).thenReturn(0L);
        MatchingSlotMapReader okReader = new MatchingSlotMapReader(consumer, 64, TOPIC);

        okReader.start();
        okReader.awaitInitialCatchUp(java.time.Duration.ofSeconds(3));
        okReader.close();
    }

    @Test
    void 토픽을_만든_직후_파티션_정보가_비어_있어도_나타날_때까지_다시_물어본_뒤_읽기를_시작한다() {
        Consumer<Integer, String> consumer = mock(Consumer.class);
        TopicPartition partition = new TopicPartition(TOPIC, 0);
        org.mockito.Mockito.when(consumer.partitionsFor(TOPIC))
            .thenReturn(List.of())
            .thenReturn(List.of())
            .thenReturn(List.of(new org.apache.kafka.common.PartitionInfo(TOPIC, 0, null, null, null)));
        org.mockito.Mockito.when(consumer.endOffsets(List.of(partition))).thenReturn(Map.of(partition, 0L));
        org.mockito.Mockito.when(consumer.poll(org.mockito.ArgumentMatchers.any(java.time.Duration.class)))
            .thenReturn(ConsumerRecords.empty());
        org.mockito.Mockito.when(consumer.position(partition)).thenReturn(0L);
        MatchingSlotMapReader lateTopicReader = new MatchingSlotMapReader(consumer, 64, TOPIC);

        lateTopicReader.start();
        lateTopicReader.awaitInitialCatchUp(java.time.Duration.ofSeconds(3));
        lateTopicReader.close();

        org.mockito.Mockito.verify(consumer).assign(List.of(partition));
    }

    @Test
    void 파티션_정보가_끝내_나타나지_않으면_시작이_실패한다() {
        Consumer<Integer, String> consumer = mock(Consumer.class);
        org.mockito.Mockito.when(consumer.partitionsFor(TOPIC)).thenReturn(List.of());
        MatchingSlotMapReader neverReader = new MatchingSlotMapReader(consumer, 64, TOPIC);
        neverReader.setPartitionWaitMillis(200L);

        org.assertj.core.api.Assertions.assertThatThrownBy(neverReader::start)
            .isInstanceOf(IllegalStateException.class).hasMessageContaining("파티션");
    }

    @SafeVarargs
    private static ConsumerRecords<Integer, String> records(ConsumerRecord<Integer, String>... records) {
        return new ConsumerRecords<>(Map.of(new TopicPartition(TOPIC, 0), List.of(records)));
    }

    private static ConsumerRecord<Integer, String> record(int slot, String value) {
        return new ConsumerRecord<>(TOPIC, 0, 0L, slot, value);
    }
}
