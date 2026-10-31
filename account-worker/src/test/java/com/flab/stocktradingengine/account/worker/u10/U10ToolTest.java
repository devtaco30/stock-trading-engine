package com.flab.stocktradingengine.account.worker.u10;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import com.flab.stocktradingengine.account.worker.coordination.MatchingDestinationResolver;
import com.flab.stocktradingengine.aeron.StockSlotHasher;

/** U10 확인 스크립트가 쓰는 도구의 단위 시험. 브로커 없이 MockConsumer로 맵 토픽 읽기를 흉내 낸다. */
class U10ToolTest {

    private static final String MAP_TOPIC = MatchingDestinationResolver.MAP_TOPIC;

    private static MockConsumer<Integer, String> consumerWithPartitions(int partitionCount, long endOffset) {
        MockConsumer<Integer, String> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        List<PartitionInfo> infos = new ArrayList<>();
        Map<TopicPartition, Long> begin = new HashMap<>();
        Map<TopicPartition, Long> end = new HashMap<>();
        for (int partition = 0; partition < partitionCount; partition++) {
            infos.add(new PartitionInfo(MAP_TOPIC, partition, null, null, null));
            begin.put(new TopicPartition(MAP_TOPIC, partition), 0L);
            end.put(new TopicPartition(MAP_TOPIC, partition), endOffset);
        }
        consumer.updatePartitions(MAP_TOPIC, infos);
        consumer.updateBeginningOffsets(begin);
        consumer.updateEndOffsets(end);
        return consumer;
    }

    private static String run(java.util.function.Consumer<PrintStream> action) {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        PrintStream stream = new PrintStream(bytes, true, StandardCharsets.UTF_8);
        action.accept(stream);
        return bytes.toString(StandardCharsets.UTF_8);
    }

    @Test
    void 끝까지_읽지_못하면_WARN_PARTIAL_MAP_READ를_표준_오류_스트림에_남기고_읽은_만큼만_돌려준다() {
        MockConsumer<Integer, String> consumer = consumerWithPartitions(1, 5L);
        consumer.schedulePollTask(() -> {
            consumer.addRecord(new ConsumerRecord<>(MAP_TOPIC, 0, 0L, 0, "aeron:udp?endpoint=localhost:20020"));
            consumer.addRecord(new ConsumerRecord<>(MAP_TOPIC, 0, 1L, 0, "aeron:udp?endpoint=localhost:20020;moving"));
        });
        List<U10Tool.MapRecord> records = new ArrayList<>();
        String err = run(stream -> records.addAll(U10Tool.readMapTopic(consumer, 300L, stream)));
        assertEquals(2, records.size());
        assertTrue(err.contains("WARN_PARTIAL_MAP_READ"), err);
    }

    @Test
    void 끝까지_읽으면_경고를_남기지_않는다() {
        MockConsumer<Integer, String> consumer = consumerWithPartitions(1, 2L);
        consumer.schedulePollTask(() -> {
            consumer.addRecord(new ConsumerRecord<>(MAP_TOPIC, 0, 0L, 0, "aeron:udp?endpoint=localhost:20020"));
            consumer.addRecord(new ConsumerRecord<>(MAP_TOPIC, 0, 1L, 0, "aeron:udp?endpoint=localhost:20021"));
        });
        List<U10Tool.MapRecord> records = new ArrayList<>();
        String err = run(stream -> records.addAll(U10Tool.readMapTopic(consumer, 2_000L, stream)));
        assertEquals(2, records.size());
        assertFalse(err.contains("WARN_PARTIAL_MAP_READ"), err);
    }

    @Test
    void slot_of는_종목_코드마다_StockSlotHasher의_slot을_한_줄씩_돌려준다() {
        StockSlotHasher hasher = new StockSlotHasher(64);
        List<String> lines = U10Tool.slotLines(64, "000001", "000002", "000003");
        assertEquals(3, lines.size());
        assertEquals("SLOT 000001=" + hasher.slotFor("000001"), lines.get(0));
        assertEquals("SLOT 000003=" + hasher.slotFor("000003"), lines.get(2));
    }

    @Test
    void send_bulk_인자에서_표지_매수_옵션과_종목_코드를_가른다() {
        U10Tool.BulkTargets targets = U10Tool.parseBulkTargets(new String[] {
            "send-bulk", "ch", "90002", "SELL", "20000", "200", "p", "000001", "--sentinel-buy=90001:20000", "000002"}, 7);
        assertEquals(List.of("000001", "000002"), targets.stockCodes());
        assertEquals(90001L, targets.sentinelAccountId());
        assertEquals(20000L, targets.sentinelPrice());
    }

    @Test
    void send_bulk_인자에_표지_옵션이_없으면_표지를_보내지_않는다() {
        U10Tool.BulkTargets targets = U10Tool.parseBulkTargets(new String[] {
            "send-bulk", "ch", "90002", "SELL", "20000", "200", "p", "000001"}, 7);
        assertEquals(List.of("000001"), targets.stockCodes());
        assertEquals(0L, targets.sentinelAccountId());
    }

    @Test
    void 맵_토픽이_없으면_MAP_TOPIC_MISSING을_남기고_부분_읽기_경고와_구별한다() {
        MockConsumer<Integer, String> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        consumer.updatePartitions(MAP_TOPIC, List.of());
        String err = run(stream -> assertTrue(U10Tool.readMapTopic(consumer, 300L, stream).isEmpty()));
        assertTrue(err.contains("MAP_TOPIC_MISSING"), err);
        assertFalse(err.contains("WARN_PARTIAL_MAP_READ"), err);
    }

    @Test
    void send_bulk_순서는_종목마다_매도_N건_바로_뒤에_표지_매수이고_종목_순서를_지킨다() {
        U10Tool.BulkTargets targets = new U10Tool.BulkTargets(List.of("000001", "000002"), 90001L, 20000L, 1L);
        List<com.flab.stocktradingengine.codec.DecodedAccountOrder> sent = new ArrayList<>();
        U10Tool.bulkOrders(90002L, com.flab.stocktradingengine.trading.entity.OrderSide.SELL, 20000L, 3, "p", targets, sent::add);
        assertEquals(8, sent.size());
        List<String> ids = sent.stream().map(order -> order.requestId()).toList();
        assertEquals(List.of("p-000001-0", "p-000001-1", "p-000001-2", "p-sentinel-000001",
            "p-000002-0", "p-000002-1", "p-000002-2", "p-sentinel-000002"), ids);
        assertEquals(com.flab.stocktradingengine.trading.entity.OrderSide.BUY, sent.get(3).type());
        assertEquals(90001L, sent.get(3).accountId());
    }

    @Test
    void send_bulk_계좌_수를_주면_i번째_주문을_기준_계좌_더하기_i_나머지_계좌_수의_계좌로_보낸다() {
        U10Tool.BulkTargets targets = new U10Tool.BulkTargets(List.of("000001", "000002"), 90001L, 20000L, 1L);
        List<com.flab.stocktradingengine.codec.DecodedAccountOrder> sent = new ArrayList<>();
        int count = U10Tool.bulkOrders(90002L, com.flab.stocktradingengine.trading.entity.OrderSide.SELL, 20000L, 7, 3, "p", targets, sent::add);
        assertEquals(14, count);
        List<Long> sellAccounts = sent.stream()
            .filter(order -> order.type() == com.flab.stocktradingengine.trading.entity.OrderSide.SELL)
            .filter(order -> order.stockCode().equals("000001")).map(order -> order.accountId()).toList();
        assertEquals(List.of(90002L, 90003L, 90004L, 90002L, 90003L, 90004L, 90002L), sellAccounts);
        List<Long> secondStock = sent.stream()
            .filter(order -> order.type() == com.flab.stocktradingengine.trading.entity.OrderSide.SELL)
            .filter(order -> order.stockCode().equals("000002")).map(order -> order.accountId()).toList();
        assertEquals(sellAccounts, secondStock, "종목마다 i가 0부터 다시 시작해 같은 계좌 순서다");
        assertTrue(sent.stream().filter(order -> order.type() == com.flab.stocktradingengine.trading.entity.OrderSide.BUY)
            .allMatch(order -> order.accountId() == 90001L), "표지 매수는 계좌 수와 무관하게 표지 계좌로 간다");
        List<Long> prices = sent.stream().filter(order -> order.stockCode().equals("000001")
            && order.type() == com.flab.stocktradingengine.trading.entity.OrderSide.SELL).map(order -> order.price().longValue()).toList();
        assertEquals(List.of(20000L, 20001L, 20002L, 20003L, 20004L, 20005L, 20006L), prices, "가격은 계좌 수와 무관하게 첫 가격 + i다");
    }

    @Test
    void send_bulk_계좌_수_1이면_모든_주문이_기준_계좌로_간다() {
        U10Tool.BulkTargets targets = new U10Tool.BulkTargets(List.of("000001"), 0L, 0L, 1L);
        List<com.flab.stocktradingengine.codec.DecodedAccountOrder> sent = new ArrayList<>();
        U10Tool.bulkOrders(90002L, com.flab.stocktradingengine.trading.entity.OrderSide.SELL, 20000L, 4, 1, "p", targets, sent::add);
        assertTrue(sent.stream().allMatch(order -> order.accountId() == 90002L));
    }

    @Test
    void BULK_ACCOUNTS_값은_없으면_1이고_양의_정수가_아니면_거부한다() {
        assertEquals(1, U10Tool.parseBulkAccounts(null));
        assertEquals(1, U10Tool.parseBulkAccounts(""));
        assertEquals(5, U10Tool.parseBulkAccounts("5"));
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class, () -> U10Tool.parseBulkAccounts("0"));
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class, () -> U10Tool.parseBulkAccounts("-2"));
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class, () -> U10Tool.parseBulkAccounts("abc"));
    }

    @Test
    void U10_SENDER_WAIT_MS_값은_없으면_30초이고_양의_정수가_아니면_거부한다() {
        assertEquals(30_000L, U10Tool.parseSenderWaitMillis(null));
        assertEquals(30_000L, U10Tool.parseSenderWaitMillis(""));
        assertEquals(600_000L, U10Tool.parseSenderWaitMillis("600000"));
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class, () -> U10Tool.parseSenderWaitMillis("0"));
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class, () -> U10Tool.parseSenderWaitMillis("-5"));
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class, () -> U10Tool.parseSenderWaitMillis("abc"));
    }

    @Test
    void stream_계획은_종목마다_매도와_매수를_번갈아_정해진_속도로_내고_잔고_초과_주문을_한_건_섞는다() {
        U10Tool.StreamSpec spec = new U10Tool.StreamSpec(1_000L, 200, 20, "000001", "000002", 90002L, 90001L, 20000L);
        List<U10Tool.PlannedOrder> plan = U10Tool.streamPlan(spec);
        long xSells = plan.stream().filter(p -> p.order().stockCode().equals("000001") && p.order().accountId() == 90002L).count();
        long xBuys = plan.stream().filter(p -> p.order().stockCode().equals("000001") && p.order().accountId() == 90001L
            && p.order().quantity() == 1).count();
        long ySells = plan.stream().filter(p -> p.order().stockCode().equals("000002") && p.order().accountId() == 90002L).count();
        long invalid = plan.stream().filter(p -> p.order().requestId().equals("u10-flow-invalid")).count();
        assertEquals(100, xSells);
        assertEquals(100, xBuys);
        assertEquals(10, ySells);
        assertEquals(1, invalid);
        assertEquals(plan.size(), plan.stream().map(p -> p.order().requestId()).distinct().count(), "요청 번호가 모두 다르다");
        long previous = -1;
        for (U10Tool.PlannedOrder planned : plan) {
            assertTrue(planned.offsetMillis() >= previous && planned.offsetMillis() < 1_000L, "발신 시각 오프셋은 줄지 않고 기간 안이다");
            previous = planned.offsetMillis();
        }
    }

    @Test
    void send_bulk_가격_간격_옵션이_0이면_모든_매도가_같은_가격이다() {
        U10Tool.BulkTargets targets = U10Tool.parseBulkTargets(new String[] {
            "send-bulk", "ch", "90002", "SELL", "20000", "3", "p", "000001", "--price-step=0"}, 7);
        assertEquals(0L, targets.priceStep());
        List<com.flab.stocktradingengine.codec.DecodedAccountOrder> sent = new ArrayList<>();
        U10Tool.bulkOrders(90002L, com.flab.stocktradingengine.trading.entity.OrderSide.SELL, 20000L, 3, "p", targets, sent::add);
        assertEquals(List.of(20000L, 20000L, 20000L), sent.stream().map(order -> order.price().longValue()).toList());
    }

    private static org.agrona.concurrent.status.CountersManager newCounters() {
        return new org.agrona.concurrent.status.CountersManager(
            new org.agrona.concurrent.UnsafeBuffer(java.nio.ByteBuffer.allocateDirect(64 * 1024)),
            new org.agrona.concurrent.UnsafeBuffer(java.nio.ByteBuffer.allocateDirect(16 * 1024)),
            StandardCharsets.UTF_8);
    }

    private static int allocateSenderPosition(org.agrona.concurrent.status.CountersManager counters, long registrationId, long value) {
        int counterId = counters.allocate("snd-pos", io.aeron.AeronCounters.DRIVER_SENDER_POSITION_TYPE_ID,
            buffer -> buffer.putLong(0, registrationId));
        counters.setCounterValue(counterId, value);
        return counterId;
    }

    @Test
    void 송신_위치가_발행_위치보다_뒤에_있으면_기다리고_따라잡으면_REACHED를_돌려준다() throws Exception {
        org.agrona.concurrent.status.CountersManager counters = newCounters();
        int counterId = allocateSenderPosition(counters, 77L, 100L);
        Thread catchUp = new Thread(() -> {
            try {
                Thread.sleep(150);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            counters.setCounterValue(counterId, 5_000L);
        });
        catchUp.start();
        long started = System.currentTimeMillis();
        assertEquals(U10Tool.SenderWait.REACHED, U10Tool.awaitSenderPosition(counters, 77L, 5_000L, 5_000L));
        assertTrue(System.currentTimeMillis() - started >= 100L, "송신 위치가 따라잡기 전에는 돌아오지 않는다");
        catchUp.join();
    }

    @Test
    void 송신_위치가_끝내_발행_위치에_닿지_못하면_NOT_REACHED다() {
        org.agrona.concurrent.status.CountersManager counters = newCounters();
        allocateSenderPosition(counters, 77L, 100L);
        assertEquals(U10Tool.SenderWait.NOT_REACHED, U10Tool.awaitSenderPosition(counters, 77L, 5_000L, 300L));
    }

    @Test
    void 이_발행의_송신_위치_카운터를_찾지_못하면_송신_위치에_못_닿은_것과_구별한다() {
        org.agrona.concurrent.status.CountersManager counters = newCounters();
        allocateSenderPosition(counters, 12L, 9_999L);
        assertEquals(U10Tool.SenderWait.COUNTER_NOT_FOUND, U10Tool.awaitSenderPosition(counters, 77L, 100L, 300L), "다른 발행의 카운터가 충분히 커도 이 발행으로 세지 않는다");
    }
}
