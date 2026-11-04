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
    void send_bulk_수량_옵션은_없으면_1이고_양의_정수가_아니면_거부한다() {
        U10Tool.BulkTargets none = U10Tool.parseBulkTargets(new String[] {
            "send-bulk", "ch", "90002", "SELL", "20000", "200", "p", "000001"}, 7);
        assertEquals(1, none.quantity());
        U10Tool.BulkTargets seven = U10Tool.parseBulkTargets(new String[] {
            "send-bulk", "ch", "90002", "SELL", "20000", "200", "p", "--quantity=7", "000001"}, 7);
        assertEquals(7, seven.quantity());
        assertEquals(List.of("000001"), seven.stockCodes());
        for (String bad : new String[] {"--quantity=0", "--quantity=-3", "--quantity=abc"}) {
            org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class, () -> U10Tool.parseBulkTargets(new String[] {
                "send-bulk", "ch", "90002", "SELL", "20000", "200", "p", bad, "000001"}, 7), bad);
        }
    }

    @Test
    void send_bulk는_수량_옵션의_수량으로_매도를_보내고_수량이_없으면_1이다() {
        List<com.flab.stocktradingengine.codec.DecodedAccountOrder> sent = new ArrayList<>();
        U10Tool.BulkTargets targets = new U10Tool.BulkTargets(List.of("000001"), 0L, 0L, 1L, 9);
        U10Tool.bulkOrders(90002L, com.flab.stocktradingengine.trading.entity.OrderSide.SELL, 20000L, 4, 2, "p", targets, sent::add);
        assertEquals(4, sent.size());
        assertTrue(sent.stream().allMatch(order -> order.quantity() == 9));
        List<com.flab.stocktradingengine.codec.DecodedAccountOrder> defaults = new ArrayList<>();
        U10Tool.bulkOrders(90002L, com.flab.stocktradingengine.trading.entity.OrderSide.SELL, 20000L, 2, 1, "q",
            new U10Tool.BulkTargets(List.of("000001"), 0L, 0L, 1L), defaults::add);
        assertTrue(defaults.stream().allMatch(order -> order.quantity() == 1));
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

    // ---- 체결 섞기(mix) 계획: 매도 1건 직후 매수 1건, 같은 가격, 매수 1건이 가장 오래된 매도 1건을 체결시킨다 ----

    private static U10Tool.MixSpec mixSpec(int sellAccounts, int buyAccounts, int prefillPerStock, int pairsPerStock) {
        return new U10Tool.MixSpec(90002L, sellAccounts, 90002L + sellAccounts, buyAccounts, 20_000L, prefillPerStock, pairsPerStock,
            List.of("000001", "000002", "000003"), "mix");
    }

    private record MixedOrder(long sequence, com.flab.stocktradingengine.codec.DecodedAccountOrder order) {
    }

    private static List<MixedOrder> collectMix(U10Tool.MixSpec spec) {
        List<MixedOrder> orders = new ArrayList<>();
        U10Tool.mixOrders(spec, (sequence, order) -> orders.add(new MixedOrder(sequence, order)));
        return orders;
    }

    @Test
    void mix_순서는_쌍마다_종목_순서로_매도_바로_뒤에_매수이고_번호는_0부터_하나씩_늘며_가격과_수량이_같다() {
        U10Tool.MixSpec spec = mixSpec(4, 2, 8, 5);
        List<MixedOrder> orders = collectMix(spec);

        assertEquals(2 * 3 * 5, orders.size(), "쌍 수 x 종목 3 x 2건");
        java.util.Set<String> requestIds = new java.util.HashSet<>();
        for (int n = 0; n < orders.size(); n++) {
            MixedOrder mixed = orders.get(n);
            assertEquals(n, mixed.sequence());
            assertEquals(n % 2 == 0 ? com.flab.stocktradingengine.trading.entity.OrderSide.SELL : com.flab.stocktradingengine.trading.entity.OrderSide.BUY,
                mixed.order().type(), "짝수 번째는 매도, 홀수 번째는 매수");
            assertEquals(spec.stockCodes().get((n / 2) % 3), mixed.order().stockCode(), "한 쌍은 같은 종목이고 쌍마다 종목 순서를 돈다");
            assertEquals(0, java.math.BigDecimal.valueOf(20_000L).compareTo(mixed.order().price()));
            assertEquals(1, mixed.order().quantity());
            assertTrue(requestIds.add(mixed.order().requestId()), "요청번호는 겹치지 않는다: " + mixed.order().requestId());
        }
    }

    @Test
    void mix_매도는_쌓은_수_다음_번호부터_매도_계좌를_돌고_매수는_매수_계좌를_돈다() {
        U10Tool.MixSpec spec = mixSpec(4, 2, 8, 5);
        List<MixedOrder> orders = collectMix(spec);

        int buyCounter = 0;
        for (int n = 0; n < orders.size(); n++) {
            int pair = n / 2;
            int stockIndex = pair % 3;
            int pairInStock = pair / 3;
            long accountId = orders.get(n).order().accountId();
            if (n % 2 == 0) {
                assertEquals(90002L + ((8 + pairInStock) % 4), accountId, "종목 안 j번째 쌍의 매도는 쌓은 수 8 + j번 매도: 계좌 90002 + (8 + j) % 4");
            } else {
                assertEquals(90006L + (buyCounter % 2), accountId, "매수는 매수 계좌 90006, 90007을 번갈아 쓴다");
                buyCounter++;
            }
        }
    }

    @Test
    void mix_쌓은_뒤_모든_쌍을_체결_순서대로_적용하면_쌍이_끝날_때마다_계좌별_종목별_미체결이_처음과_같다() {
        int sellAccounts = 4;
        int prefillPerStock = 8;
        U10Tool.MixSpec spec = mixSpec(sellAccounts, 2, prefillPerStock, 37);
        Map<String, java.util.ArrayDeque<Long>> asksByStock = new HashMap<>();
        for (String stock : spec.stockCodes()) {
            java.util.ArrayDeque<Long> asks = new java.util.ArrayDeque<>();
            for (int i = 0; i < prefillPerStock; i++) {
                asks.addLast(90002L + (i % sellAccounts));
            }
            asksByStock.put(stock, asks);
        }

        Map<String, Map<Long, Integer>> openByStockAndAccount = new HashMap<>();
        for (String stock : spec.stockCodes()) {
            Map<Long, Integer> open = new HashMap<>();
            for (long account : asksByStock.get(stock)) {
                open.merge(account, 1, Integer::sum);
            }
            openByStockAndAccount.put(stock, open);
        }
        Map<String, Map<Long, Integer>> initial = new HashMap<>();
        openByStockAndAccount.forEach((stock, open) -> initial.put(stock, new HashMap<>(open)));

        List<MixedOrder> orders = collectMix(spec);
        for (int n = 0; n < orders.size(); n++) {
            com.flab.stocktradingengine.codec.DecodedAccountOrder order = orders.get(n).order();
            java.util.ArrayDeque<Long> asks = asksByStock.get(order.stockCode());
            Map<Long, Integer> open = openByStockAndAccount.get(order.stockCode());
            if (order.type() == com.flab.stocktradingengine.trading.entity.OrderSide.SELL) {
                asks.addLast(order.accountId());
                open.merge(order.accountId(), 1, Integer::sum);
            } else {
                long filledSeller = asks.pollFirst();
                open.merge(filledSeller, -1, Integer::sum);
            }
            if (n % 2 == 1) {
                assertEquals(initial.get(order.stockCode()), open, "쌍 " + (n / 2) + " 뒤 " + order.stockCode() + "의 계좌별 미체결이 처음과 달라졌다");
            }
        }
    }

    @Test
    void mix_쌓은_수가_매도_계좌_수의_배수가_아니거나_인자가_양수가_아니면_거부한다() {
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class, () -> collectMix(mixSpec(4, 2, 9, 5)), "쌓은 수 9는 매도 계좌 4의 배수가 아니다");
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class, () -> collectMix(mixSpec(0, 2, 8, 5)));
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class, () -> collectMix(mixSpec(4, 0, 8, 5)));
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class, () -> collectMix(mixSpec(4, 2, 8, 0)));
    }

    @Test
    void send_paced_인자에서_계획과_초당_주문_수를_읽고_종목_코드를_가른다() {
        U10Tool.PacedArgs paced = U10Tool.parsePacedArgs(new String[] {"send-paced", "aeron:udp?endpoint=localhost:20040", "90002", "300", "90302", "30",
            "20000", "200100", "600", "4000", "mix", "000001", "000002", "000003"});

        assertEquals("aeron:udp?endpoint=localhost:20040", paced.channel());
        assertEquals(4_000L, paced.ordersPerSecond());
        U10Tool.MixSpec spec = paced.spec();
        assertEquals(90002L, spec.sellAccountBase());
        assertEquals(300, spec.sellAccounts());
        assertEquals(90302L, spec.buyAccountBase());
        assertEquals(30, spec.buyAccounts());
        assertEquals(20_000L, spec.price());
        assertEquals(200_100, spec.prefillPerStock());
        assertEquals(600, spec.pairsPerStock());
        assertEquals("mix", spec.prefix());
        assertEquals(List.of("000001", "000002", "000003"), spec.stockCodes());
    }

    @Test
    void send_paced는_초당_주문_수가_양수가_아니거나_종목이_없으면_거부한다() {
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class, () -> U10Tool.parsePacedArgs(new String[] {"send-paced", "c", "1", "2", "3", "4",
            "20000", "8", "5", "0", "mix", "000001"}));
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class, () -> U10Tool.parsePacedArgs(new String[] {"send-paced", "c", "1", "2", "3", "4",
            "20000", "8", "5", "100", "mix"}));
    }

    @Test
    void 주문_번호의_목표_시각은_번호_곱하기_10억_나누기_초당_주문_수_나노초다() {
        assertEquals(0L, U10Tool.targetOffsetNanos(0, 4_000L));
        assertEquals(250_000L, U10Tool.targetOffsetNanos(1, 4_000L));
        assertEquals(1_000_000_000L, U10Tool.targetOffsetNanos(4_000, 4_000L));
        assertEquals(1_200_000_000_000L, U10Tool.targetOffsetNanos(4_800_000L, 4_000L), "480만 번째 주문은 1200초 지점");
    }

    // ---- 계좌 shard 둘로 나눠 보내기: 계좌 슬롯(256칸)의 앞 절반은 첫 채널, 뒤 절반은 둘째 채널 ----

    private static long firstAccountInSlotRange(int from, int to) {
        com.flab.stocktradingengine.aeron.SlotHasher hasher = new com.flab.stocktradingengine.aeron.SlotHasher(256);
        for (long accountId = 90_001L; ; accountId++) {
            int slot = hasher.slotFor(accountId);
            if (slot >= from && slot <= to) {
                return accountId;
            }
        }
    }

    @Test
    void shardOf_슬롯_0부터_127은_첫_채널_128부터_255는_둘째_채널이다() {
        long low = firstAccountInSlotRange(0, 127);
        long high = firstAccountInSlotRange(128, 255);
        assertEquals(0, U10Tool.shardOf(low, 2));
        assertEquals(1, U10Tool.shardOf(high, 2));
    }

    @Test
    void shardOf_채널이_하나면_어떤_계좌든_첫_채널이다() {
        for (long accountId = 90_001L; accountId < 90_400L; accountId++) {
            assertEquals(0, U10Tool.shardOf(accountId, 1));
        }
    }

    @Test
    void shardOf_채널_수가_양수가_아니거나_256을_넘으면_거부한다() {
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class, () -> U10Tool.shardOf(90_001L, 0));
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class, () -> U10Tool.shardOf(90_001L, 257));
    }

    @Test
    void parseChannels_쉼표로_나누고_빈_항목이나_빈_문자열은_거부한다() {
        assertEquals(List.of("aeron:udp?endpoint=localhost:20040"), U10Tool.parseChannels("aeron:udp?endpoint=localhost:20040"));
        assertEquals(List.of("aeron:udp?endpoint=localhost:20040", "aeron:udp?endpoint=localhost:20041"),
            U10Tool.parseChannels("aeron:udp?endpoint=localhost:20040, aeron:udp?endpoint=localhost:20041"));
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class, () -> U10Tool.parseChannels(""));
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class, () -> U10Tool.parseChannels("a,,b"));
    }

    @Test
    void 나눠_보내도_주문은_하나도_빠지지_않고_계좌마다_한_채널로만_가며_채널_안_계좌별_순서는_원래대로다() {
        U10Tool.MixSpec spec = mixSpec(300, 30, 300, 40);
        List<MixedOrder> all = collectMix(spec);
        List<List<com.flab.stocktradingengine.codec.DecodedAccountOrder>> channels = List.of(new ArrayList<>(), new ArrayList<>());
        Map<Long, Integer> channelOfAccount = new HashMap<>();
        for (MixedOrder mixed : all) {
            int channel = U10Tool.shardOf(mixed.order().accountId(), 2);
            channels.get(channel).add(mixed.order());
            Integer seen = channelOfAccount.put(mixed.order().accountId(), channel);
            assertTrue(seen == null || seen == channel, "계좌 " + mixed.order().accountId() + "의 주문이 두 채널로 갔다");
        }
        assertEquals(all.size(), channels.get(0).size() + channels.get(1).size());
        assertFalse(channels.get(0).isEmpty());
        assertFalse(channels.get(1).isEmpty());
        for (long accountId : channelOfAccount.keySet()) {
            List<String> globalOrder = all.stream().filter(m -> m.order().accountId() == accountId).map(m -> m.order().requestId()).toList();
            List<String> channelOrder = channels.get(channelOfAccount.get(accountId)).stream()
                .filter(o -> o.accountId() == accountId).map(o -> o.requestId()).toList();
            assertEquals(globalOrder, channelOrder, "계좌 " + accountId + "의 채널 안 순서");
        }
    }

    @Test
    void 쌓기_뒤_채널로_나눈_주문도_계좌별_종목별_쌓은_수는_계좌_수로_나눈_몫과_같다() {
        U10Tool.MixSpec spec = mixSpec(300, 30, 300, 1);
        Map<String, Integer> prefilled = new HashMap<>();
        List<com.flab.stocktradingengine.codec.DecodedAccountOrder> bulk = new ArrayList<>();
        U10Tool.bulkOrders(90_002L, com.flab.stocktradingengine.trading.entity.OrderSide.SELL, 20_000L, 600, 300, "pre",
            new U10Tool.BulkTargets(spec.stockCodes(), 0L, 0L, 0L, 1), bulk::add);
        for (com.flab.stocktradingengine.codec.DecodedAccountOrder order : bulk) {
            prefilled.merge(order.accountId() + "/" + order.stockCode(), 1, Integer::sum);
        }
        assertEquals(300 * 3, prefilled.size());
        assertTrue(prefilled.values().stream().allMatch(count -> count == 2), "쌓은 수 600을 계좌 300개로 나눈 몫 2");
    }

    @Test
    void 계좌_분포는_채널별_개수를_세고_합이_전체와_같다() {
        long[] counts = U10Tool.shardCounts(2, 90_002L, 300);
        assertEquals(2, counts.length);
        assertEquals(300, counts[0] + counts[1]);
    }
}
