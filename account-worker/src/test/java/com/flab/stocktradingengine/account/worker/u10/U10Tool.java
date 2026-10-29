package com.flab.stocktradingengine.account.worker.u10;

import java.io.File;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.TreeMap;

import org.agrona.CloseHelper;
import org.agrona.concurrent.UnsafeBuffer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import com.flab.stocktradingengine.account.disruptor.snapshot.AccountStateSnapshot;
import com.flab.stocktradingengine.account.disruptor.snapshot.TradeIdGenerationSnapshot;
import com.flab.stocktradingengine.account.worker.coordination.MatchingDestinationResolver;
import com.flab.stocktradingengine.account.worker.coordination.MatchingSlotTarget;
import com.flab.stocktradingengine.account.worker.recovery.AccountSnapshotStore;
import com.flab.stocktradingengine.account.worker.recovery.StoredAccountSnapshot;
import com.flab.stocktradingengine.aeron.AeronStreamIds;
import com.flab.stocktradingengine.aeron.StockSlotHasher;
import com.flab.stocktradingengine.codec.AccountOrderCodec;
import com.flab.stocktradingengine.codec.DecodedAccountOrder;
import com.flab.stocktradingengine.time.EpochNanos;
import com.flab.stocktradingengine.trading.entity.OrderSide;

import io.aeron.Aeron;
import io.aeron.CommonContext;
import io.aeron.Publication;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;

/**
 * U10 확인 스크립트용 도구. 하위 명령 셋:
 * <ul>
 *   <li>{@code pick <kafkaBootstrap> <stockSlotCount>} — 맵 토픽을 읽어 64개 종목 slot이 모두 배정됐는지 보고,
 *       서로 다른 매칭 pod가 맡는 종목코드 둘을 {@code X=.. XEND=.. Y=.. YEND=..}로 출력한다. 배정이 덜 됐으면 종료 코드 2.</li>
 *   <li>{@code send <accountIntakeChannel> <accountId> <stockCode> <BUY|SELL> <price> <quantity> <requestId>} —
 *       계좌 워커 주문 입구(스트림 4004)에 주문 한 건을 보낸다. 연결이 안 되면 종료 코드 3.</li>
 *   <li>{@code dump-account <archiveDir>} — 계좌 워커가 정상 종료할 때 쓴 스냅샷을 줄 단위로 출력한다. 스냅샷이 없으면 종료 코드 2.</li>
 * </ul>
 */
public final class U10Tool {

    private static final long CONNECT_TIMEOUT_MILLIS = 15_000L;

    private U10Tool() {
    }

    public static void main(String[] args) throws Exception {
        switch (args[0]) {
            case "pick" -> pick(args[1], Integer.parseInt(args[2]));
            case "send" -> send(args);
            case "dump-account" -> dumpAccount(new File(args[1]));
            default -> {
                System.err.println("알 수 없는 하위 명령: " + args[0]);
                System.exit(1);
            }
        }
    }

    private static void pick(String bootstrap, int stockSlotCount) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        Map<Integer, MatchingSlotTarget> targets = new HashMap<>();
        try (KafkaConsumer<Integer, String> consumer = new KafkaConsumer<>(props)) {
            List<PartitionInfo> infos = consumer.partitionsFor(MatchingDestinationResolver.MAP_TOPIC, Duration.ofSeconds(10));
            List<TopicPartition> partitions = new ArrayList<>();
            for (PartitionInfo info : infos) {
                partitions.add(new TopicPartition(MatchingDestinationResolver.MAP_TOPIC, info.partition()));
            }
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            Map<TopicPartition, Long> ends = consumer.endOffsets(partitions);
            long deadline = System.currentTimeMillis() + 10_000L;
            while (System.currentTimeMillis() < deadline && !reachedEnds(consumer, partitions, ends)) {
                for (ConsumerRecord<Integer, String> record : consumer.poll(Duration.ofMillis(200))) {
                    if (record.value() == null) {
                        targets.remove(record.key());
                    } else {
                        targets.put(record.key(), MatchingSlotTarget.parse(record.value()));
                    }
                }
            }
        }
        long assigned = targets.values().stream().filter(target -> !target.moving()).count();
        System.out.println("ASSIGNED " + assigned + "/" + stockSlotCount);
        if (assigned != stockSlotCount) {
            System.exit(2);
        }
        StockSlotHasher hasher = new StockSlotHasher(stockSlotCount);
        String firstCode = null;
        String firstEndpoint = null;
        for (int number = 1; number <= 999_999; number++) {
            String code = String.format("%06d", number);
            String endpoint = targets.get(hasher.slotFor(code)).endpoint();
            if (firstCode == null) {
                firstCode = code;
                firstEndpoint = endpoint;
            } else if (!endpoint.equals(firstEndpoint)) {
                System.out.println("X=" + firstCode + " XEND=" + firstEndpoint + " Y=" + code + " YEND=" + endpoint);
                return;
            }
        }
        System.out.println("ONE_ENDPOINT_ONLY");
        System.exit(2);
    }

    private static boolean reachedEnds(KafkaConsumer<Integer, String> consumer, List<TopicPartition> partitions,
                                       Map<TopicPartition, Long> ends) {
        for (TopicPartition partition : partitions) {
            if (consumer.position(partition) < ends.get(partition)) {
                return false;
            }
        }
        return true;
    }

    private static void send(String[] args) throws Exception {
        String channel = args[1];
        DecodedAccountOrder order = new DecodedAccountOrder(
            OrderSide.valueOf(args[4]), Long.parseLong(args[2]), args[3],
            new BigDecimal(args[5]), Integer.parseInt(args[6]), args[7], EpochNanos.now());
        MediaDriver driver = MediaDriver.launchEmbedded(new MediaDriver.Context()
            .aeronDirectoryName(CommonContext.generateRandomDirName())
            .dirDeleteOnStart(true)
            .threadingMode(ThreadingMode.SHARED));
        Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()));
        try {
            Publication publication = aeron.addPublication(channel, AeronStreamIds.ACCOUNT_INTAKE);
            long deadline = System.currentTimeMillis() + CONNECT_TIMEOUT_MILLIS;
            while (!publication.isConnected() && System.currentTimeMillis() < deadline) {
                Thread.sleep(20);
            }
            if (!publication.isConnected()) {
                System.out.println("NOT_CONNECTED " + channel);
                System.exit(3);
            }
            UnsafeBuffer buffer = new UnsafeBuffer(new byte[256]);
            int length = new AccountOrderCodec().encode(buffer, 0, order);
            long result;
            while ((result = publication.offer(buffer, 0, length)) < 0 && System.currentTimeMillis() < deadline) {
                Thread.sleep(5);
            }
            System.out.println(result < 0 ? "OFFER_FAILED " + result : "SENT " + args[7]);
            if (result < 0) {
                System.exit(3);
            }
            Thread.sleep(300); // 전송이 끝나기 전에 클라이언트가 닫혀 버리지 않게 잠깐 둔다
        } finally {
            CloseHelper.quietClose(aeron);
            CloseHelper.quietClose(driver);
        }
    }

    private static void dumpAccount(File archiveDir) {
        Optional<StoredAccountSnapshot> stored = new AccountSnapshotStore(archiveDir).read();
        if (stored.isEmpty()) {
            System.out.println("NO_SNAPSHOT");
            System.exit(2);
        }
        Map<Long, AccountStateSnapshot> accounts = new TreeMap<>(stored.get().snapshot().accountsById());
        for (AccountStateSnapshot account : accounts.values()) {
            StringBuilder tradeIds = new StringBuilder();
            for (TradeIdGenerationSnapshot generation : account.tradeIdGenerations()) {
                for (Long tradeId : generation.tradeIds()) {
                    if (tradeIds.length() > 0) {
                        tradeIds.append(',');
                    }
                    tradeIds.append(tradeId);
                }
            }
            System.out.println("ACCOUNT id=" + account.accountId() + " balance=" + account.balance().toPlainString()
                + " holdings=" + new TreeMap<>(account.holdings()).toString().replace(" ", "") + " unpaid=" + account.unpaid().toPlainString()
                + " reservations=" + account.reservations().size() + " sellReservations=" + account.sellReservations().size()
                + " tradeIds=" + tradeIds);
        }
    }
}
