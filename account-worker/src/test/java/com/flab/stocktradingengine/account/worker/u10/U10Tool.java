package com.flab.stocktradingengine.account.worker.u10;

import java.io.File;
import java.io.PrintStream;
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
import org.apache.kafka.clients.consumer.Consumer;
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
import com.flab.stocktradingengine.aeron.MatchingSlotTarget;
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
 *   <li>{@code codes <stockSlotCount> <splitSlot>} — 종목 slot이 splitSlot 이상인 종목코드 X와 미만인 종목코드 Y를 출력한다
 *       (Kafka 없이 계산만 한다).</li>
 *   <li>{@code codes-many <stockSlotCount> <splitSlot> <count>} — 종목 slot이 splitSlot 이상인 종목코드 count개를
 *       {@code HIGH=a,b,c}로 출력한다.</li>
 *   <li>{@code send-bulk <channel> <accountId> <BUY|SELL> <firstPrice> <perStock> <requestIdPrefix> <stockCode...>} —
 *       종목마다 perStock건을 가격 firstPrice, firstPrice+1, …로 한 연결에서 보낸다(수량 1). 환경변수 {@code BULK_ACCOUNTS=<계좌 수>}(기본 1)를 주면
 *       종목의 i번째 주문(0부터)을 계좌 {@code accountId + (i % 계좌 수)}로 나눠 보낸다(표지 매수는 표지 계좌 그대로).
 *       이 도구는 그 계좌들이 계좌 워커에 시드되어 있는지 확인하지 않는다: 시드되지 않은 계좌의 주문은 계좌 워커가 ACCOUNT_NOT_FOUND로 거부한다.
 *       {@code loadtest/u10/u10-account.yml}은 계좌 90001과 90002만 시드하므로, 계좌 수를 2 이상으로 쓰려면 호출하는 쪽이 시드를 맞춘다
 *       (c1의 {@code loadtest/s0/probe.sh}는 90002부터 계좌 수만큼 직접 시드한다).</li>
 *   <li>{@code assigned <kafkaBootstrap> <stockSlotCount>} — 맵 토픽의 종목 slot별 마지막 값을 읽어
 *       {@code ASSIGNED 배정된 수/전체 MOVING 이동 중인 수 ENDPOINTS 주소=개수,...}를 출력한다.</li>
 *   <li>{@code map-history <kafkaBootstrap>} — 맵 토픽의 모든 기록을 {@code slot<TAB>CreateTime(ms)<TAB>값} 줄로 출력한다.</li>
 *   <li>{@code dump-account <archiveDir>} — 계좌 워커가 정상 종료할 때 쓴 스냅샷을 줄 단위로 출력한다. 스냅샷이 없으면 종료 코드 2.</li>
 *   <li>{@code stream <channel> <기간 ms> <X 초당> <Y 초당> <X 종목> <Y 종목> <매도 계좌> <매수 계좌> <가격>} — 이동 중 주문 시험용 연속 발신. 주문마다 「ORDER 발신시각 요청번호 종류」, 끝에 「STREAM_DONE」 합계를 출력한다.</li>
 *   <li>{@code slot-of <종목 slot 수> <종목 코드...>} — 종목 코드마다 「SLOT 코드=slot」 한 줄을 출력한다.</li>
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
            case "codes" -> codes(Integer.parseInt(args[1]), Integer.parseInt(args[2]));
            case "codes-many" -> codesMany(Integer.parseInt(args[1]), Integer.parseInt(args[2]), Integer.parseInt(args[3]));
            case "send-bulk" -> sendBulk(args);
            case "stream" -> stream(args);
            case "assigned" -> assigned(args[1], Integer.parseInt(args[2]));
            case "map-history" -> mapHistory(args[1]);
            case "slot-of" -> slotOf(Integer.parseInt(args[1]), java.util.Arrays.copyOfRange(args, 2, args.length));
            default -> {
                System.err.println("알 수 없는 하위 명령: " + args[0]);
                System.exit(1);
            }
        }
    }

    private static void pick(String bootstrap, int stockSlotCount) {
        Map<Integer, MatchingSlotTarget> targets = new HashMap<>();
        try (KafkaConsumer<Integer, String> consumer = new KafkaConsumer<>(mapConsumerProperties(bootstrap))) {
            for (MapRecord record : readMapTopic(consumer, MAP_READ_TIMEOUT_MILLIS, System.err)) {
                if (record.value() == null) {
                    targets.remove(record.slot());
                } else {
                    targets.put(record.slot(), MatchingSlotTarget.parse(record.value()));
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


    static final long MAP_READ_TIMEOUT_MILLIS = 10_000L;

    record MapRecord(int slot, long timestamp, String timestampType, String value) {
    }

    private static Properties mapConsumerProperties(String bootstrap) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        return props;
    }

    private static List<MapRecord> readMapRecords(String bootstrap) {
        try (KafkaConsumer<Integer, String> consumer = new KafkaConsumer<>(mapConsumerProperties(bootstrap))) {
            return readMapTopic(consumer, MAP_READ_TIMEOUT_MILLIS, System.err);
        }
    }

    /**
     * 맵 토픽을 처음부터 읽어 기록을 순서대로 돌려준다. 제한 시간 안에 읽기 시작 시점의 끝까지 닿지 못하면 부분 결과를 돌려주되
     * {@code err}에 WARN_PARTIAL_MAP_READ 줄을 남긴다(부분 결과가 이동이 안 된 것처럼 보이지 않게 스크립트가 이 줄을 찾는다).
     */
    static List<MapRecord> readMapTopic(Consumer<Integer, String> consumer, long timeoutMillis, PrintStream err) {
        List<MapRecord> records = new ArrayList<>();
        List<PartitionInfo> infos = consumer.partitionsFor(MatchingDestinationResolver.MAP_TOPIC, Duration.ofSeconds(10));
        if (infos == null || infos.isEmpty()) {
            err.println("MAP_TOPIC_MISSING " + MatchingDestinationResolver.MAP_TOPIC + " 토픽이 아직 없거나 파티션 정보를 받지 못했다(부분 읽기가 아님)");
            return records;
        }
        List<TopicPartition> partitions = new ArrayList<>();
        for (PartitionInfo info : infos) {
            partitions.add(new TopicPartition(MatchingDestinationResolver.MAP_TOPIC, info.partition()));
        }
        consumer.assign(partitions);
        consumer.seekToBeginning(partitions);
        Map<TopicPartition, Long> ends = consumer.endOffsets(partitions);
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline && !reachedEnds(consumer, partitions, ends)) {
            for (ConsumerRecord<Integer, String> record : consumer.poll(Duration.ofMillis(200))) {
                records.add(new MapRecord(record.key(), record.timestamp(), record.timestampType().name, record.value()));
            }
        }
        if (!reachedEnds(consumer, partitions, ends)) {
            err.println("WARN_PARTIAL_MAP_READ " + timeoutMillis + "ms 안에 " + MatchingDestinationResolver.MAP_TOPIC
                + "의 끝까지 읽지 못했다: 읽은 기록 " + records.size() + "건");
        }
        return records;
    }

    static List<String> slotLines(int stockSlotCount, String... stockCodes) {
        StockSlotHasher hasher = new StockSlotHasher(stockSlotCount);
        List<String> lines = new ArrayList<>();
        for (String stockCode : stockCodes) {
            lines.add("SLOT " + stockCode + "=" + hasher.slotFor(stockCode));
        }
        return lines;
    }

    private static void codesMany(int stockSlotCount, int splitSlot, int count) {
        StockSlotHasher hasher = new StockSlotHasher(stockSlotCount);
        List<String> picked = new ArrayList<>();
        for (int number = 1; number <= 999_999 && picked.size() < count; number++) {
            String code = String.format("%06d", number);
            if (hasher.slotFor(code) >= splitSlot) {
                picked.add(code);
            }
        }
        System.out.println("HIGH=" + String.join(",", picked));
    }

    /** send-bulk의 종목 코드 목록과 선택 옵션 {@code --sentinel-buy=<계좌>:<가격>}(종목마다 매도 N건 뒤에 같은 Publication으로 보낼 표지 매수)와 {@code --price-step=<n>}(매도 가격을 건마다 올리는 폭, 기본 1). */
    record BulkTargets(List<String> stockCodes, long sentinelAccountId, long sentinelPrice, long priceStep) {
    }

    static BulkTargets parseBulkTargets(String[] args, int firstStockIndex) {
        List<String> stockCodes = new ArrayList<>();
        long sentinelAccountId = 0L;
        long sentinelPrice = 0L;
        long priceStep = 1L;
        for (int index = firstStockIndex; index < args.length; index++) {
            if (args[index].startsWith("--sentinel-buy=")) {
                String[] parts = args[index].substring("--sentinel-buy=".length()).split(":");
                sentinelAccountId = Long.parseLong(parts[0]);
                sentinelPrice = Long.parseLong(parts[1]);
            } else if (args[index].startsWith("--price-step=")) {
                priceStep = Long.parseLong(args[index].substring("--price-step=".length()));
            } else {
                stockCodes.add(args[index]);
            }
        }
        return new BulkTargets(stockCodes, sentinelAccountId, sentinelPrice, priceStep);
    }

    /** 주문 한 건을 어디로 보낼지의 추상. 실제 발신은 Aeron Publication, 시험은 기록하는 목록으로 바꾼다. */
    @FunctionalInterface
    interface OrderSink {
        void accept(DecodedAccountOrder order);
    }

    /** 종목마다 매도 perStock건을 보내고 그 바로 뒤에 표지 매수(있으면)를 보낸다. 모든 주문을 기준 계좌 하나로 보낸다. 보낸 주문 수를 돌려준다. */
    static int bulkOrders(long accountId, OrderSide side, long firstPrice, int perStock, String prefix, BulkTargets targets, OrderSink sink) {
        return bulkOrders(accountId, side, firstPrice, perStock, 1, prefix, targets, sink);
    }

    /** 환경변수 U10_SENDER_WAIT_MS 값을 읽는다. 없거나 비면 30초(30_000)이고, 양의 정수가 아니면 거부한다. 느린 계좌 워커로 보내는 긴 실행은 더 길게 준다. */
    static long parseSenderWaitMillis(String value) {
        if (value == null || value.isEmpty()) {
            return 30_000L;
        }
        long millis;
        try {
            millis = Long.parseLong(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("U10_SENDER_WAIT_MS는 양의 정수여야 한다: " + value, e);
        }
        if (millis < 1) {
            throw new IllegalArgumentException("U10_SENDER_WAIT_MS는 양의 정수여야 한다: " + value);
        }
        return millis;
    }

    /** 환경변수 BULK_ACCOUNTS 값을 읽는다. 없거나 비면 1(계좌 하나)이고, 양의 정수가 아니면 거부한다. */
    static int parseBulkAccounts(String value) {
        if (value == null || value.isEmpty()) {
            return 1;
        }
        int accounts;
        try {
            accounts = Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("BULK_ACCOUNTS는 양의 정수여야 한다: " + value, e);
        }
        if (accounts < 1) {
            throw new IllegalArgumentException("BULK_ACCOUNTS는 양의 정수여야 한다: " + value);
        }
        return accounts;
    }

    /**
     * 종목마다 매도 perStock건을 보내고 그 바로 뒤에 표지 매수(있으면)를 보낸다. 종목의 i번째 주문(0부터)은 계좌 {@code accountId + (i % accounts)}로 보낸다.
     * 표지 매수는 계좌 수와 무관하게 표지 계좌로 가고, 가격은 계좌 수와 무관하게 첫 가격 + i * 가격 간격이다. 보낸 주문 수(표지 제외)를 돌려준다.
     */
    static int bulkOrders(long accountId, OrderSide side, long firstPrice, int perStock, int accounts, String prefix, BulkTargets targets,
                          OrderSink sink) {
        int sent = 0;
        for (String stockCode : targets.stockCodes()) {
            for (int i = 0; i < perStock; i++) {
                sink.accept(new DecodedAccountOrder(side, accountId + (i % accounts), stockCode,
                    BigDecimal.valueOf(firstPrice + i * targets.priceStep()), 1,
                    prefix + "-" + stockCode + "-" + i, EpochNanos.now()));
                sent++;
            }
            if (targets.sentinelAccountId() != 0L) {
                sink.accept(new DecodedAccountOrder(OrderSide.BUY, targets.sentinelAccountId(), stockCode,
                    BigDecimal.valueOf(targets.sentinelPrice()), 1, prefix + "-sentinel-" + stockCode, EpochNanos.now()));
            }
        }
        return sent;
    }

    /**
     * 발행 위치({@code targetPosition})까지 임베디드 media driver가 UDP로 내보냈는지(송신 위치 카운터) 기다린다. Aeron의 offer는 발행 버퍼에 넣을 뿐이라
     * 바로 닫으면 아직 내보내지 못한 주문이 사라진다. 결과: REACHED, 카운터는 찾았으나 제한 시간 안에 닿지 못함(NOT_REACHED), 이 발행의 송신 위치
     * 카운터를 찾지 못함(COUNTER_NOT_FOUND — 카운터 형식이나 타입 번호가 어긋났을 수 있어 NOT_REACHED와 구별한다).
     */
    enum SenderWait { REACHED, NOT_REACHED, COUNTER_NOT_FOUND }

    static SenderWait awaitSenderPosition(org.agrona.concurrent.status.CountersReader counters, long registrationId, long targetPosition,
                                          long timeoutMillis) {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        int counterId = -1;
        while (System.currentTimeMillis() <= deadline) {
            if (counterId < 0) {
                counterId = findSenderPositionCounter(counters, registrationId);
            }
            if (counterId >= 0 && counters.getCounterValue(counterId) >= targetPosition) {
                return SenderWait.REACHED;
            }
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return counterId < 0 ? SenderWait.COUNTER_NOT_FOUND : SenderWait.NOT_REACHED;
            }
        }
        return counterId < 0 ? SenderWait.COUNTER_NOT_FOUND : SenderWait.NOT_REACHED;
    }

    private static int findSenderPositionCounter(org.agrona.concurrent.status.CountersReader counters, long registrationId) {
        int[] found = {-1};
        counters.forEach((counterId, typeId, keyBuffer, label) -> {
            if (typeId == io.aeron.AeronCounters.DRIVER_SENDER_POSITION_TYPE_ID && keyBuffer.getLong(0) == registrationId) {
                found[0] = counterId;
            }
        });
        return found[0];
    }

    /** 마지막 offer 뒤 닫기 전에 부른다. 내보내지 못했으면 SENDER_POSITION_NOT_REACHED(종료 코드 3), 카운터를 못 찾으면 SENDER_POSITION_COUNTER_NOT_FOUND(종료 코드 4)로 끝낸다. */
    private static void flushOrFail(Aeron aeron, Publication publication) {
        long target = publication.position();
        long waitMillis = parseSenderWaitMillis(System.getenv("U10_SENDER_WAIT_MS"));
        SenderWait result = awaitSenderPosition(aeron.countersReader(), publication.registrationId(), target, waitMillis);
        switch (result) {
            case REACHED -> System.out.println("SENDER_POSITION_REACHED " + target);
            case NOT_REACHED -> {
                System.out.println("SENDER_POSITION_NOT_REACHED 발행 위치=" + target + " (" + (waitMillis / 1000) + "초 안에 UDP로 다 내보내지 못했다)");
                System.exit(3);
            }
            case COUNTER_NOT_FOUND -> {
                System.out.println("SENDER_POSITION_COUNTER_NOT_FOUND 이 발행(registrationId=" + publication.registrationId()
                    + ")의 송신 위치 카운터를 찾지 못했다 — 카운터 형식이나 타입 번호를 확인해야 한다");
                System.exit(4);
            }
        }
    }

    private static void offerBlocking(Publication publication, AccountOrderCodec codec, UnsafeBuffer buffer, DecodedAccountOrder order,
                                      long timeoutMillis) {
        int length = codec.encode(buffer, 0, order);
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (publication.offer(buffer, 0, length) < 0) {
            if (System.currentTimeMillis() > deadline) {
                System.out.println("OFFER_FAILED " + order.requestId());
                System.exit(3);
            }
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
        }
    }

    /** stream 명령의 계획: 이동하는 종목 X와 이동하지 않는 종목 Y에 같은 가격 수량 1짜리 매도와 매수를 번갈아 일정 속도로 낸다. */
    record StreamSpec(long durationMillis, int xPerSecond, int yPerSecond, String xStock, String yStock,
                      long sellAccount, long buyAccount, long price) {
    }

    record PlannedOrder(long offsetMillis, DecodedAccountOrder order) {
    }

    static final int INVALID_QUANTITY = 2_000_000_000;

    /**
     * 발신 계획. 계좌 잔고를 넘는 매수(잔고 초과 거부용)를 X에 기간 한가운데 한 건 섞는다. 오프셋 순으로 정렬해 돌려준다.
     * 주문의 발행 시각({@code publishedAtEpochNanos}, 계획을 만든 시각이라 실제 발신보다 최대 기간만큼 앞선다)은 계좌 엔진이 순서나 판정에 쓰지 않는다:
     * 사용처는 모두 측정이다 [코드로 읽음: grep]: {@code AccountEventHandler.java:313,374}의 {@code LatencyHistogram.record}, 단계 측정이 켜졌을 때만 매칭행 발신에
     * 함께 싣는 {@code AccountEventHandler.java:334}, 단계 시간을 계산하는 {@code AccountSegmentStages.java:68}.
     */
    static List<PlannedOrder> streamPlan(StreamSpec spec) {
        List<PlannedOrder> plan = new ArrayList<>();
        addStreamOrders(plan, spec, spec.xStock(), spec.xPerSecond(), "x");
        addStreamOrders(plan, spec, spec.yStock(), spec.yPerSecond(), "y");
        plan.add(new PlannedOrder(spec.durationMillis() / 2, new DecodedAccountOrder(OrderSide.BUY, spec.buyAccount(), spec.xStock(),
            BigDecimal.valueOf(spec.price()), INVALID_QUANTITY, "u10-flow-invalid", EpochNanos.now())));
        plan.sort(java.util.Comparator.comparingLong(PlannedOrder::offsetMillis));
        return plan;
    }

    private static void addStreamOrders(List<PlannedOrder> plan, StreamSpec spec, String stockCode, int perSecond, String tag) {
        if (perSecond <= 0) {
            return;
        }
        long count = perSecond * spec.durationMillis() / 1000L;
        for (long i = 0; i < count; i++) {
            boolean sell = i % 2 == 0;
            plan.add(new PlannedOrder(i * 1000L / perSecond, new DecodedAccountOrder(sell ? OrderSide.SELL : OrderSide.BUY,
                sell ? spec.sellAccount() : spec.buyAccount(), stockCode, BigDecimal.valueOf(spec.price()), 1,
                "u10-flow-" + tag + "-" + i, EpochNanos.now())));
        }
    }

    /** 사용: stream <channel> <기간 ms> <X 초당 건수> <Y 초당 건수> <X 종목> <Y 종목> <매도 계좌> <매수 계좌> <가격>. 주문마다 발신 시각을 출력한다. */
    private static void stream(String[] args) throws Exception {
        String channel = args[1];
        StreamSpec spec = new StreamSpec(Long.parseLong(args[2]), Integer.parseInt(args[3]), Integer.parseInt(args[4]), args[5], args[6],
            Long.parseLong(args[7]), Long.parseLong(args[8]), Long.parseLong(args[9]));
        List<PlannedOrder> plan = streamPlan(spec);
        MediaDriver driver = MediaDriver.launchEmbedded(new MediaDriver.Context()
            .aeronDirectoryName(CommonContext.generateRandomDirName())
            .dirDeleteOnStart(true)
            .threadingMode(ThreadingMode.SHARED));
        Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()));
        try {
            Publication publication = aeron.addPublication(channel, AeronStreamIds.ACCOUNT_INTAKE);
            long connectDeadline = System.currentTimeMillis() + CONNECT_TIMEOUT_MILLIS;
            while (!publication.isConnected() && System.currentTimeMillis() < connectDeadline) {
                Thread.sleep(20);
            }
            if (!publication.isConnected()) {
                System.out.println("NOT_CONNECTED " + channel);
                System.exit(3);
            }
            AccountOrderCodec codec = new AccountOrderCodec();
            UnsafeBuffer buffer = new UnsafeBuffer(new byte[256]);
            long startNanos = System.nanoTime();
            long startEpochMillis = System.currentTimeMillis();
            System.out.println("STREAM_START " + startEpochMillis);
            long xSells = 0;
            long xBuys = 0;
            long ySells = 0;
            long yBuys = 0;
            long invalid = 0;
            long maxLateness = 0;
            long totalLateness = 0;
            for (PlannedOrder planned : plan) {
                long targetNanos = startNanos + planned.offsetMillis() * 1_000_000L;
                while (System.nanoTime() < targetNanos) {
                    if (targetNanos - System.nanoTime() > 2_000_000L) {
                        Thread.sleep(1);
                    } else {
                        Thread.onSpinWait();
                    }
                }
                DecodedAccountOrder order = planned.order();
                offerBlocking(publication, codec, buffer, order, 30_000L);
                long sentEpochMillis = System.currentTimeMillis();
                long lateness = sentEpochMillis - (startEpochMillis + planned.offsetMillis());
                maxLateness = Math.max(maxLateness, lateness);
                totalLateness += lateness;
                boolean isX = order.stockCode().equals(spec.xStock());
                String kind;
                if (order.quantity() == INVALID_QUANTITY) {
                    invalid++;
                    kind = "INVALID";
                } else if (order.type() == OrderSide.SELL) {
                    kind = isX ? "X_SELL" : "Y_SELL";
                    if (isX) { xSells++; } else { ySells++; }
                } else {
                    kind = isX ? "X_BUY" : "Y_BUY";
                    if (isX) { xBuys++; } else { yBuys++; }
                }
                System.out.println("ORDER " + sentEpochMillis + " " + order.requestId() + " " + kind);
            }
            System.out.println("STREAM_DONE xSells=" + xSells + " xBuys=" + xBuys + " ySells=" + ySells + " yBuys=" + yBuys
                + " invalid=" + invalid + " maxLatenessMs=" + maxLateness + " avgLatenessMs=" + (plan.isEmpty() ? 0 : totalLateness / plan.size()));
            flushOrFail(aeron, publication);
            Thread.sleep(500);
        } finally {
            CloseHelper.quietClose(aeron);
            CloseHelper.quietClose(driver);
        }
    }

    private static void sendBulk(String[] args) throws Exception {
        String channel = args[1];
        long accountId = Long.parseLong(args[2]);
        OrderSide side = OrderSide.valueOf(args[3]);
        long firstPrice = Long.parseLong(args[4]);
        int perStock = Integer.parseInt(args[5]);
        String prefix = args[6];
        MediaDriver driver = MediaDriver.launchEmbedded(new MediaDriver.Context()
            .aeronDirectoryName(CommonContext.generateRandomDirName())
            .dirDeleteOnStart(true)
            .threadingMode(ThreadingMode.SHARED));
        Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()));
        try {
            Publication publication = aeron.addPublication(channel, AeronStreamIds.ACCOUNT_INTAKE);
            long connectDeadline = System.currentTimeMillis() + CONNECT_TIMEOUT_MILLIS;
            while (!publication.isConnected() && System.currentTimeMillis() < connectDeadline) {
                Thread.sleep(20);
            }
            if (!publication.isConnected()) {
                System.out.println("NOT_CONNECTED " + channel);
                System.exit(3);
            }
            AccountOrderCodec codec = new AccountOrderCodec();
            UnsafeBuffer buffer = new UnsafeBuffer(new byte[256]);
            long started = System.currentTimeMillis();
            BulkTargets targets = parseBulkTargets(args, 7);
            int bulkAccounts = parseBulkAccounts(System.getenv("BULK_ACCOUNTS"));
            int sent = bulkOrders(accountId, side, firstPrice, perStock, bulkAccounts, prefix, targets,
                order -> offerBlocking(publication, codec, buffer, order, 30_000L));
            for (String stockCode : targets.stockCodes()) {
                if (targets.sentinelAccountId() != 0L) {
                    System.out.println("SENTINEL " + stockCode);
                }
            }
            System.out.println("SENT_BULK " + sent + " elapsedMs=" + (System.currentTimeMillis() - started));
            flushOrFail(aeron, publication);
            Thread.sleep(500);
        } finally {
            CloseHelper.quietClose(aeron);
            CloseHelper.quietClose(driver);
        }
    }

    private static void codes(int stockSlotCount, int splitSlot) {
        StockSlotHasher hasher = new StockSlotHasher(stockSlotCount);
        String high = null;
        String low = null;
        for (int number = 1; number <= 999_999 && (high == null || low == null); number++) {
            String code = String.format("%06d", number);
            int slot = hasher.slotFor(code);
            if (slot >= splitSlot && high == null) {
                high = code;
            } else if (slot < splitSlot && low == null) {
                low = code;
            }
        }
        System.out.println("X=" + high + " XSLOT=" + hasher.slotFor(high) + " Y=" + low + " YSLOT=" + hasher.slotFor(low));
    }

    private static void assigned(String bootstrap, int stockSlotCount) {
        Map<Integer, MatchingSlotTarget> latest = new HashMap<>();
        for (MapRecord record : readMapRecords(bootstrap)) {
            if (record.value() == null) {
                latest.remove(record.slot());
            } else {
                latest.put(record.slot(), MatchingSlotTarget.parse(record.value()));
            }
        }
        long moving = latest.values().stream().filter(MatchingSlotTarget::moving).count();
        Map<String, Integer> perEndpoint = new TreeMap<>();
        for (MatchingSlotTarget target : latest.values()) {
            if (!target.moving()) {
                perEndpoint.merge(target.endpoint(), 1, Integer::sum);
            }
        }
        long steady = perEndpoint.values().stream().mapToInt(Integer::intValue).sum();
        System.out.println("ASSIGNED " + steady + "/" + stockSlotCount + " MOVING " + moving + " ENDPOINTS " + perEndpoint);
    }

    private static void slotOf(int stockSlotCount, String[] stockCodes) {
        slotLines(stockSlotCount, stockCodes).forEach(System.out::println);
    }

    private static void mapHistory(String bootstrap) {
        for (MapRecord record : readMapRecords(bootstrap)) {
            System.out.println(record.slot() + "\t" + record.timestamp() + "\t" + record.value() + "\t" + record.timestampType());
        }
    }

    private static boolean reachedEnds(Consumer<Integer, String> consumer, List<TopicPartition> partitions,
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
            if (result < 0) {
                System.out.println("OFFER_FAILED " + result);
                System.exit(3);
            }
            flushOrFail(aeron, publication); // 전송이 끝나기 전에 클라이언트가 닫혀 버리지 않게 송신 위치가 발행 위치에 닿을 때까지 기다린다
            System.out.println("SENT " + args[7]);
            Thread.sleep(300);
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
