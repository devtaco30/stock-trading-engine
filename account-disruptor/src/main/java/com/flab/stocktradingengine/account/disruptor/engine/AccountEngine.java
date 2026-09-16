package com.flab.stocktradingengine.account.disruptor.engine;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.TimeoutException;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.lmax.disruptor.util.DaemonThreadFactory;
import com.flab.stocktradingengine.account.disruptor.domain.AccountResultListener;
import com.flab.stocktradingengine.account.disruptor.domain.AccountState;
import com.flab.stocktradingengine.account.disruptor.domain.NoOpAccountResultListener;
import com.flab.stocktradingengine.account.disruptor.handler.AccountEventHandler;
import com.flab.stocktradingengine.account.disruptor.handler.AccountExceptionHandler;
import com.flab.stocktradingengine.account.disruptor.handler.AccountJournalEventHandler;
import com.flab.stocktradingengine.account.disruptor.io.AccountSnapshotSink;
import com.flab.stocktradingengine.account.disruptor.io.MatchingOrderSender;
import com.flab.stocktradingengine.account.disruptor.journal.AccountJournal;
import com.flab.stocktradingengine.account.disruptor.journal.InMemoryAccountJournal;
import com.flab.stocktradingengine.account.disruptor.journal.JournalUnavailableException;
import com.flab.stocktradingengine.account.disruptor.snapshot.AccountSnapshot;
import com.flab.stocktradingengine.account.disruptor.snapshot.AccountStateSnapshot;
import com.flab.stocktradingengine.codec.AccountJournalEntry;
import com.flab.stocktradingengine.time.EpochNanos;
import com.flab.stocktradingengine.time.LatencyHistogram;

/**
 * 계좌 축 워커의 진입점.
 *
 * <p>accountId 로 소유한 계좌들을 단일 소비자 스레드가 만진다(single writer). 매칭 코어처럼
 * 링버퍼 하나 + 저널(2b-1)·비즈니스 두 단계 소비자이며, 계좌 검증·예약·체결·정산 반영을
 * 여기서 직렬 처리한다. 매칭과 다른 축(계좌)이라 별도 링버퍼다.</p>
 *
 * <h3>시드 후 기동</h3>
 * <p>계좌 상태는 DB 없이 {@link #seed} 로 미리 넣는다. 시드는 소비자 스레드가 뜨기 전
 * ({@link #start} 이전) 설정 스레드에서만 호출해야 한다 — 기동 후 시드하면 소비자와 경쟁한다.</p>
 *
 * <h3>ProducerType</h3>
 * <p>피더(발행자)가 하나뿐이면(테스트, Aeron 수신 스레드 하나) {@link ProducerType#SINGLE}로 충분하다.
 * 발행자가 둘 이상이면(예: order-manager — 주문 접수 스레드와 매칭의 체결 콜백 스레드가 각각
 * publishBuy/Sell 과 publishBuyFill/SellFill 로 같은 링버퍼에 쓴다) {@link ProducerType#MULTI}가 필요하다.
 * 소비자(계좌 상태를 실제로 만지는 쪽)는 이 값과 무관하게 항상 {@link AccountEventHandler} 하나뿐이다 —
 * ProducerType 은 "누가 넣는지"에만 영향을 주고 "누가 처리하는지"는 안 바꾼다.</p>
 */
public class AccountEngine {

    private static final MatchingOrderSender NO_OP_MATCHING_ORDER_SENDER =
        (orderId, accountId, stockCode, side, price, quantity) -> {};

    // 러닝 중 스냅샷 파이프라인(1-3)을 안 쓰는 생성자(대부분의 테스트)를 위한 기본값 — 큐잉을
    // 항상 성공시키되(디스크에 실제로 안 쓰이니) durableSeq는 항상 0(아무것도 durable 아님)으로
    // 둬, 실수로 가지치기가 진행돼 테스트가 세대 데이터를 잃는 일이 없게 한다(보수적 기본값).
    private static final AccountSnapshotSink NO_OP_SNAPSHOT_SINK = new AccountSnapshotSink() {
        @Override
        public boolean offer(byte[] snapshotBytes, Map<Integer, Long> fillPositions, long appliedSeq) {
            return true;
        }

        @Override
        public long durableSeq() {
            return 0L;
        }
    };

    // 접수 지연 측정(끝점①)을 안 쓰는 생성자(대부분의 테스트)를 위한 기본값 — 꺼진 채로 두면
    // record가 분기 하나만 타고 즉시 반환한다(LatencyHistogram 참고).
    private static final LatencyHistogram NO_OP_LATENCY_HISTOGRAM = new LatencyHistogram(false);

    // blockUntilJournaled 최대 대기 시간. 저널 스레드가 죽어(fail-fast) 시퀀스가 영영 안 올라오는
    // 상황에서 호출 스레드가 무한 스핀하는 걸 막는다. Kafka max.poll.interval(기본 5분)보다 한참 짧아
    // 리밸런스를 유발하지 않는다.
    private static final long JOURNAL_WAIT_TIMEOUT_MILLIS = 5000;

    // 소비자 스레드가 fail-fast로 죽으면 disruptor.shutdown()이 hasBacklog() 스핀에서 무한 대기할 수
    // 있다 — 죽은 소비자 뒤에 물린 핸들러가 그 시퀀스를 영원히 기다리며 블록되기 때문. 이 타임아웃
    // 안에 못 끝나면 halt()로 강제 정지한다.
    private static final long SHUTDOWN_TIMEOUT_SECONDS = 5;

    private final Disruptor<AccountEvent> disruptor;
    private final Map<Long, AccountState> accounts = new HashMap<>();
    private final AccountJournal journal;
    private final AccountJournalEventHandler journalHandler;
    private final AccountEventHandler businessHandler;
    private final AccountOrderIdGenerator orderIdGenerator;
    private RingBuffer<AccountEvent> ringBuffer;

    /**
     * @param nodeId orderId 발급기(2b-0, {@link AccountOrderIdGenerator})의 노드 구분자. 카운터는
     *               엔진 안(발급기 자신)에 있다 — 벽시계가 아니라 (nodeId, 카운터)만으로 orderId가
     *               정해져, 같은 nodeId로 같은 입력을 리플레이하면 항상 같은 orderId가 나온다.
     * @param matchingOrderSender accept한 매수·매도를 매칭으로 넘기는 발신 포트(②-b). 실제 배선은
     *                             Aeron 발신(예: {@code AeronMatchingOrderSender}), 매칭 연동이 필요 없는
     *                             테스트는 no-op을 넣는다.
     * @param journal 계좌 상태를 만드는 모든 입력을 처리 순서대로 기록하는 저널(2b-1) — matching과
     *                같은 결로 {@code handleEventsWith(journal).then(business)}로 배선해, 기록이
     *                끝난 이벤트만 비즈니스 핸들러가 본다.
     * @param snapshotSink 러닝 중 스냅샷을 디스크에 내보내는 통로(1-3). 안 쓰면(테스트 등)
     *                     {@link #NO_OP_SNAPSHOT_SINK}를 쓰는 오버로드를 대신 호출한다.
     * @param latencyHistogram 끝점①(접수·예약) 지연 측정기(decision_records/v1-v2-e2e-measurement.md).
     *                         안 쓰면(테스트 등) {@link #NO_OP_LATENCY_HISTOGRAM}을 쓰는 오버로드를 대신 호출한다.
     */
    public AccountEngine(int bufferSize, WaitStrategy waitStrategy, ProducerType producerType, long nodeId,
                         MatchingOrderSender matchingOrderSender, AccountResultListener listener, AccountJournal journal,
                         AccountSnapshotSink snapshotSink, LatencyHistogram latencyHistogram) {
        this.journal = journal;
        this.orderIdGenerator = new AccountOrderIdGenerator(nodeId);
        ThreadFactory threadFactory = DaemonThreadFactory.INSTANCE;
        this.disruptor = new Disruptor<>(
            AccountEvent::new,
            bufferSize,
            threadFactory,
            producerType,
            waitStrategy
        );
        // 예상 못한 예외는 fail-fast(handleEventsWith 배선 전에 설정해야 적용됨).
        this.disruptor.setDefaultExceptionHandler(new AccountExceptionHandler());
        // 저널러가 먼저 기록 → 비즈니스 핸들러가 그 뒤에 반영(SequenceBarrier로 게이팅, matching과 같은 결).
        // orderIdGenerator를 필드로 공유하는 이유는 recover() 참고 — 복구가 진행시킨 카운터를
        // 라이브가 그대로 이어받아야 발급 충돌이 없다.
        // blockUntilJournaled가 이 핸들러의 시퀀스를 읽어야 하므로 필드로 잡아둔다.
        this.journalHandler = new AccountJournalEventHandler(journal);
        this.businessHandler = new AccountEventHandler(accounts, orderIdGenerator, matchingOrderSender, listener, snapshotSink, true, latencyHistogram);
        this.disruptor.handleEventsWith(journalHandler).then(businessHandler);
    }

    /** 접수 지연 측정 없이(테스트 등) 생성한다. */
    public AccountEngine(int bufferSize, WaitStrategy waitStrategy, ProducerType producerType, long nodeId,
                         MatchingOrderSender matchingOrderSender, AccountResultListener listener, AccountJournal journal,
                         AccountSnapshotSink snapshotSink) {
        this(bufferSize, waitStrategy, producerType, nodeId, matchingOrderSender, listener, journal, snapshotSink, NO_OP_LATENCY_HISTOGRAM);
    }

    /** 러닝 중 스냅샷 파이프라인 없이(테스트 등) 생성한다. */
    public AccountEngine(int bufferSize, WaitStrategy waitStrategy, ProducerType producerType, long nodeId,
                         MatchingOrderSender matchingOrderSender, AccountResultListener listener, AccountJournal journal) {
        this(bufferSize, waitStrategy, producerType, nodeId, matchingOrderSender, listener, journal, NO_OP_SNAPSHOT_SINK);
    }

    /** 기본 저널({@link InMemoryAccountJournal})로 생성한다. */
    public AccountEngine(int bufferSize, WaitStrategy waitStrategy, ProducerType producerType, long nodeId,
                         MatchingOrderSender matchingOrderSender, AccountResultListener listener) {
        this(bufferSize, waitStrategy, producerType, nodeId, matchingOrderSender, listener, new InMemoryAccountJournal());
    }

    /** 발행자가 하나뿐인 경우({@link ProducerType#SINGLE}) + 기본 저널로 생성한다. */
    public AccountEngine(int bufferSize, WaitStrategy waitStrategy, long nodeId,
                         MatchingOrderSender matchingOrderSender, AccountResultListener listener) {
        this(bufferSize, waitStrategy, ProducerType.SINGLE, nodeId, matchingOrderSender, listener);
    }

    /** 발행자 하나 + 기본 대기 전략({@link BlockingWaitStrategy}) + 기본 저널로 생성한다. */
    public AccountEngine(int bufferSize, long nodeId, MatchingOrderSender matchingOrderSender, AccountResultListener listener) {
        this(bufferSize, new BlockingWaitStrategy(), ProducerType.SINGLE, nodeId, matchingOrderSender, listener);
    }

    /** 저널을 반환한다. 테스트·복구 검증용. */
    public AccountJournal journal() {
        return journal;
    }

    /**
     * 계좌 상태를 미리 넣는다. 반드시 {@link #start} 전에 호출한다(기동 후엔 소비자와 경쟁).
     */
    public void seed(long accountId, BigDecimal balance, BigDecimal marginRate) {
        requireNotStarted();
        accounts.put(accountId, new AccountState(accountId, balance, marginRate));
    }

    /** 초기 보유(종목코드 → 수량)까지 함께 미리 넣는다. 반드시 {@link #start} 전에 호출한다. */
    public void seed(long accountId, BigDecimal balance, BigDecimal marginRate, Map<String, Integer> initialHoldings) {
        requireNotStarted();
        accounts.put(accountId, new AccountState(accountId, balance, marginRate, initialHoldings));
    }

    /** 계좌 하나의 현재 상태를 반환한다(없으면 null). 테스트·복구 검증용 — {@link #journal()}과 같은 자리. */
    public AccountState accountState(long accountId) {
        return accounts.get(accountId);
    }

    /**
     * 소비자(비즈니스 핸들러)가 실제로 처리한 시점의 체결 수신 위치를 발행자(Aeron sessionId)별로
     * 담은 맵(1-2, ADR-032 I1 D1). 발행자가 여럿이면(매칭 프로세스가 둘 이상) recording이 여럿
     * 생기므로 값 하나로는 어느 recording의 위치인지 구분할 수 없다 — 러닝 중 스냅샷(1-3)은 이
     * 맵을 써야 그 체결들을 replay 대상에서 빠뜨리지 않는다.
     */
    public Map<Integer, Long> lastAppliedFillPositions() {
        return businessHandler.lastAppliedFillPositions();
    }

    /**
     * 복구 시(단일 스레드, {@link #start} 전) 소비자의 체결 수신 위치를 스냅샷이 가리키던 값으로
     * 시드한다(1-3, 39 리뷰 지적) — 안 하면 복구 직후~첫 라이브 체결 사이에 러닝 중 스냅샷이
     * 찍힐 때 위치가 빈 채로 저장돼, 다음 복구가 체결 스트림을 처음부터 다시 replay한다. 반드시
     * {@link #restore} 이후, {@link #start} 이전에 부른다.
     */
    public void seedFillPositions(Map<Integer, Long> fillPositions) {
        requireNotStarted();
        businessHandler.seedLastAppliedFillPositions(fillPositions);
    }

    /**
     * 현재 계좌들·orderId 발급기 카운터·저널 위치를 통째로 찍는다(2d-2, ADR-019 "자체 스냅샷").
     *
     * <p>graceful shutdown(소비자 스레드 quiescent) 시점에만 안전하다 — matching
     * {@code MatchingEngine#snapshot}과 같은 이유(단일 스레드 전제가 깨진 채로 읽게 된다).</p>
     *
     * <p>저널 위치는 호스트 스레드가 {@code journal.position()}을 직접 읽지 않고 {@link
     * #businessHandler}가 처리 시점에 기억해 둔 값({@link AccountEventHandler#lastJournaledPosition()})을
     * 쓴다(1-2) — 러닝 중에는 저널러가 비즈니스 핸들러보다 앞서 나갈 수 있어, 직접 읽으면 아직
     * 반영 안 된 이벤트까지 스냅샷 경계에 포함될 수 있다.</p>
     */
    public AccountSnapshot snapshot() {
        Map<Long, AccountStateSnapshot> accountsById = new HashMap<>();
        accounts.forEach((accountId, state) -> accountsById.put(accountId, state.toSnapshot()));
        return new AccountSnapshot(accountsById, orderIdGenerator.counter(), businessHandler.lastJournaledPosition());
    }

    /**
     * 스냅샷으로 계좌들·발급기 카운터를 되살린다(2d-2). 반드시 {@link #start} 전에 호출한다 —
     * seed 이후, recover 이전에 부른다(seed가 만든 계좌를 스냅샷 값으로 그대로 바꿔 끼운다 — 계좌는
     * matching 호가창과 달리 동적으로 새로 생기지 않고 seed로만 생기므로, 스냅샷의 계좌 집합은
     * 항상 seed의 계좌 집합과 같다). 스냅샷이 있으면 marginRate 등도 스냅샷 값이 권위다 — 기존
     * 예약들이 이미 그 값으로 계산돼 있기 때문이다.
     */
    public void restore(AccountSnapshot snapshot) {
        requireNotStarted();
        snapshot.accountsById().forEach((accountId, stateSnapshot) -> accounts.put(accountId, new AccountState(stateSnapshot)));
        orderIdGenerator.restoreCounter(snapshot.generatorCounter());
    }

    /**
     * 저널 엔트리를 순서대로 재적용해 계좌 상태·dedup·orderId 발급기를 되살린다(2b-2). 반드시
     * {@link #start} 전에(설정 스레드에서만) 호출한다 — {@link #seed}와 같은 이유로, 기동 후엔
     * 소비자 스레드와 경쟁한다. 보통 seed 다음, start 이전에 부른다(seed로 초기 상태를 깔고 그
     * 위에 저널을 재생한다).
     *
     * <p>실제 비즈니스 로직({@link AccountEventHandler})을 그대로 재사용해 계좌 상태·dedup 장부를
     * 똑같이 재구성하되, 결과 리스너·매칭 발신은 no-op으로 막는다 — 이미 일어난 일을 다시 바깥에
     * 통지하거나 매칭에 재전송할 이유가 없다. 링버퍼·저널도 거치지 않는다(이미 기록된 입력을
     * 다시 저널에 넣거나 링에 발행할 이유가 없다).</p>
     *
     * <p>여기서 쓰는 {@link #orderIdGenerator}는 이 엔진의 필드라 {@link #start} 이후 라이브
     * 트래픽을 처리하는 핸들러와 같은 인스턴스다 — 리플레이가 카운터를 진행시킨 뒤 라이브가 그
     * 지점부터 이어받는다(같은 requestId를 두 번 다른 orderId로 발급하는 충돌을 막는다).</p>
     */
    public void recover(Iterable<AccountJournalEntry> entries) {
        requireNotStarted();
        // 복구 replay는 저널을 그대로 재실행하는 것뿐, 아직 시작 전이라 리플레이 중 스냅샷을
        // 찍을 이유가 없다(1-3) — no-op sink에 더해 스냅샷 트리거 자체를 끈다(39 리뷰 비블로킹 ①,
        // 큰 저널일수록 매 N건마다 전체 계좌 상태를 인코딩해 버리는 낭비가 복구 시간에 직접 얹힘 —
        // 자세한 근거는 AccountEventHandler의 snapshotTriggerEnabled 파라미터 문서 참고).
        // 발행 시각 정보가 없는 replay라 접수 지연 측정도 의미가 없다 — no-op 히스토그램.
        AccountEventHandler recoveryHandler = new AccountEventHandler(
            accounts, orderIdGenerator, NO_OP_MATCHING_ORDER_SENDER, NoOpAccountResultListener.INSTANCE,
            NO_OP_SNAPSHOT_SINK, false, NO_OP_LATENCY_HISTOGRAM);
        AccountEvent scratch = new AccountEvent();
        for (AccountJournalEntry entry : entries) {
            applyToScratch(scratch, entry);
            recoveryHandler.onEvent(scratch, 0L, false);
        }
    }

    /** 저널 엔트리 하나를 스크래치 슬롯에 채운다 — {@code AccountEvent}의 타입별 setter와 1:1 대응. */
    private void applyToScratch(AccountEvent event, AccountJournalEntry entry) {
        switch (entry.type()) {
            // 저널 엔트리엔 발행 시각도 없다(리플레이 대상은 저널이지 발행 스트림이 아니다) — 0으로 채운다.
            case BUY -> event.setBuy(entry.accountId(), entry.stockCode(), entry.price(), entry.quantity(), entry.requestId(), 0L);
            case SELL -> event.setSell(entry.accountId(), entry.stockCode(), entry.price(), entry.quantity(), entry.requestId(), 0L);
            // 저널 엔트리엔 sourcePosition·sourceSessionId가 없다(리플레이 대상 자체가 저널이지
            // 체결 수신 스트림이 아니다) — 0으로 채운다. start() 전에만 도는 복구 경로라 이후 라이브
            // 첫 체결이 곧 덮어쓴다.
            case BUY_FILL -> event.setBuyFill(entry.tradeId(), entry.orderId(), entry.accountId(), entry.stockCode(), entry.price(), entry.quantity(), 0L, 0);
            case SELL_FILL -> event.setSellFill(entry.tradeId(), entry.orderId(), entry.accountId(), entry.stockCode(), entry.quantity(), 0L, 0);
            case SETTLEMENT -> event.setSettlement(entry.tradeId(), entry.accountId(), entry.price());
        }
    }

    /** {@link #seed}·{@link #recover}가 {@link #start} 뒤에 불려 소비자 스레드와 경쟁하는 걸 막는다. */
    private void requireNotStarted() {
        if (ringBuffer != null) {
            throw new IllegalStateException("start() 이후에는 호출할 수 없습니다 — 소비자 스레드와 경쟁합니다");
        }
    }

    /** 소비자 스레드를 기동하고 링버퍼를 준비한다. */
    public void start() {
        this.ringBuffer = disruptor.start();
    }

    /**
     * 남은 이벤트를 모두 처리하고 소비자 스레드를 종료한다. 죽은 소비자 앞에서 무한 대기하지
     * 않도록 {@link #SHUTDOWN_TIMEOUT_SECONDS} 안에 못 끝나면 강제로 halt한다.
     */
    public void shutdown() {
        try {
            disruptor.shutdown(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            disruptor.halt();
        }
    }

    /**
     * 매수 검증·예약을 링버퍼에 발행한다. 발행 시각을 모르는 발행자(테스트 등)를 위한 오버로드 —
     * 지금(EpochNanos.now())을 발행 시각으로 채운다.
     *
     * <p>빈 슬롯을 예약({@code next})하고 값을 채운 뒤 발행({@code publish})한다.
     * publish 를 finally 에 둬, 값 채우는 중 예외가 나도 예약한 자리가 막히지 않게 한다.</p>
     */
    public void publishBuy(long accountId, String stockCode, BigDecimal price, int quantity, String requestId) {
        publishBuy(accountId, stockCode, price, quantity, requestId, EpochNanos.now());
    }

    /**
     * 매수 검증·예약을 링버퍼에 발행한다. publishedAtEpochNanos는 이 주문이 실제로 발행된 시각
     * (끝점① 접수 지연 측정용, decision_records/v1-v2-e2e-measurement.md) — 발신자(API 게이트웨이)가
     * 발행 직전에 찍어 와이어에 실어 보낸 값을 그대로 넘긴다.
     */
    public void publishBuy(long accountId, String stockCode, BigDecimal price, int quantity, String requestId, long publishedAtEpochNanos) {
        long sequence = ringBuffer.next();
        try {
            AccountEvent event = ringBuffer.get(sequence);
            event.setBuy(accountId, stockCode, price, quantity, requestId, publishedAtEpochNanos);
        } finally {
            ringBuffer.publish(sequence);
        }
    }

    /**
     * 매도 검증·예약을 링버퍼에 발행한다. 담보는 보유 수량이라 예약 자체엔 price 를 안 쓰지만,
     * 매칭 전달용으로 함께 싣는다(②-a). publishedAtEpochNanos을 모르는 발행자를 위한 오버로드 —
     * {@link #publishBuy(long, String, BigDecimal, int, String)}과 같은 이유.
     */
    public void publishSell(long accountId, String stockCode, BigDecimal price, int quantity, String requestId) {
        publishSell(accountId, stockCode, price, quantity, requestId, EpochNanos.now());
    }

    /** 매도 검증·예약을 링버퍼에 발행한다. publishedAtEpochNanos는 {@link #publishBuy(long, String, BigDecimal, int, String, long)}과 같다. */
    public void publishSell(long accountId, String stockCode, BigDecimal price, int quantity, String requestId, long publishedAtEpochNanos) {
        long sequence = ringBuffer.next();
        try {
            AccountEvent event = ringBuffer.get(sequence);
            event.setSell(accountId, stockCode, price, quantity, requestId, publishedAtEpochNanos);
        } finally {
            ringBuffer.publish(sequence);
        }
    }

    /**
     * 매수 체결 반영을 링버퍼에 발행한다. tradeId 는 체결 신원(멱등키). sourcePosition을 모르는
     * 발행자(테스트 등)를 위한 오버로드 — 0으로 채운다({@link #lastAppliedFillPositions()}이 이
     * 체결의 위치를 모른다는 뜻).
     *
     * @return 발행한 링버퍼 시퀀스. {@link #blockUntilJournaled}에 넘겨 이 이벤트가 저널에 기록될
     *         때까지 기다리는 데 쓴다(Kafka ack를 저널 뒤로 미루는 A안).
     */
    public long publishBuyFill(long tradeId, long orderId, long accountId, String stockCode, BigDecimal matchPrice, int quantity) {
        return publishBuyFill(tradeId, orderId, accountId, stockCode, matchPrice, quantity, 0L);
    }

    /**
     * 매수 체결 반영을 링버퍼에 발행한다. sourcePosition은 이 체결을 실어 보낸 수신 스트림의 위치(1-2,
     * {@code AccountFillReceiver}가 {@code Header.position()}으로 실어 보낸다) — 소비자가 처리한 뒤
     * {@link #lastAppliedFillPositions()}로 읽힌다. sourceSessionId를 모르는 발행자(테스트 등)를
     * 위한 오버로드 — 0으로 채운다(ADR-032 I1 D1, 발행자가 하나뿐이면 항상 0이라 구분이 필요 없다).
     */
    public long publishBuyFill(long tradeId, long orderId, long accountId, String stockCode, BigDecimal matchPrice, int quantity, long sourcePosition) {
        return publishBuyFill(tradeId, orderId, accountId, stockCode, matchPrice, quantity, sourcePosition, 0);
    }

    /**
     * 매수 체결 반영을 링버퍼에 발행한다. sourceSessionId는 sourcePosition이 어느 발행자(Aeron
     * sessionId)의 것인지 구분하는 키(ADR-032 I1 D1) — {@code AccountFillReceiver}가 {@code
     * Header.sessionId()}로 실어 보낸다.
     */
    public long publishBuyFill(long tradeId, long orderId, long accountId, String stockCode, BigDecimal matchPrice, int quantity, long sourcePosition, int sourceSessionId) {
        long sequence = ringBuffer.next();
        try {
            AccountEvent event = ringBuffer.get(sequence);
            event.setBuyFill(tradeId, orderId, accountId, stockCode, matchPrice, quantity, sourcePosition, sourceSessionId);
        } finally {
            ringBuffer.publish(sequence);
        }
        return sequence;
    }

    /** 매도 체결 반영을 링버퍼에 발행한다. sourcePosition을 모르는 발행자를 위한 오버로드 — {@link #publishBuyFill(long, long, long, String, BigDecimal, int)}과 같은 이유. */
    public long publishSellFill(long tradeId, long orderId, long accountId, String stockCode, int quantity) {
        return publishSellFill(tradeId, orderId, accountId, stockCode, quantity, 0L);
    }

    /** 매도 체결 반영을 링버퍼에 발행한다. sourceSessionId를 모르는 발행자를 위한 오버로드 — {@link #publishBuyFill(long, long, long, String, BigDecimal, int, long)}과 같은 이유. */
    public long publishSellFill(long tradeId, long orderId, long accountId, String stockCode, int quantity, long sourcePosition) {
        return publishSellFill(tradeId, orderId, accountId, stockCode, quantity, sourcePosition, 0);
    }

    /** 매도 체결 반영을 링버퍼에 발행한다. tradeId 는 체결 신원(멱등키). sourcePosition·sourceSessionId는 {@link #publishBuyFill(long, long, long, String, BigDecimal, int, long, int)}과 같다. */
    public long publishSellFill(long tradeId, long orderId, long accountId, String stockCode, int quantity, long sourcePosition, int sourceSessionId) {
        long sequence = ringBuffer.next();
        try {
            AccountEvent event = ringBuffer.get(sequence);
            event.setSellFill(tradeId, orderId, accountId, stockCode, quantity, sourcePosition, sourceSessionId);
        } finally {
            ringBuffer.publish(sequence);
        }
        return sequence;
    }

    /** 정산(T+2) 되돌림을 링버퍼에 발행한다. settlementRef 는 정산 신원(멱등키). {@link #publishBuyFill}과 같은 이유로 발행 시퀀스를 돌려준다. */
    public long publishSettlement(long settlementRef, long accountId, BigDecimal amount) {
        long sequence = ringBuffer.next();
        try {
            AccountEvent event = ringBuffer.get(sequence);
            event.setSettlement(settlementRef, accountId, amount);
        } finally {
            ringBuffer.publish(sequence);
        }
        return sequence;
    }

    /**
     * 저널러가 주어진 시퀀스까지 기록(append)을 마칠 때까지 호출 스레드를 동기로 붙잡는다(A안).
     *
     * <p>Kafka로 온 체결·정산을 발행한 컨슈머는 이 메서드로 "저널 기록 완료"를 확인한 뒤에 ack해야
     * 한다 — 그래야 저널에 durable하게 남기 전에 Kafka 오프셋이 넘어가는 유실 창이 닫힌다. 저널러는
     * {@code handleEventsWith(journal).then(business)} 게이팅으로 이미 링 시퀀스를 순서대로 처리하므로,
     * 그 핸들러의 시퀀스가 대상 시퀀스 이상이면 그 이벤트는 append가 끝난 것이다.</p>
     *
     * <p>저널 스레드가 fail-fast로 죽으면 시퀀스가 영영 안 올라온다. 그래서 {@code JOURNAL_WAIT_TIMEOUT_MILLIS}
     * 안에 도달하지 못하면 예외를 던진다 — 컨슈머는 ack하지 못하고, Kafka가 나중에 재전송한다(멱등이 중복 흡수).</p>
     */
    public void blockUntilJournaled(long sequence) {
        blockUntilJournaled(sequence, JOURNAL_WAIT_TIMEOUT_MILLIS);
    }

    /** 타임아웃을 주입받는 오버로드 — 테스트에서 짧게 줘 5초를 안 기다리게 한다(패키지 가시성). */
    void blockUntilJournaled(long sequence, long timeoutMillis) {
        long deadline = System.nanoTime() + timeoutMillis * 1_000_000L;
        while (disruptor.getSequenceValueFor(journalHandler) < sequence) {
            if (System.nanoTime() > deadline) {
                throw new JournalUnavailableException(
                    "저널이 " + timeoutMillis + "ms 안에 시퀀스 " + sequence + "를 기록하지 못했습니다");
            }
            Thread.onSpinWait();
        }
    }
}
