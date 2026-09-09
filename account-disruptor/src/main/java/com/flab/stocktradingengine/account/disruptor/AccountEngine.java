package com.flab.stocktradingengine.account.disruptor;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadFactory;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.lmax.disruptor.util.DaemonThreadFactory;

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

    private final Disruptor<AccountEvent> disruptor;
    private final Map<Long, AccountState> accounts = new HashMap<>();
    private final AccountJournal journal;
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
     */
    public AccountEngine(int bufferSize, WaitStrategy waitStrategy, ProducerType producerType, long nodeId,
                         MatchingOrderSender matchingOrderSender, AccountResultListener listener, AccountJournal journal) {
        this.journal = journal;
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
        this.disruptor.handleEventsWith(new AccountJournalEventHandler(journal))
            .then(new AccountEventHandler(accounts, new AccountOrderIdGenerator(nodeId), matchingOrderSender, listener));
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
        accounts.put(accountId, new AccountState(accountId, balance, marginRate));
    }

    /** 초기 보유(종목코드 → 수량)까지 함께 미리 넣는다. 반드시 {@link #start} 전에 호출한다. */
    public void seed(long accountId, BigDecimal balance, BigDecimal marginRate, Map<String, Integer> initialHoldings) {
        accounts.put(accountId, new AccountState(accountId, balance, marginRate, initialHoldings));
    }

    /** 소비자 스레드를 기동하고 링버퍼를 준비한다. */
    public void start() {
        this.ringBuffer = disruptor.start();
    }

    /** 남은 이벤트를 모두 처리하고 소비자 스레드를 종료한다. */
    public void shutdown() {
        disruptor.shutdown();
    }

    /**
     * 매수 검증·예약을 링버퍼에 발행한다.
     *
     * <p>빈 슬롯을 예약({@code next})하고 값을 채운 뒤 발행({@code publish})한다.
     * publish 를 finally 에 둬, 값 채우는 중 예외가 나도 예약한 자리가 막히지 않게 한다.</p>
     */
    public void publishBuy(long accountId, String stockCode, BigDecimal price, int quantity, String requestId) {
        long sequence = ringBuffer.next();
        try {
            AccountEvent event = ringBuffer.get(sequence);
            event.setBuy(accountId, stockCode, price, quantity, requestId);
        } finally {
            ringBuffer.publish(sequence);
        }
    }

    /**
     * 매도 검증·예약을 링버퍼에 발행한다. 담보는 보유 수량이라 예약 자체엔 price 를 안 쓰지만,
     * 매칭 전달용으로 함께 싣는다(②-a).
     */
    public void publishSell(long accountId, String stockCode, BigDecimal price, int quantity, String requestId) {
        long sequence = ringBuffer.next();
        try {
            AccountEvent event = ringBuffer.get(sequence);
            event.setSell(accountId, stockCode, price, quantity, requestId);
        } finally {
            ringBuffer.publish(sequence);
        }
    }

    /** 매수 체결 반영을 링버퍼에 발행한다. tradeId 는 체결 신원(멱등키). */
    public void publishBuyFill(long tradeId, long orderId, long accountId, String stockCode, BigDecimal matchPrice, int quantity) {
        long sequence = ringBuffer.next();
        try {
            AccountEvent event = ringBuffer.get(sequence);
            event.setBuyFill(tradeId, orderId, accountId, stockCode, matchPrice, quantity);
        } finally {
            ringBuffer.publish(sequence);
        }
    }

    /** 매도 체결 반영을 링버퍼에 발행한다. tradeId 는 체결 신원(멱등키). */
    public void publishSellFill(long tradeId, long orderId, long accountId, String stockCode, int quantity) {
        long sequence = ringBuffer.next();
        try {
            AccountEvent event = ringBuffer.get(sequence);
            event.setSellFill(tradeId, orderId, accountId, stockCode, quantity);
        } finally {
            ringBuffer.publish(sequence);
        }
    }

    /** 정산(T+2) 되돌림을 링버퍼에 발행한다. settlementRef 는 정산 신원(멱등키). */
    public void publishSettlement(long settlementRef, long accountId, BigDecimal amount) {
        long sequence = ringBuffer.next();
        try {
            AccountEvent event = ringBuffer.get(sequence);
            event.setSettlement(settlementRef, accountId, amount);
        } finally {
            ringBuffer.publish(sequence);
        }
    }
}
