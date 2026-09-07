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
 * 링버퍼 하나 + 소비자 하나이며, 계좌 검증·예약을 여기서 직렬 처리한다. 매칭과 다른 축(계좌)이라
 * 별도 링버퍼다. B2 는 저널 없이 검증·예약만 한다.</p>
 *
 * <h3>시드 후 기동</h3>
 * <p>계좌 상태는 DB 없이 {@link #seed} 로 미리 넣는다. 시드는 소비자 스레드가 뜨기 전
 * ({@link #start} 이전) 설정 스레드에서만 호출해야 한다 — 기동 후 시드하면 소비자와 경쟁한다.</p>
 *
 * <h3>ProducerType.SINGLE</h3>
 * <p>B2 는 피더(테스트/이후 Aeron 수신 스레드)가 하나다. 여러 프로듀서가 필요해지면 MULTI 로 바꾼다.</p>
 */
public class AccountEngine {

    private final Disruptor<AccountEvent> disruptor;
    private final Map<Long, AccountState> accounts = new HashMap<>();
    private RingBuffer<AccountEvent> ringBuffer;

    public AccountEngine(int bufferSize, WaitStrategy waitStrategy, AccountResultListener listener) {
        ThreadFactory threadFactory = DaemonThreadFactory.INSTANCE;
        this.disruptor = new Disruptor<>(
            AccountEvent::new,
            bufferSize,
            threadFactory,
            ProducerType.SINGLE,
            waitStrategy
        );
        // 예상 못한 예외는 fail-fast(handleEventsWith 배선 전에 설정해야 적용됨).
        this.disruptor.setDefaultExceptionHandler(new AccountExceptionHandler());
        this.disruptor.handleEventsWith(new AccountEventHandler(accounts, listener));
    }

    /** 기본 대기 전략({@link BlockingWaitStrategy})으로 생성한다. */
    public AccountEngine(int bufferSize, AccountResultListener listener) {
        this(bufferSize, new BlockingWaitStrategy(), listener);
    }

    /**
     * 계좌 상태를 미리 넣는다. 반드시 {@link #start} 전에 호출한다(기동 후엔 소비자와 경쟁).
     */
    public void seed(long accountId, BigDecimal balance, BigDecimal marginRate) {
        accounts.put(accountId, new AccountState(accountId, balance, marginRate));
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
    public void publishBuy(long orderId, long accountId, String stockCode, BigDecimal price, int quantity, String requestId) {
        long sequence = ringBuffer.next();
        try {
            AccountEvent event = ringBuffer.get(sequence);
            event.setBuy(orderId, accountId, stockCode, price, quantity, requestId);
        } finally {
            ringBuffer.publish(sequence);
        }
    }

    /** 매도 검증·예약을 링버퍼에 발행한다. 담보가 돈이 아니라 보유 수량이라 price 는 없다. */
    public void publishSell(long orderId, long accountId, String stockCode, int quantity, String requestId) {
        long sequence = ringBuffer.next();
        try {
            AccountEvent event = ringBuffer.get(sequence);
            event.setSell(orderId, accountId, stockCode, quantity, requestId);
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
}
