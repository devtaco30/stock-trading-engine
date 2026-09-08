package com.flab.stocktradingengine.account.disruptor;

import java.util.concurrent.atomic.AtomicBoolean;

import org.agrona.DirectBuffer;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;

import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;

/**
 * C5-1b — Aeron으로 들어온 매수·매도 주문을 계좌 엔진에 넣는 수신 게이트웨이.
 *
 * <p>matching-disruptor의 {@code AeronOrderReceiver}와 같은 결이다. 전용 스레드가 Aeron
 * {@link Subscription}을 계속 폴링해, 도착한 바이트를 {@link AccountOrderCodec}로 디코딩하고
 * {@link AccountEngine}의 발행 메서드({@code publishBuy}/{@code publishSell})로 링버퍼에 넣는다.</p>
 *
 * <h3>단일 프로듀서 유지</h3>
 * <p>이 폴링 스레드 하나만 publishBuy·publishSell을 호출한다. {@link AccountEngine}은
 * {@code ProducerType.MULTI}로 만들어지지만(account-fills 컨슈머 스레드와 이 스레드, 둘이
 * 동시에 발행하므로) — 두 스레드 각각은 여전히 자기 발행 메서드만 단독으로 호출한다.</p>
 */
public final class AccountOrderReceiver implements AutoCloseable {

    private static final int FRAGMENT_LIMIT = 10;
    private static final long CLOSE_JOIN_TIMEOUT_MILLIS = 1000L;

    private final Subscription subscription;
    private final AccountEngine engine;
    private final AccountOrderCodec codec = new AccountOrderCodec();
    private final IdleStrategy idleStrategy = new BackoffIdleStrategy();
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final FragmentHandler fragmentHandler = this::onFragment;

    private Thread pollThread;

    public AccountOrderReceiver(Subscription subscription, AccountEngine engine) {
        this.subscription = subscription;
        this.engine = engine;
    }

    /** 폴링 스레드를 기동한다. */
    public void start() {
        running.set(true);
        pollThread = new Thread(this::pollLoop, "aeron-account-order-receiver");
        pollThread.setDaemon(true);
        pollThread.start();
    }

    private void pollLoop() {
        while (running.get()) {
            int fragments = subscription.poll(fragmentHandler, FRAGMENT_LIMIT);
            idleStrategy.idle(fragments);
        }
    }

    private void onFragment(DirectBuffer buffer, int offset, int length, Header header) {
        DecodedAccountOrder order = codec.decode(buffer, offset);
        if (order.type() == EventType.BUY) {
            engine.publishBuy(
                order.orderId(), order.accountId(), order.stockCode(),
                order.price(), order.quantity(), order.requestId());
        } else {
            engine.publishSell(
                order.orderId(), order.accountId(), order.stockCode(),
                order.quantity(), order.requestId());
        }
    }

    /** 폴링 스레드를 멈추고 종료를 기다린다. */
    @Override
    public void close() {
        running.set(false);
        if (pollThread != null) {
            try {
                pollThread.join(CLOSE_JOIN_TIMEOUT_MILLIS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
