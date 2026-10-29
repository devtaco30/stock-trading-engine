package com.flab.stocktradingengine.account.worker.messaging;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import org.agrona.DirectBuffer;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;

import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;

import com.flab.stocktradingengine.codec.JournaledOrder;
import com.flab.stocktradingengine.codec.OrderCodec;

import lombok.extern.slf4j.Slf4j;

/**
 * 매칭 샤딩 U9d(계좌 쪽) — 매칭 pod가 담당이 아니라며 돌려보낸 주문을 읽는 수신기. 체결을 받는 주소
 * ({@code transport.fill.channel})에서 스트림 번호 {@code AeronStreamIds.REJECT_REPLY}만 구독하고,
 * 전용 poll 스레드가 {@link OrderCodec#tryDecode}로 읽어 거부 회신 입구
 * ({@link AeronMatchingOrderSender#acceptRejectedOrder})에 넘긴다. 계좌 엔진 링버퍼에는 넣지 않고
 * Archive 녹화나 위치 추적도 하지 않는다. 주문 한 건이 256바이트 이하라 조각난 메시지는 없다.
 *
 * <p>디코딩에 실패하면 그 메시지만 ERROR 로그를 남기고 건너뛴다 — 예외로 poll 스레드를 죽이지 않는다.
 * 입구가 가득 차면 {@code acceptRejectedOrder}가 이 스레드 안에서 기다린다(버리지 않는다).</p>
 */
@Slf4j
public final class MatchingRejectReplyReceiver implements AutoCloseable {

    private static final int FRAGMENT_LIMIT = 10;
    private static final long CLOSE_JOIN_TIMEOUT_MILLIS = 1000L;

    private final Subscription subscription;
    private final Consumer<JournaledOrder> rejectedOrderSink;
    private final OrderCodec codec = new OrderCodec();
    private final IdleStrategy idleStrategy = new BackoffIdleStrategy();
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final FragmentHandler fragmentHandler = this::onFragment;

    private Thread pollThread;

    public MatchingRejectReplyReceiver(Subscription subscription, Consumer<JournaledOrder> rejectedOrderSink) {
        this.subscription = subscription;
        this.rejectedOrderSink = rejectedOrderSink;
    }

    public void start() {
        running.set(true);
        pollThread = new Thread(this::pollLoop, "aeron-matching-reject-reply-receiver");
        pollThread.setDaemon(true);
        pollThread.start();
    }

    private void pollLoop() {
        while (running.get()) {
            int fragments = subscription.poll(fragmentHandler, FRAGMENT_LIMIT);
            idleStrategy.idle(fragments);
        }
    }

    /** 패키지 가시성 — 단위 시험이 실제 Subscription 없이 직접 호출한다. */
    void onFragment(DirectBuffer buffer, int offset, int length, Header header) {
        Optional<JournaledOrder> decoded = codec.tryDecode(buffer, offset);
        if (decoded.isEmpty()) {
            log.error("[계좌] 거부 회신 메시지를 디코딩하지 못해 건너뜁니다: length={}", length);
            return;
        }
        rejectedOrderSink.accept(decoded.get());
    }

    @Override
    public void close() {
        running.set(false);
        if (pollThread != null) {
            try {
                pollThread.join(CLOSE_JOIN_TIMEOUT_MILLIS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            if (pollThread.isAlive()) {
                log.warn("[계좌] 거부 회신 수신 폴링 스레드가 {}ms 안에 멈추지 않았습니다", CLOSE_JOIN_TIMEOUT_MILLIS);
            }
        }
    }
}
