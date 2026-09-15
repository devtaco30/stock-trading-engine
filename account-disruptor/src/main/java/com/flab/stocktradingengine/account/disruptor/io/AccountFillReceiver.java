package com.flab.stocktradingengine.account.disruptor.io;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import org.agrona.DirectBuffer;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;

import io.aeron.Image;
import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;

import com.flab.stocktradingengine.account.disruptor.engine.AccountEngine;
import com.flab.stocktradingengine.codec.FillCodec;
import com.flab.stocktradingengine.codec.FilledTrade;

/**
 * ADR-032, U3 — Aeron으로 들어온 체결을 계좌 엔진에 반영하는 수신 게이트웨이.
 * {@link AccountOrderReceiver}와 같은 결(전용 폴 스레드가 {@link Subscription}을 계속 폴링해
 * 도착한 바이트를 디코딩하고 {@link AccountEngine}의 발행 메서드로 링버퍼에 넣는다)이지만
 * 두 가지가 다르다.
 *
 * <h3>tryDecode — content-poison 방어</h3>
 * <p>체결은 매칭이 이미 검증한 데이터라 decode 실패는 저장소·전송 손상(poison)만을 뜻한다.
 * {@link FillCodec#tryDecode}가 빈 값을 돌려주면 그 fragment만 조용히 skip하고 계속 돈다 —
 * 예외를 던져 폴 스레드를 죽이지 않는다.</p>
 *
 * <h3>체결 1건 = publishBuyFill + publishSellFill 둘 다</h3>
 * <p>단일 계좌 엔진이 매수·매도 계좌를 둘 다 소유할 수도, 한쪽만 소유할 수도 있다 — 소유하지
 * 않은 쪽은 엔진이 ACCOUNT_NOT_FOUND로 스스로 격리하므로 여기서 미리 구분하지 않는다. tradeId
 * 멱등이 중복 도착도 흡수한다({@code AccountFillConsumer}(U2 이전 Kafka 구현)와 같은 반영 방식).</p>
 *
 * <h3>ack·durability 없음 (라이브 경로만, U4에서 닫는다)</h3>
 * <p>Kafka 시절의 {@code AccountFillConsumer}는 저널 기록 확인 뒤에 ack했다(A안) — offset 커밋을
 * 저널 뒤로 미뤄 유실 창을 닫는 규약이었다. Aeron Archive에는 그 규약이 없다 — position 추적·
 * 크래시 복구는 U4 몫이고, 이 유닛은 라이브 경로만 닫는다.</p>
 *
 * <h3>소비 position 추적 (U4a)</h3>
 * <p>{@link #consumedPosition()}은 이 수신기가 지금까지 소비한 스트림 위치를 돌려준다 — graceful
 * shutdown(quiescent) 시점에 스냅샷 라이프사이클(다른 스레드)이 읽어 저장한다({@code
 * AccountSnapshotLifecycle}). 폴 스레드가 쓰고 다른 스레드가 읽으므로 필드를 volatile로 둔다.</p>
 */
public final class AccountFillReceiver implements AutoCloseable {

    private static final int FRAGMENT_LIMIT = 10;
    private static final long CLOSE_JOIN_TIMEOUT_MILLIS = 1000L;

    private final Subscription subscription;
    private final AccountEngine engine;
    private final FillCodec codec = new FillCodec();
    private final IdleStrategy idleStrategy = new BackoffIdleStrategy();
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final FragmentHandler fragmentHandler = this::onFragment;

    private volatile long consumedPosition = 0L;
    private Thread pollThread;

    public AccountFillReceiver(Subscription subscription, AccountEngine engine) {
        this.subscription = subscription;
        this.engine = engine;
    }

    /** 폴링 스레드를 기동한다. */
    public void start() {
        running.set(true);
        pollThread = new Thread(this::pollLoop, "aeron-account-fill-receiver");
        pollThread.setDaemon(true);
        pollThread.start();
    }

    private void pollLoop() {
        while (running.get()) {
            int fragments = subscription.poll(fragmentHandler, FRAGMENT_LIMIT);
            updateConsumedPosition();
            idleStrategy.idle(fragments);
        }
    }

    /** image가 아직 연결되기 전(발행자가 아직 안 붙음)이면 건드리지 않는다 — 초기값 0이 안전하다. */
    private void updateConsumedPosition() {
        if (subscription.imageCount() == 0) {
            return;
        }
        Image image = subscription.imageAtIndex(0);
        consumedPosition = image.position();
    }

    /** 이 수신기가 지금까지 소비한 스트림 위치. 스냅샷 라이프사이클(다른 스레드)이 읽는다. */
    public long consumedPosition() {
        return consumedPosition;
    }

    /**
     * 패키지 가시성 — 단위 테스트가 실제 Subscription 없이 이 메서드를 직접 호출한다.
     *
     * <p>{@code header.position()}(1-2)은 "이 메시지를 읽은 뒤 image가 도달한 위치"다({@code
     * image.position()}과 같은 값) — 소비자(계좌 엔진)가 이 체결을 실제로 반영한 뒤 {@code
     * lastAppliedFillPosition}으로 기억해, 아직 링에만 들어가고 처리는 안 된 체결의 위치와
     * 구분한다. header가 null이면(Aeron 없이 디코딩만 검증하는 단위 테스트) 0으로 둔다.</p>
     */
    void onFragment(DirectBuffer buffer, int offset, int length, Header header) {
        Optional<FilledTrade> decoded = codec.tryDecode(buffer, offset);
        if (decoded.isEmpty()) {
            return;
        }
        FilledTrade trade = decoded.get();
        long sourcePosition = header != null ? header.position() : 0L;
        engine.publishBuyFill(trade.tradeId(), trade.buyOrderId(), trade.buyAccountId(),
            trade.stockCode(), trade.matchPrice(), trade.filledQuantity(), sourcePosition);
        engine.publishSellFill(trade.tradeId(), trade.sellOrderId(), trade.sellAccountId(),
            trade.stockCode(), trade.filledQuantity(), sourcePosition);
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
