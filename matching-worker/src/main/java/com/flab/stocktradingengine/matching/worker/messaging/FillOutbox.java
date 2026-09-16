package com.flab.stocktradingengine.matching.worker.messaging;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.OneToOneConcurrentArrayQueue;
import org.agrona.concurrent.UnsafeBuffer;

import com.flab.stocktradingengine.codec.FillCodec;
import com.flab.stocktradingengine.codec.FilledTrade;

import io.aeron.ExclusivePublication;
import io.aeron.Publication;

import lombok.extern.slf4j.Slf4j;

/**
 * 체결을 계좌 샤드 endpoint 하나로 보내는 발신 통로(I4 — 매칭 단일 소비자 스레드가 발신 재시도에
 * 묶이는 문제를 닫는 유닛). endpoint마다 이 인스턴스를 하나씩 둬 큐 하나·전용 스레드 하나·인코딩
 * 버퍼 하나를 갖는다(D1). endpoint A로 가는 재시도가 오래 걸려도 A의 큐·스레드만 막히고, endpoint
 * B의 큐·스레드는 영향받지 않는다 — 하나의 공유 큐를 썼다면 A 뒤에 줄 선 B행 체결도 같이 막혔을
 * 것이다(head-of-line blocking).
 *
 * <h3>버리지 않는다(D2)</h3>
 * <p>{@link #enqueueNeverDrop}은 매칭 단일 소비자 스레드에서 불린다. 큐가 가득 차면(용량={@value
 * #QUEUE_CAPACITY}) 버리는 대신 그 자리에서 대기한다 — 큐 용량만큼은 이 목적지가 잠깐 안 받아도
 * 매칭이 계속 돈다. 목적지가 영구히 안 살아나는 경우(계좌 프로세스가 다시 안 뜨는 것)까지 닫는
 * 것은 이 유닛 밖이다(대기 프로세스를 두는 I8의 몫).</p>
 *
 * <h3>인코딩은 전용 스레드에서(D3)</h3>
 * <p>{@link #publisherThread} 하나만 {@link #encodeBuffer}에 접근하므로 필드로 재사용해도 안전하다.
 * tradeId 발급은 이 클래스로 옮기지 않는다 — 매칭 소비자 스레드({@link AccountFillPublisher#onFill})가
 * 체결이 일어난 순서 그대로 발급해야 tradeId 순서가 어긋나지 않는다.</p>
 *
 * <h3>일시적 실패와 복구 불가 실패를 다르게 다룬다</h3>
 * <p>{@code BACK_PRESSURED} 같은 일시적 실패는 {@link #publisherThread}가 조용히 계속 재시도한다
 * (D2) — I4가 벌어주려는 시간이 바로 이 구간이다. 반면 {@code offer}가 {@code CLOSED}·
 * {@code MAX_POSITION_EXCEEDED}를 돌려주면 이 목적지로는 다시 보낼 수 없다는 뜻이라 성격이 다르다.
 * 이때는 {@link #fatalError}에 예외를 기록하고 {@link #publisherThread}가 그 예외로 죽는다(ERROR
 * 로그로 남음). 이 스레드는 Disruptor 핸들러가 아니라 예외를 던져도 매칭 소비자 스레드까지 자동으로
 * 전파되지 않으므로, {@link #enqueueNeverDrop}이 호출될 때마다 {@link #fatalError}를 확인해 매칭
 * 소비자 스레드 자신에서 다시 던진다 — 그래야 {@code MatchingExceptionHandler}가 예전처럼 매칭
 * 전체를 fail-fast로 멈출 수 있다(조용히 쌓이다 큐가 찼을 때에야 막히는 걸 막는다).</p>
 */
@Slf4j
class FillOutbox {

    private static final int ENCODE_BUFFER_SIZE = 256;
    static final int QUEUE_CAPACITY = 65536;

    private final String endpoint;
    private final ExclusivePublication publication;
    private final FillCodec codec;
    private final OneToOneConcurrentArrayQueue<FilledTrade> queue = new OneToOneConcurrentArrayQueue<>(QUEUE_CAPACITY);
    private final IdleStrategy enqueueIdleStrategy = new BackoffIdleStrategy();
    private final IdleStrategy pollIdleStrategy = new BackoffIdleStrategy();
    private final IdleStrategy offerIdleStrategy = new BackoffIdleStrategy();
    private final UnsafeBuffer encodeBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(ENCODE_BUFFER_SIZE));
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicReference<IllegalStateException> fatalError = new AtomicReference<>();

    private Thread publisherThread;

    FillOutbox(String endpoint, ExclusivePublication publication, FillCodec codec) {
        this.endpoint = endpoint;
        this.publication = publication;
        this.codec = codec;
    }

    /**
     * 매칭 소비자 스레드가 호출한다. 큐가 가득 차면 버리지 않고 그 자리에서 기다린다(D2). 이
     * endpoint가 복구 불가 상태로 기록돼 있으면 큐에 넣지 않고 그 자리에서 예외를 던진다 — 그래야
     * 이 호출을 부른 {@link com.flab.stocktradingengine.matching.worker.messaging.AccountFillPublisher#onFill}까지
     * 예외가 올라가 {@code MatchingExceptionHandler}가 매칭 전체를 멈춘다.
     */
    void enqueueNeverDrop(FilledTrade trade) {
        throwIfFatal();
        enqueueIdleStrategy.reset();
        while (!queue.offer(trade)) {
            throwIfFatal();
            enqueueIdleStrategy.idle();
        }
    }

    private void throwIfFatal() {
        IllegalStateException error = fatalError.get();
        if (error != null) {
            throw new IllegalStateException(error.getMessage(), error);
        }
    }

    /** 이 endpoint 전용 발신 스레드를 기동한다. */
    void start() {
        running.set(true);
        publisherThread = new Thread(this::publishLoop, "fill-outbox-" + endpoint);
        publisherThread.setDaemon(true);
        publisherThread.setUncaughtExceptionHandler((thread, error) ->
            log.error("[매칭] 체결 발신 스레드가 복구 불가 상태로 멈췄습니다: endpoint={}", endpoint, error));
        publisherThread.start();
    }

    private void publishLoop() {
        while (running.get() || !queue.isEmpty()) {
            FilledTrade trade = queue.poll();
            if (trade != null) {
                publish(trade);
            }
            pollIdleStrategy.idle(trade != null ? 1 : 0);
        }
    }

    /** 전용 스레드에서만 돈다 — 여기서 블로킹 재시도해도 매칭 소비자 스레드를 막지 않는다. */
    private void publish(FilledTrade trade) {
        int length = codec.encode(encodeBuffer, 0, trade);

        offerIdleStrategy.reset();
        long result;
        while ((result = publication.offer(encodeBuffer, 0, length)) < 0) {
            if (result == Publication.CLOSED || result == Publication.MAX_POSITION_EXCEEDED) {
                IllegalStateException error = new IllegalStateException(
                    "체결 발행 스트림이 복구 불가 상태입니다(offer 결과=" + result
                        + ", 목적지=" + endpoint + "): 계좌 축이 이 체결을 영원히 못 받으므로 매칭을 멈춥니다");
                fatalError.set(error);
                throw error;
            }
            offerIdleStrategy.idle();
        }
    }

    /**
     * 발신 스레드를 멈추고 큐에 남은 체결을 드레인한다(D4). {@code drainTimeoutMillis} 안에 드레인이
     * 끝나지 않으면 남은 개수를 ERROR로 남긴다 — 조용히 사라지는 자리를 만들지 않는다.
     */
    void close(long drainTimeoutMillis) {
        running.set(false);
        if (publisherThread == null) {
            return;
        }

        try {
            publisherThread.join(drainTimeoutMillis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // 스레드가 아직 살아 있다는 건 큐가 빈 뒤에도(publishLoop 종료 조건) 안 끝났다는 뜻이라,
        // 지금 발신 재시도 중인 체결 1건이 있다 — queue.size()만 세면 그 1건이 빠진다. +1이 정확히
        // 1인 이유: publishLoop가 멈춰 있을 수 있는 자리는 publish() 안의 재시도 루프뿐이고, 그
        // 루프는 poll()로 큐에서 꺼낸 trade 하나를 손에 든 채로 돈다 — 항상 정확히 한 건이다.
        if (publisherThread.isAlive()) {
            int remaining = queue.size() + 1;
            log.error("[매칭] 체결 발신 드레인이 타임아웃 안에 끝나지 않았습니다 — 유실 가능한 체결 {}건 남음: endpoint={}",
                remaining, endpoint);
        }
    }
}
