package com.flab.stocktradingengine.matching.disruptor;

import java.nio.ByteBuffer;
import java.util.List;

import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;

import com.flab.stocktradingengine.codec.JournaledOrder;
import com.flab.stocktradingengine.codec.OrderCodec;

import io.aeron.ExclusivePublication;
import io.aeron.Publication;

/**
 * 매칭 저널을 Aeron Archive 로 durable 하게 내보내는 {@link Journal} 구현(2c-1).
 * account-disruptor 의 {@code AeronArchiveAccountJournal}과 같은 결이다.
 *
 * <p>{@link #append}는 {@link JournalEventHandler}(저널 게이팅 단계)의 단일 스레드에서만
 * 불린다 — 이 발행 스트림은 그 스레드 하나만 쓰므로 {@link ExclusivePublication}을 받는다. 워커
 * 호스트가 이 스트림을 Archive 로 녹화해야 디스크에 durable 하게 남는다(녹화 자체는 호스트 배선이
 * 담당, 여기는 스트림에 내보내기만 한다).</p>
 *
 * <h3>never-drop — offer 를 성공할 때까지 재시도</h3>
 * <p>저널은 리플레이(2c-2)가 호가창을 되살리는 유일한 근거라 하나라도 빠지면 복구가 어긋난다.
 * 그래서 {@code offer} 가 성공(반환값 ≥ 0)할 때까지 {@link BackoffIdleStrategy}로 재시도한다.
 * NOT_CONNECTED·BACK_PRESSURED·ADMIN_ACTION 은 일시적이라 재시도로 넘어가지만, CLOSED·
 * MAX_POSITION_EXCEEDED 는 이 스트림이 복구 불가 상태라는 뜻이라 예외를 던진다 —
 * {@link MatchingExceptionHandler}가 그 예외를 받아 저널 소비자 스레드를 fail-fast 로 멈춘다
 * (오염된 상태 위에서 계속 진행하지 않는다).</p>
 *
 * <p>호출 스레드가 하나뿐이라({@code append}가 저널러 단일 스레드에서만 불린다) 인코딩 버퍼를
 * 필드로 재사용해도 안전하다.</p>
 */
public class AeronArchiveMatchingJournal implements Journal {

    private static final int ENCODE_BUFFER_SIZE = 256;

    private final ExclusivePublication journalPublication;
    private final OrderCodec codec = new OrderCodec();
    private final UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(ENCODE_BUFFER_SIZE));
    private final IdleStrategy idleStrategy = new BackoffIdleStrategy();

    public AeronArchiveMatchingJournal(ExclusivePublication journalPublication) {
        this.journalPublication = journalPublication;
    }

    @Override
    public void append(JournaledOrder order) {
        int length = codec.encode(buffer, 0, order);

        idleStrategy.reset();
        long result;
        while ((result = journalPublication.offer(buffer, 0, length)) < 0) {
            if (result == Publication.CLOSED || result == Publication.MAX_POSITION_EXCEEDED) {
                throw new IllegalStateException(
                    "매칭 저널 스트림이 복구 불가 상태입니다(offer 결과=" + result + "): 리플레이 신뢰가 깨지므로 소비자를 멈춥니다");
            }
            idleStrategy.idle();
        }
    }

    /** 읽기(read-back)는 2c-2 리플레이가 Archive 에서 직접 하므로 여기서는 지원하지 않는다. */
    @Override
    public List<JournaledOrder> entries() {
        throw new UnsupportedOperationException("Aeron Archive 저널의 read-back 은 2c-2 리플레이에서 다룬다");
    }

    /** 발행 스트림의 현재 위치. 스냅샷(2d-1)이 "여기까지는 스냅샷에 담겼다"로 함께 저장한다. */
    @Override
    public long position() {
        return journalPublication.position();
    }
}
