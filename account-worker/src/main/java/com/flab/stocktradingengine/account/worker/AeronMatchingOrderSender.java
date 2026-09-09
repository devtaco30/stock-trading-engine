package com.flab.stocktradingengine.account.worker;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;

import org.agrona.concurrent.UnsafeBuffer;

import com.flab.stocktradingengine.account.disruptor.MatchingOrderSender;
import com.flab.stocktradingengine.codec.EventType;
import com.flab.stocktradingengine.codec.JournaledOrder;
import com.flab.stocktradingengine.codec.OrderCodec;
import com.flab.stocktradingengine.trading.entity.OrderSide;

import io.aeron.Publication;

import lombok.extern.slf4j.Slf4j;

/**
 * {@link MatchingOrderSender}의 Aeron 구현(②-b) — 계좌가 accept한 매수·매도를 매칭 인테이크
 * (파이프라인 연결 ①, stream {@link MatchingOrderSenderConfig#MATCHING_STREAM_ID})로 인코딩해 보낸다.
 *
 * <h3>best-effort·non-blocking</h3>
 * <p>이 발신은 계좌 엔진의 단일 상시 컨슈머 스레드 안에서 동기 호출된다(fire-and-forget) — 그래서
 * {@code offer}를 재시도하거나 실패를 예외로 던지면 안 된다. 재시도 루프를 두면 구독자가 없을 때마다
 * 컨슈머 스레드를 블로킹하고, 예외를 던지면 fail-fast 정책({@code AccountExceptionHandler})이 계좌
 * 엔진 전체를 죽인다 — 매칭 발신 실패가 계좌 예약 처리를 막아선 안 된다(LMAX의 single-writer는 I/O로
 * 블로킹·중단되면 안 된다는 원칙). 그래서 {@code offer} 는 딱 한 번만 부르고, 실패(구독자 없음·
 * 백프레셔 등 음수 반환)는 로그만 남기고 넘어간다. 이 주문은 매칭에 발신되지 않은 채 계좌엔 예약만
 * 남는다 — 보장 전달·내구성(버퍼링, Aeron Archive 리플레이, 또는 발신을 별도 스레드로 분리)은
 * 이 유닛 범위 밖이다(이후 단위: 전송반전 내구성 / C5-4).</p>
 *
 * <p>코어는 시간을 모르므로 주문 시각(orderAt)은 여기서 {@link Instant#now()}로 직접 찍는다
 * (Clock 주입 금지 — 가장자리에서 now를 읽고 계산은 순수 함수로 두는 정책과 같은 결).</p>
 *
 * <p>범위는 인프로세스까지다 — 실제 두 프로세스(account-worker↔matching-worker) 연결과
 * accountId/stockCode 라우팅은 C5-4에서 다룬다.</p>
 */
@Slf4j
public class AeronMatchingOrderSender implements MatchingOrderSender {

    private static final int ENCODE_BUFFER_SIZE = 256;

    private final Publication matchingOrderPublication;
    private final OrderCodec codec = new OrderCodec();

    public AeronMatchingOrderSender(Publication matchingOrderPublication) {
        this.matchingOrderPublication = matchingOrderPublication;
    }

    @Override
    public void forwardPlace(long orderId, long accountId, String stockCode, OrderSide side, BigDecimal price, int quantity) {
        JournaledOrder order = new JournaledOrder(
            EventType.PLACE, orderId, accountId, stockCode, side, price, quantity, Instant.now());

        UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(ENCODE_BUFFER_SIZE));
        int length = codec.encode(buffer, 0, order);

        long result = matchingOrderPublication.offer(buffer, 0, length);
        if (result <= 0) {
            log.warn("[계좌] 매칭 발신 실패(best-effort, 재시도 없음): orderId={} accountId={} offer 반환={}",
                orderId, accountId, result);
        }
    }
}
