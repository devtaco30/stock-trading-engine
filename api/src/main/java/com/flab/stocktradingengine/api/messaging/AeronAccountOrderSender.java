package com.flab.stocktradingengine.api.messaging;

import java.math.BigDecimal;
import java.nio.ByteBuffer;

import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;

import com.flab.stocktradingengine.api.exception.OrderPublishException;
import com.flab.stocktradingengine.codec.AccountOrderCodec;
import com.flab.stocktradingengine.codec.DecodedAccountOrder;
import com.flab.stocktradingengine.trading.entity.OrderSide;

import io.aeron.Publication;

/**
 * fork5, U1b — v2 게이트웨이가 검증을 통과한 주문을 계좌 인테이크
 * ({@link com.flab.stocktradingengine.api.config.AccountOrderPublishConfig})로 Aeron 발신한다.
 *
 * <h3>동기 발신 — {@code AeronMatchingOrderSender}(outbox+전용 스레드)와 다르다</h3>
 * <p>계좌 워커의 {@code AeronMatchingOrderSender}는 발신을 outbox 큐+전용 스레드로 비동기화한다 —
 * 계좌 엔진의 단일 상시 컨슈머(single-writer) 스레드를 Aeron {@code offer}(I/O)로부터 보호하기
 * 위해서다. 이 클래스는 그 장치를 미러하지 않는다: 호출자가 HTTP 요청 스레드 하나뿐이고, 그
 * 스레드는 어차피 이 요청 하나의 응답을 기다리는 중이라 보호할 "다른 로직 스레드"가 없다.
 * 오히려 {@code offer} 결과를 그대로 응답(202/503)에 실어야 하므로 동기라야 한다.</p>
 *
 * <h3>발신 실패 = 503, 주문을 조용히 버리지 않는다</h3>
 * <p>{@code offer}가 실패(≤0)하면 짧게 재시도한다. 그래도 실패하면(스트림 정체·복구 불가)
 * {@link OrderPublishException}을 던져 503으로 응답한다 — 여기서 로그만 남기고 넘어가면(계좌
 * 워커의 best-effort와 같은 방식) 이 요청의 유일한 발신 기회가 조용히 사라진다(돈). 클라이언트가
 * 같은 requestId로 재전송하면 계좌 엔진의 멱등이 중복 예약을 막아준다.</p>
 */
public class AeronAccountOrderSender {

    private static final int ENCODE_BUFFER_SIZE = 256;
    private static final int MAX_ATTEMPTS = 3;

    private final Publication accountOrderPublication;
    private final AccountOrderCodec codec = new AccountOrderCodec();

    public AeronAccountOrderSender(Publication accountOrderPublication) {
        this.accountOrderPublication = accountOrderPublication;
    }

    /**
     * 매수·매도 주문을 계좌 인테이크로 동기 발신한다. orderId는 싣지 않는다(C5-2a — 계좌 워커가
     * requestId 첫 접수 시점에 직접 발급한다).
     *
     * <p>이 인스턴스는 싱글톤 빈이라 여러 HTTP 요청 스레드가 동시에 {@code send}를 부른다.
     * {@link BackoffIdleStrategy}는 내부에 spin/yield/park 카운터를 갖는 비스레드안전 객체라
     * 필드로 공유하면 동시 재시도 때 백오프 상태가 서로 덮어써 뒤섞인다 — 그래서 호출마다
     * 지역 변수로 새로 만든다({@link AccountOrderCodec}은 필드가 없는 순수 인코더라 공유해도
     * 안전하다).</p>
     *
     * @throws OrderPublishException {@link #MAX_ATTEMPTS}번 재시도해도 offer가 성공하지 못하면
     */
    public void send(OrderSide side, long accountId, String stockCode, BigDecimal price, int quantity, String requestId) {
        DecodedAccountOrder order = new DecodedAccountOrder(side, accountId, stockCode, price, quantity, requestId);
        UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(ENCODE_BUFFER_SIZE));
        int length = codec.encode(buffer, 0, order);

        IdleStrategy idleStrategy = new BackoffIdleStrategy();
        long result = -1;
        for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
            result = accountOrderPublication.offer(buffer, 0, length);
            if (result > 0) {
                return;
            }
            idleStrategy.idle();
        }
        throw new OrderPublishException(
            "계좌 인테이크로 주문 발신 실패(재시도 " + MAX_ATTEMPTS + "회 소진): accountId=" + accountId
                + " requestId=" + requestId + " offer 결과=" + result);
    }
}
