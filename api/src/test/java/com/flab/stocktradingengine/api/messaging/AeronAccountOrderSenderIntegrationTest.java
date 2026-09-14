package com.flab.stocktradingengine.api.messaging;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import org.agrona.DirectBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.flab.stocktradingengine.codec.AccountOrderCodec;
import com.flab.stocktradingengine.codec.DecodedAccountOrder;
import com.flab.stocktradingengine.trading.entity.OrderSide;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import io.aeron.logbuffer.FragmentHandler;

/**
 * fork5, U1b — 같은 JVM 임베디드 드라이버 위에 api 발행 스트림과 계좌 인테이크 구독을 같은
 * 채널·스트림으로 붙여, {@link AeronAccountOrderSender}가 보낸 바이트를 그대로 디코딩할 수
 * 있는지 확인한다(end-to-end, 매수·매도 둘 다). 실제 두 프로세스(api↔account-worker) 연결과
 * accountId 라우팅은 C5 몫 — 이 테스트는 발신 로직·인코딩까지만 검증한다.
 *
 * <p>{@code AeronAccountEndToEndTest}(account-disruptor)와 같은 결이되, 계좌 엔진까지는
 * 태우지 않고 구독에서 직접 {@link AccountOrderCodec#decode}해 필드만 비교한다.</p>
 */
class AeronAccountOrderSenderIntegrationTest {

    private static final String CHANNEL = "aeron:ipc";
    private static final int STREAM_ID = 4004;
    private static final long TIMEOUT_NANOS = 5_000_000_000L;
    private static final String STOCK_CODE = "005930";

    private final AccountOrderCodec codec = new AccountOrderCodec();

    private MediaDriver driver;
    private Aeron aeron;
    private Subscription subscription;
    private Publication publication;

    @BeforeEach
    void setUp() {
        driver = MediaDriver.launchEmbedded();
        aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()));
        subscription = aeron.addSubscription(CHANNEL, STREAM_ID);
        publication = aeron.addPublication(CHANNEL, STREAM_ID);
        awaitConnected(publication);
    }

    @AfterEach
    void tearDown() {
        publication.close();
        subscription.close();
        aeron.close();
        driver.close();
    }

    @Test
    void 매수_주문을_발신하면_계좌_인테이크_구독에서_그대로_디코딩된다() {
        AeronAccountOrderSender sender = new AeronAccountOrderSender(publication);

        sender.send(OrderSide.BUY, 1L, STOCK_CODE, new BigDecimal("10000"), 10, "req-buy-1");

        DecodedAccountOrder decoded = awaitOne();
        assertThat(decoded).isEqualTo(new DecodedAccountOrder(OrderSide.BUY, 1L, STOCK_CODE, new BigDecimal("10000"), 10, "req-buy-1"));
    }

    @Test
    void 매도_주문을_발신하면_계좌_인테이크_구독에서_그대로_디코딩된다() {
        AeronAccountOrderSender sender = new AeronAccountOrderSender(publication);

        sender.send(OrderSide.SELL, 2L, STOCK_CODE, new BigDecimal("11000"), 5, "req-sell-1");

        DecodedAccountOrder decoded = awaitOne();
        assertThat(decoded).isEqualTo(new DecodedAccountOrder(OrderSide.SELL, 2L, STOCK_CODE, new BigDecimal("11000"), 5, "req-sell-1"));
    }

    private DecodedAccountOrder awaitOne() {
        List<DecodedAccountOrder> received = new ArrayList<>();
        FragmentHandler handler = (buffer, offset, length, header) -> {
            DirectBuffer directBuffer = buffer;
            received.add(codec.decode(directBuffer, offset));
        };

        long deadline = System.nanoTime() + TIMEOUT_NANOS;
        while (received.isEmpty()) {
            subscription.poll(handler, 10);
            if (received.isEmpty() && System.nanoTime() > deadline) {
                throw new AssertionError("5초 안에 주문이 도착하지 않았습니다");
            }
        }
        return received.get(0);
    }

    private void awaitConnected(Publication publication) {
        long deadline = System.nanoTime() + TIMEOUT_NANOS;
        while (!publication.isConnected()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("5초 안에 Publication이 연결되지 않음");
            }
            Thread.yield();
        }
    }
}
