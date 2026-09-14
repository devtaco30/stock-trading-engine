package com.flab.stocktradingengine.account.worker.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.nio.ByteBuffer;

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;

import com.flab.stocktradingengine.account.disruptor.io.AccountFillReceiver;
import com.flab.stocktradingengine.account.worker.AccountWorkerApplication;
import com.flab.stocktradingengine.aeron.AeronStreamIds;
import com.flab.stocktradingengine.codec.FillCodec;
import com.flab.stocktradingengine.codec.FilledTrade;

import io.aeron.Aeron;
import io.aeron.Publication;

/**
 * account-worker 앱을 실제로 띄우고, 체결을 Aeron으로 발행하면 {@link AccountFillReceiver}가
 * 소비한 스트림 position이 실제로 늘어나는지 확인하는 end-to-end 테스트(ADR-032, U4a). 재기동 시
 * 이 값부터 fill 스트림을 replay하는 배선은 U4b 몫이라, 여기서는 "소비할수록 position이 커진다"
 * 까지만 증명한다.
 *
 * <p>{@code @DirtiesContext} — 이 컨텍스트가 만든 임베디드 MediaDriver를 다음 테스트와 안 겹치게 한다.</p>
 */
@SpringBootTest(
    classes = AccountWorkerApplication.class,
    properties = {
        "account-worker.seed-accounts[0].account-id=1",
        "account-worker.seed-accounts[0].balance=1000000",
        "account-worker.seed-accounts[0].margin-rate=0.40"
    }
)
@DirtiesContext
class AccountFillReceiverPositionIntegrationTest {

    private static final String STOCK = "005930";
    private static final long TIMEOUT_NANOS = 5_000_000_000L;

    private final FillCodec codec = new FillCodec();

    @Autowired
    private Aeron aeron;

    @Autowired
    private AccountFillReceiver accountFillReceiver;

    @Test
    void 체결을_소비할수록_consumedPosition이_늘어난다() {
        long positionBefore = accountFillReceiver.consumedPosition();

        Publication publication = aeron.addPublication(AccountFillIntakeConfig.DEFAULT_FILL_CHANNEL, AeronStreamIds.FILL);
        try {
            awaitConnected(publication);
            FilledTrade trade = new FilledTrade(9001L, STOCK, 1L, 1L, 2L, 2L, 4, new BigDecimal("10000"));
            send(publication, trade);

            awaitPositionAdvance(positionBefore);
        } finally {
            publication.close();
        }
    }

    /** 체결을 인코딩해 Aeron으로 발행한다. 백프레셔면 받아들여질 때까지 재시도한다. */
    private void send(Publication publication, FilledTrade trade) {
        UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(256));
        int length = codec.encode(buffer, 0, trade);

        long deadline = System.nanoTime() + TIMEOUT_NANOS;
        long result;
        do {
            result = publication.offer(buffer, 0, length);
            if (result > 0) {
                return;
            }
            if (System.nanoTime() > deadline) {
                throw new AssertionError("5초 안에 체결 발행 실패 — offer 반환=" + result);
            }
            Thread.yield();
        } while (true);
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

    private void awaitPositionAdvance(long positionBefore) {
        long deadline = System.nanoTime() + TIMEOUT_NANOS;
        while (accountFillReceiver.consumedPosition() <= positionBefore) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("5초 안에 consumedPosition이 늘지 않음 — 체결이 소비되지 않은 것으로 보임");
            }
            Thread.yield();
        }
        assertThat(accountFillReceiver.consumedPosition()).isGreaterThan(positionBefore);
    }
}
