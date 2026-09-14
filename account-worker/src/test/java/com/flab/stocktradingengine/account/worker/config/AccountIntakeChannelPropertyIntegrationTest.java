package com.flab.stocktradingengine.account.worker.config;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;

import com.flab.stocktradingengine.account.worker.AccountWorkerApplication;

import io.aeron.Subscription;

/**
 * fork1, Unit 1 — 주문 인테이크 채널이 하드코딩 상수가 아니라 {@code transport.account-intake.channel}
 * 속성에서 해석되는지 확인한다. 기본값과 다른 채널을 property로 주입하고, 실제로 열린
 * {@link Subscription#channel()}이 그 값과 같은지로 검증한다(3-JVM udp 전환은 Unit 2, 여기선
 * 값이 config에서 나오는지만 본다).
 *
 * <p>{@code @DirtiesContext} — 이 컨텍스트가 만든 임베디드 MediaDriver를 다음 테스트와 안 겹치게 한다.</p>
 */
@SpringBootTest(
    classes = AccountWorkerApplication.class,
    properties = {
        "account-worker.seed-accounts[0].account-id=1",
        "account-worker.seed-accounts[0].balance=1000000",
        "account-worker.seed-accounts[0].margin-rate=0.40",
        "transport.account-intake.channel=aeron:udp?endpoint=localhost:24004"
    }
)
@DirtiesContext
class AccountIntakeChannelPropertyIntegrationTest {

    @Autowired
    @Qualifier("accountOrderSubscription")
    private Subscription accountOrderSubscription;

    @Test
    void 인테이크_채널을_property로_주면_그_값으로_구독이_열린다() {
        assertThat(accountOrderSubscription.channel()).isEqualTo("aeron:udp?endpoint=localhost:24004");
    }
}
