package com.flab.stocktradingengine.account.worker.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

/**
 * fork1, Unit 3a — {@code account-worker.seed-accounts}가 아예 안 주어졌을 때(seedless 기동,
 * 3-JVM udp 단독 기동 검증에서 실제로 겪은 경우) {@link AccountWorkerProperties#seedAccounts()}가
 * null이 아니라 빈 리스트를 돌려주는지 확인한다 — {@code AccountEngineConfig#accountEngine}이
 * 이 값을 바로 for-each 하기 때문에 null이면 NPE로 기동 자체가 죽는다.
 *
 * <p>계좌 미체결 상한({@code account-worker.max-open-orders-per-account})은 필수 설정이다 —
 * 코드에 기본값을 두면 설정을 빼먹은 워커가 상한 없이 조용히 떠서 상한을 두는 이유가 사라진다.</p>
 */
class AccountWorkerPropertiesTest {

    @Test
    void seedAccounts가_null로_주어지면_빈_리스트로_정규화된다() {
        AccountWorkerProperties properties = new AccountWorkerProperties(null, 100);

        assertThat(properties.seedAccounts()).isEmpty();
    }

    @Test
    void 계좌_미체결_상한이_주어지지_않으면_기동을_거부한다() {
        assertThatThrownBy(() -> new AccountWorkerProperties(null, null))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("account-worker.max-open-orders-per-account");
    }

    @Test
    void 계좌_미체결_상한이_1_미만이면_기동을_거부한다() {
        assertThatThrownBy(() -> new AccountWorkerProperties(null, 0))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("account-worker.max-open-orders-per-account");
        assertThatThrownBy(() -> new AccountWorkerProperties(null, -1))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("account-worker.max-open-orders-per-account");
    }

    @Test
    void 계좌_미체결_상한이_1_이상이면_그대로_읽힌다() {
        AccountWorkerProperties properties = new AccountWorkerProperties(null, 1);

        assertThat(properties.maxOpenOrdersPerAccount()).isEqualTo(1);
    }
}
