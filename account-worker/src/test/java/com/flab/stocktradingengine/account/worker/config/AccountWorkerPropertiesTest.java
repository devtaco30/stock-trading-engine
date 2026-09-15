package com.flab.stocktradingengine.account.worker.config;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * fork1, Unit 3a — {@code account-worker.seed-accounts}가 아예 안 주어졌을 때(seedless 기동,
 * 3-JVM udp 단독 기동 검증에서 실제로 겪은 경우) {@link AccountWorkerProperties#seedAccounts()}가
 * null이 아니라 빈 리스트를 돌려주는지 확인한다 — {@code AccountEngineConfig#accountEngine}이
 * 이 값을 바로 for-each 하기 때문에 null이면 NPE로 기동 자체가 죽는다.
 */
class AccountWorkerPropertiesTest {

    @Test
    void seedAccounts가_null로_주어지면_빈_리스트로_정규화된다() {
        AccountWorkerProperties properties = new AccountWorkerProperties(null);

        assertThat(properties.seedAccounts()).isEmpty();
    }
}
