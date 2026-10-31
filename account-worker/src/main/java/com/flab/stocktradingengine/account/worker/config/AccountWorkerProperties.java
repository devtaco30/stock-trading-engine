package com.flab.stocktradingengine.account.worker.config;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 이 인스턴스(샤드)가 소유하는 계좌 목록. 인스턴스마다 다른 accountId 부분집합을 시드해야
 * (ADR-018) 여러 컨테이너로 나눠 뜰 수 있다 — 설정값으로 빼서 인스턴스별로 다르게 준다.
 *
 * @param maxOpenOrdersPerAccount 계좌 하나의 미체결 주문 수(매수 예약 + 매도 예약) 상한. 이 수에 이른 계좌의
 *     새 주문은 거부된다. 필수 설정이다 — 코드에 기본값을 두면 설정을 빼먹은 워커가 상한 없이 뜬다.
 */
@ConfigurationProperties(prefix = "account-worker")
public record AccountWorkerProperties(List<SeedAccount> seedAccounts, Integer maxOpenOrdersPerAccount) {

    /**
     * fork1, Unit 3a — {@code account-worker.seed-accounts}가 아예 안 주어지면(seedless 기동)
     * 바인더가 null을 준다. {@code AccountEngineConfig#accountEngine}이 이 값을 바로 for-each 해
     * null이면 기동 자체가 NPE로 죽는다 — 빈 리스트로 정규화해 seedless 기동도 안전하게 한다.
     */
    public AccountWorkerProperties {
        seedAccounts = seedAccounts == null ? List.of() : seedAccounts;
        if (maxOpenOrdersPerAccount == null) {
            throw new IllegalStateException("account-worker.max-open-orders-per-account는 필수 설정입니다(1 이상). 값을 주지 않으면 계좌당 미체결 상한이 없는 채로 뜹니다.");
        }
        if (maxOpenOrdersPerAccount < 1) {
            throw new IllegalArgumentException("account-worker.max-open-orders-per-account는 1 이상이어야 합니다: " + maxOpenOrdersPerAccount);
        }
    }

    public record SeedAccount(long accountId, BigDecimal balance, BigDecimal marginRate, Map<String, Integer> holdings) {
    }
}
