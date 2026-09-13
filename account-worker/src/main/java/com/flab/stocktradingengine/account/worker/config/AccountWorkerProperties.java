package com.flab.stocktradingengine.account.worker.config;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 이 인스턴스(샤드)가 소유하는 계좌 목록. 인스턴스마다 다른 accountId 부분집합을 시드해야
 * (ADR-018) 여러 컨테이너로 나눠 뜰 수 있다 — 설정값으로 빼서 인스턴스별로 다르게 준다.
 */
@ConfigurationProperties(prefix = "account-worker")
public record AccountWorkerProperties(List<SeedAccount> seedAccounts) {

    public record SeedAccount(long accountId, BigDecimal balance, BigDecimal marginRate, Map<String, Integer> holdings) {
    }
}
