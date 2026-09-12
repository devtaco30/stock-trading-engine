package com.flab.stocktradingengine.account.worker;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * v2 계좌 워커 앱. {@code account-disruptor}(프레임워크 0 라이브러리)를 얹어 실행하고,
 * {@code account-fills} 토픽을 소비해 계좌 상태에 체결을 반영한다. DB 없음(ADR-018).
 */
@SpringBootApplication(scanBasePackages = "com.flab.stocktradingengine.account.worker")
public class AccountWorkerApplication {

    public static void main(String[] args) {
        SpringApplication.run(AccountWorkerApplication.class, args);
    }
}
