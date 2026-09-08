package com.flab.stocktradingengine.settlement.worker;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * v2 정산 워커 앱. {@code settlement-requests} 토픽을 소비해 미수금을 DB에 저장한다(a2-2).
 * T+2 스캔·되돌림 발행은 a2-3.
 */
@SpringBootApplication(scanBasePackages = "com.flab.stocktradingengine.settlement.worker")
public class SettlementWorkerApplication {

    public static void main(String[] args) {
        SpringApplication.run(SettlementWorkerApplication.class, args);
    }
}
