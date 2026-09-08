package com.flab.stocktradingengine.settlement.worker;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * v2 정산 워커 앱. {@code settlement-requests} 토픽을 소비해 미수금을 DB에 저장하고(a2-2),
 * T+2 만기가 지난 건을 스캔해 {@code account-settlements}로 되돌림을 발행한다(a2-3).
 */
@EnableScheduling
@SpringBootApplication(scanBasePackages = "com.flab.stocktradingengine.settlement.worker")
public class SettlementWorkerApplication {

    public static void main(String[] args) {
        SpringApplication.run(SettlementWorkerApplication.class, args);
    }
}
