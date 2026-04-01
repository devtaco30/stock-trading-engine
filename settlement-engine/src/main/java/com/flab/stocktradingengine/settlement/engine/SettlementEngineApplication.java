package com.flab.stocktradingengine.settlement.engine;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

/**
 * 정산 엔진. fills.{stockCode} 토픽을 소비해 DB에 체결 결과를 반영.
 */
@SpringBootApplication(scanBasePackages = "com.flab.stocktradingengine")
@EntityScan(basePackages = "com.flab.stocktradingengine")
@EnableJpaRepositories(basePackages = "com.flab.stocktradingengine")
public class SettlementEngineApplication {

    public static void main(String[] args) {
        SpringApplication.run(SettlementEngineApplication.class, args);
    }
}
