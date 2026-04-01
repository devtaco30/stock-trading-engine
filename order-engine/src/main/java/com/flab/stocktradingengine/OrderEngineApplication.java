package com.flab.stocktradingengine;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

/**
 * 주문 접수 엔진. order-requests 토픽을 소비해 DB에 주문을 저장하고 orders.{stockCode} 로 발행.
 */
@SpringBootApplication(scanBasePackages = "com.flab.stocktradingengine")
@EntityScan(basePackages = "com.flab.stocktradingengine")
@EnableJpaRepositories(basePackages = "com.flab.stocktradingengine")
public class OrderEngineApplication {

    public static void main(String[] args) {
        SpringApplication.run(OrderEngineApplication.class, args);
    }
}
