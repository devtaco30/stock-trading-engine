package com.flab.stocktradingengine.matching;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

/**
 * 매칭 엔진. orders.{stockCode} 토픽을 소비해 인메모리 OrderBook 에서 매칭 후 fills.{stockCode} 발행.
 */
@SpringBootApplication(scanBasePackages = "com.flab.stocktradingengine")
@EntityScan(basePackages = "com.flab.stocktradingengine")
@EnableJpaRepositories(basePackages = "com.flab.stocktradingengine")
public class MatchingApplication {

    public static void main(String[] args) {
        SpringApplication.run(MatchingApplication.class, args);
    }
}
