package com.flab.stocktradingengine.api;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

@SpringBootApplication(scanBasePackages = "com.flab.stocktradingengine")
@EntityScan(basePackages = "com.flab.stocktradingengine")
@EnableJpaRepositories(basePackages = "com.flab.stocktradingengine")
public class StockTradingEngineApplication {

	public static void main(String[] args) {
		SpringApplication.run(StockTradingEngineApplication.class, args);
	}

}
