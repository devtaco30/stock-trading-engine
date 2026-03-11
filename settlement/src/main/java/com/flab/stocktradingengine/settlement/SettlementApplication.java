package com.flab.stocktradingengine.settlement;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.context.annotation.Profile;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

/**
 * Settlement 워커 애플리케이션.
 *
 * <p>Phase 2: 별도 JAR 로 분리해 독립 프로세스로 실행.</p>
 * <p>{@code --spring.profiles.active=settlement} 으로 기동한다.</p>
 */
@Profile("settlement")
@SpringBootApplication(scanBasePackages = "com.flab.stocktradingengine")
@EntityScan("com.flab.stocktradingengine")
@EnableJpaRepositories("com.flab.stocktradingengine")
public class SettlementApplication {

    public static void main(String[] args) {
        SpringApplication.run(SettlementApplication.class, args);
    }
}
