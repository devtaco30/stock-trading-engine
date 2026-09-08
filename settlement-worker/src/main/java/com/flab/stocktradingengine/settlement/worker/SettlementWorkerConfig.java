package com.flab.stocktradingengine.settlement.worker;

import java.time.Clock;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SettlementWorkerConfig {

    /** T+2 만기 판정에 쓰는 시각 소스. 테스트에서 고정 시각으로 교체할 수 있도록 빈으로 분리한다. */
    @Bean
    public Clock clock() {
        return Clock.systemDefaultZone();
    }
}
