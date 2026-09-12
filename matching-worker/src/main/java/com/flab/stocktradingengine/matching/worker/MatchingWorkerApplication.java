package com.flab.stocktradingengine.matching.worker;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * v2 매칭 워커 앱. {@code matching-disruptor}(프레임워크 0 라이브러리)를 얹어 실행하고,
 * 체결을 {@code account-fills} 토픽으로 발행한다. DB 없음(ADR-018과 같은 결).
 */
@SpringBootApplication(scanBasePackages = "com.flab.stocktradingengine.matching.worker")
public class MatchingWorkerApplication {

    public static void main(String[] args) {
        SpringApplication.run(MatchingWorkerApplication.class, args);
    }
}
