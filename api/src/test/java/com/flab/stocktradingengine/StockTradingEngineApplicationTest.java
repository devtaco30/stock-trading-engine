package com.flab.stocktradingengine;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * 애플리케이션 컨텍스트 로드 검증.
 * H2 in-memory DB, Kafka auto-startup 비활성화(실제 브로커 없이 빈 생성만 검증).
 * SQL 초기화 비활성화(data-scenario1-test.sql 불필요).
 */
@SpringBootTest(classes = StockTradingEngineApplication.class,
    properties = {
        "spring.sql.init.mode=never",
        "spring.kafka.listener.auto-startup=false"
    }
)
public class StockTradingEngineApplicationTest {

    @Test
    void contextLoads() {}
}
