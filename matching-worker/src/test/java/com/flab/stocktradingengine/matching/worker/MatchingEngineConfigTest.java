package com.flab.stocktradingengine.matching.worker;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.ConfigurableApplicationContext;

import com.flab.stocktradingengine.matching.disruptor.MatchListener;
import com.flab.stocktradingengine.matching.disruptor.MatchingEngine;
import com.flab.stocktradingengine.trading.entity.OrderSide;
import com.flab.stocktradingengine.trading.matching.FillResult;

/**
 * 설정된 {@link MatchingEngine} 빈이 컨텍스트 기동과 함께 실제로 도는지, 컨텍스트 종료 시
 * 정상 셧다운되는지 확인한다. account-worker의 AccountEngineConfigTest와 같은 결.
 */
class MatchingEngineConfigTest {

    private static final String STOCK = "005930";

    @Test
    void 컨텍스트가_뜨면_엔진이_실제로_동작하고_종료시_정상_셧다운된다() throws InterruptedException {
        List<FillResult> fills = new CopyOnWriteArrayList<>();
        CountDownLatch latch = new CountDownLatch(1);
        MatchListener recorder = (stockCode, fill) -> {
            fills.add(fill);
            latch.countDown();
        };

        new ApplicationContextRunner()
            .withUserConfiguration(MatchingEngineConfig.class)
            .withBean(MatchListener.class, () -> recorder)
            .run(context -> {
                MatchingEngine engine = context.getBean(MatchingEngine.class);
                Instant now = Instant.now();

                engine.publishPlace(1L, 100L, STOCK, OrderSide.BUY, new BigDecimal("10000"), 10, now);
                engine.publishPlace(2L, 200L, STOCK, OrderSide.SELL, new BigDecimal("10000"), 10, now.plusMillis(1));

                assertThat(latch.await(1, TimeUnit.SECONDS)).as("1초 안에 체결이 도착해야 한다").isTrue();
                assertThat(fills).hasSize(1);

                assertShutsDownCleanly(context);
            });
    }

    private void assertShutsDownCleanly(ConfigurableApplicationContext context) {
        context.close(); // SmartLifecycle.stop() → engine.shutdown() 이 예외 없이 불려야 한다
    }
}
