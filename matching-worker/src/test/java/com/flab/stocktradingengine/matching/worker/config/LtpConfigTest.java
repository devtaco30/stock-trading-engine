package com.flab.stocktradingengine.matching.worker.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.time.Instant;

import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import com.flab.stocktradingengine.matching.disruptor.io.MatchListener;
import com.flab.stocktradingengine.matching.worker.ltp.OrderbookRecordingMatchListener;
import com.flab.stocktradingengine.matching.worker.messaging.AccountFillPublisher;
import com.flab.stocktradingengine.trading.entity.OrderSide;
import com.flab.stocktradingengine.trading.matching.FillResult;
import com.flab.stocktradingengine.trading.matching.OrderBook;
import com.flab.stocktradingengine.trading.matching.OrderEntry;

/**
 * 매칭 엔진에 주입되는 최종 listener가 「계좌 발신 → LTP 기록 → 호가창 기록」 순서로 감싼 것이고, 체결 한 건과 호가창 변경 한 건이
 * 각각 Redis의 {@code ltp:} 키와 {@code orderbook:} 키에 기록되는지 확인한다.
 */
class LtpConfigTest {

    @Test
    @SuppressWarnings("unchecked")
    void 최종_listener는_발신_LTP_호가창_순서로_감싸고_둘_다_Redis에_기록한다() {
        StringRedisTemplate template = mock(StringRedisTemplate.class);
        ValueOperations<String, String> ops = mock(ValueOperations.class);
        when(template.opsForValue()).thenReturn(ops);
        AccountFillPublisher publisher = mock(AccountFillPublisher.class);

        try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
            context.registerBean(StringRedisTemplate.class, () -> template);
            context.registerBean(AccountFillPublisher.class, () -> publisher);
            context.register(LtpConfig.class);
            System.setProperty("matching.worker.ltp.flush-interval-millis", "10");
            System.setProperty("matching.worker.orderbook.flush-interval-millis", "10");
            try {
                context.refresh();
            } finally {
                System.clearProperty("matching.worker.ltp.flush-interval-millis");
                System.clearProperty("matching.worker.orderbook.flush-interval-millis");
            }

            MatchListener listener = context.getBean(MatchListener.class);
            assertThat(listener).isInstanceOf(OrderbookRecordingMatchListener.class);

            OrderBook book = new OrderBook();
            book.addOrder(new OrderEntry(1L, 1L, "A1", OrderSide.BUY, new BigDecimal("100"), 5, Instant.now()));
            listener.onFill("A1", new FillResult(1L, 10L, 2L, 20L, 1, new BigDecimal("24050")));
            listener.onBookChanged("A1", book);

            verify(publisher).onFill(anyString(), org.mockito.ArgumentMatchers.any(FillResult.class));
            verify(ops, timeout(2_000)).set("ltp:A1", "24050");
            verify(ops, timeout(2_000)).set("orderbook:A1", "{\"bids\":[{\"price\":100,\"quantity\":5}],\"asks\":[]}");
        }
    }
}
