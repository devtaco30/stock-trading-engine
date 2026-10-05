package com.flab.stocktradingengine.api.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.slf4j.LoggerFactory;

import com.flab.stocktradingengine.aeron.AccountDestinationResolver;
import com.flab.stocktradingengine.api.exception.OrderPublishException;
import com.flab.stocktradingengine.trading.entity.OrderSide;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;

/**
 * v2 api를 「받으면 즉시 202」로 바꾸는 전달 큐와 전용 스레드. HTTP 스레드의 submit은 큐에 넣고 바로 돌아오고,
 * 전용 스레드가 Aeron 발신과 재시도를 맡으며, 끝내 실패한 주문은 Redis 기록과 ERROR 로그로 남는다.
 */
class AccountOrderDispatcherTest {

    private final AccountDestinationResolver resolver = mock(AccountDestinationResolver.class);
    private final AeronAccountOrderSender sender = mock(AeronAccountOrderSender.class);
    private final OrderDeliveryFailureStore store = mock(OrderDeliveryFailureStore.class);
    private final List<ILoggingEvent> logged = new CopyOnWriteArrayList<>();
    private final Logger dispatcherLogger = (Logger) LoggerFactory.getLogger(AccountOrderDispatcher.class);
    private AccountOrderDispatcher dispatcher;
    private AppenderBase<ILoggingEvent> appender;

    @BeforeEach
    void setUp() {
        when(resolver.orderEndpointFor(anyLong())).thenReturn(Optional.of("aeron:ipc"));
        appender = new AppenderBase<>() {
            @Override
            protected void append(ILoggingEvent event) {
                logged.add(event);
            }
        };
        appender.start();
        dispatcherLogger.addAppender(appender);
    }

    @AfterEach
    void tearDown() {
        dispatcherLogger.detachAppender(appender);
        if (dispatcher != null) {
            dispatcher.close();
        }
    }

    private static PendingOrder order(String requestId) {
        return new PendingOrder(OrderSide.BUY, 123L, "005930", new BigDecimal("70000"), 10, requestId, 1_000L);
    }

    private AccountOrderDispatcher dispatcher(int capacity, long deadlineMillis) {
        dispatcher = new AccountOrderDispatcher(resolver, sender, store, capacity, deadlineMillis);
        return dispatcher;
    }

    private List<String> errorLines() {
        return logged.stream().filter(e -> e.getLevel() == Level.ERROR).map(ILoggingEvent::getFormattedMessage).toList();
    }

    @Test
    void submit은_큐에_넣고_발신이_막혀도_바로_돌아온다() throws Exception {
        CountDownLatch neverReleased = new CountDownLatch(1);
        doAnswer(invocation -> {
            neverReleased.await();
            return null;
        }).when(sender).send(any(), anyLong(), anyString(), any(), anyInt(), anyString(), anyLong());
        AccountOrderDispatcher d = dispatcher(100, 2_000);
        d.start();
        try {
            assertTimeoutPreemptively(Duration.ofMillis(500), () -> {
                for (int i = 0; i < 50; i++) {
                    d.submit(order("req-" + i));
                }
            });
        } finally {
            neverReleased.countDown();
        }
    }

    @Test
    void 큐가_가득이면_OrderPublishException() {
        AccountOrderDispatcher d = dispatcher(2, 2_000);      // 시작하지 않아 큐가 비워지지 않는다

        d.submit(order("req-1"));
        d.submit(order("req-2"));

        assertThatThrownBy(() -> d.submit(order("req-3"))).isInstanceOf(OrderPublishException.class);
    }

    @Test
    void 목적지가_없으면_OrderPublishException이고_큐에_넣지_않는다() {
        when(resolver.orderEndpointFor(123L)).thenReturn(Optional.empty());
        AccountOrderDispatcher d = dispatcher(2, 2_000);

        assertThatThrownBy(() -> d.submit(order("req-1"))).isInstanceOf(OrderPublishException.class);
        d.start();

        verify(sender, never()).send(any(), anyLong(), anyString(), any(), anyInt(), anyString(), anyLong());
    }

    @Test
    void 전용_스레드가_꺼내_원래_발신_시각과_함께_sender로_보낸다() {
        List<String> threadNames = new CopyOnWriteArrayList<>();
        doAnswer(invocation -> {
            threadNames.add(Thread.currentThread().getName());
            return null;
        }).when(sender).send(any(), anyLong(), anyString(), any(), anyInt(), anyString(), anyLong());
        AccountOrderDispatcher d = dispatcher(100, 2_000);
        d.start();

        d.submit(order("req-1"));

        verify(sender, timeout(2_000)).send(
            eq(OrderSide.BUY), eq(123L), eq("005930"), eq(new BigDecimal("70000")), eq(10), eq("req-1"), eq(1_000L));
        assertThat(threadNames).containsExactly("order-dispatcher");
    }

    @Test
    void 발신이_일시적으로_실패하면_마감까지_재시도하고_성공하면_기록하지_않는다() {
        AtomicInteger calls = new AtomicInteger();
        doAnswer(invocation -> {
            if (calls.incrementAndGet() <= 2) {
                throw new OrderPublishException("back pressured");
            }
            return null;
        }).when(sender).send(any(), anyLong(), anyString(), any(), anyInt(), anyString(), anyLong());
        AccountOrderDispatcher d = dispatcher(100, 2_000);
        d.start();

        d.submit(order("req-1"));

        verify(sender, timeout(2_000).times(3)).send(any(), anyLong(), anyString(), any(), anyInt(), anyString(), anyLong());
        verify(store, never()).put(any(), anyString(), anyInt(), anyLong());
        assertThat(errorLines()).isEmpty();
    }

    @Test
    void 마감을_넘기면_Redis_기록과_ERROR_로그에_주문_전_필드와_시도_횟수와_사유를_남긴다() {
        doThrow(new OrderPublishException("offer 결과=-2")).when(sender)
            .send(any(), anyLong(), anyString(), any(), anyInt(), anyString(), anyLong());
        AccountOrderDispatcher d = dispatcher(100, 60);
        d.start();

        d.submit(order("req-fail"));

        ArgumentCaptor<Integer> attempts = ArgumentCaptor.forClass(Integer.class);
        ArgumentCaptor<String> reason = ArgumentCaptor.forClass(String.class);
        verify(store, timeout(2_000)).put(eq(order("req-fail")), reason.capture(), attempts.capture(), anyLong());
        assertThat(attempts.getValue()).isGreaterThanOrEqualTo(2);
        assertThat(reason.getValue()).contains("offer 결과=-2");
        assertThat(errorLines()).anySatisfy(line -> assertThat(line)
            .contains("req-fail", "123", "005930", "BUY", "70000", "10", "시도=" + attempts.getValue(), "offer 결과=-2"));
    }

    @Test
    void 기록_저장소가_예외를_던져도_스레드는_계속_돌고_ERROR가_남는다() {
        AtomicInteger calls = new AtomicInteger();
        doAnswer(invocation -> {
            if ("req-fail".equals(invocation.getArgument(5))) {
                throw new OrderPublishException("실패");
            }
            calls.incrementAndGet();
            return null;
        }).when(sender).send(any(), anyLong(), anyString(), any(), anyInt(), anyString(), anyLong());
        doThrow(new IllegalStateException("redis down")).when(store).put(any(), anyString(), anyInt(), anyLong());
        AccountOrderDispatcher d = dispatcher(100, 30);
        d.start();

        d.submit(order("req-fail"));
        d.submit(order("req-ok"));

        verify(sender, timeout(2_000)).send(any(), anyLong(), anyString(), any(), anyInt(), eq("req-ok"), anyLong());
        assertThat(errorLines()).anySatisfy(line -> assertThat(line).contains("req-fail"));
        assertThat(errorLines()).anySatisfy(line -> assertThat(line).contains("redis down"));
    }

    @Test
    void deliver에서_예상하지_못한_예외가_나도_ERROR를_남기고_스레드가_계속_돈다() {
        doAnswer(invocation -> {
            if ("req-bad".equals(invocation.getArgument(5))) {
                throw new IllegalStateException("boom");
            }
            return null;
        }).when(sender).send(any(), anyLong(), anyString(), any(), anyInt(), anyString(), anyLong());
        AccountOrderDispatcher d = dispatcher(100, 2_000);
        d.start();

        d.submit(order("req-bad"));
        d.submit(order("req-next"));

        verify(sender, timeout(2_000)).send(any(), anyLong(), anyString(), any(), anyInt(), eq("req-next"), anyLong());
        verify(store, timeout(2_000)).put(eq(order("req-bad")), org.mockito.ArgumentMatchers.contains("IllegalStateException"), anyInt(), anyLong());
    }

    @Test
    void 종료할_때_남은_주문을_기록하고_ERROR로_남긴다() {
        AccountOrderDispatcher d = dispatcher(100, 2_000);      // 시작하지 않아 큐에 남는다
        d.submit(order("req-1"));
        d.submit(order("req-2"));

        d.close();

        verify(store).put(eq(order("req-1")), org.mockito.ArgumentMatchers.contains("종료 시 미전달"), eq(0), anyLong());
        verify(store).put(eq(order("req-2")), org.mockito.ArgumentMatchers.contains("종료 시 미전달"), eq(0), anyLong());
        assertThat(errorLines()).hasSize(2);
    }

    @Test
    void 종료_뒤에는_submit을_받지_않는다() {
        AccountOrderDispatcher d = dispatcher(100, 2_000);
        d.close();

        assertThatThrownBy(() -> d.submit(order("req-1"))).isInstanceOf(OrderPublishException.class);
    }

    @Test
    void 주입한_쉬는_방식을_전달_스레드가_큐가_비었을_때_부른다() throws InterruptedException {
        AtomicInteger idleCalls = new AtomicInteger();
        org.agrona.concurrent.IdleStrategy counting = new org.agrona.concurrent.IdleStrategy() {
            @Override
            public void idle(int workCount) {
                idleCalls.incrementAndGet();
            }

            @Override
            public void idle() {
                idleCalls.incrementAndGet();
            }

            @Override
            public void reset() {
            }
        };
        dispatcher = new AccountOrderDispatcher(resolver, sender, store, 8, 1_000L, counting);

        dispatcher.start();
        long deadline = System.nanoTime() + 2_000_000_000L;
        while (idleCalls.get() == 0 && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }

        assertThat(idleCalls.get()).as("전달 스레드가 주입한 전략의 idle을 불러야 한다").isPositive();
    }

    @Test
    void 주입이_없으면_지금과_같은_기본_Backoff를_쓴다() throws Exception {
        org.junit.jupiter.api.Assumptions.assumeTrue(System.getenv(com.flab.stocktradingengine.support.IdleStrategies.ENV_NAME) == null);
        AccountOrderDispatcher plain = new AccountOrderDispatcher(resolver, sender, store, 8, 1_000L);
        java.lang.reflect.Field field = AccountOrderDispatcher.class.getDeclaredField("idleStrategy");
        field.setAccessible(true);

        assertThat(field.get(plain).getClass()).isEqualTo(org.agrona.concurrent.BackoffIdleStrategy.class);
    }
}
