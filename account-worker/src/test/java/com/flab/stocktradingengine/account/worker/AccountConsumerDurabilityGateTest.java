package com.flab.stocktradingengine.account.worker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.math.BigDecimal;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.dsl.ProducerType;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.Acknowledgment;

import com.flab.stocktradingengine.account.disruptor.AccountEngine;
import com.flab.stocktradingengine.account.disruptor.AccountJournal;
import com.flab.stocktradingengine.account.disruptor.AccountResultListener;
import com.flab.stocktradingengine.account.disruptor.InMemoryAccountJournal;
import com.flab.stocktradingengine.account.disruptor.MatchingOrderSender;
import com.flab.stocktradingengine.codec.AccountJournalEntry;
import com.flab.stocktradingengine.kafka.event.SettlementResultEvent;
import com.flab.stocktradingengine.kafka.event.TradeFilledEvent;

/**
 * A안(계좌 유실 창 막기) — Kafka 컨슈머가 체결·정산을 링에 발행한 뒤, <b>저널에 기록된 걸
 * 확인한 뒤에만 ack</b> 하는지 검증한다.
 *
 * <p>배경: 발행은 링에 넣기만 하는 비동기라, 저널러가 그 이벤트를 durable하게 기록하기 전에
 * ack가 나가면 Kafka 오프셋이 먼저 넘어간다 — 그 사이 크래시하면 저널에도 Kafka에도 없어 유실.
 * 그래서 컨슈머는 {@code blockUntilJournaled}로 기록 완료를 확인한 뒤 ack해야 한다.</p>
 *
 * <p>검증: append에 지연을 준 저널을 끼운다. ack 시점에 저널 크기를 캡처해, 그 시점에 이미
 * 이벤트가 기록돼 있음을 확인한다 — ack가 저널보다 먼저 나가는 옛 코드였다면 이 시점 저널은
 * 아직 비어 있어 실패한다.</p>
 */
class AccountConsumerDurabilityGateTest {

    private static final String STOCK = "005930";
    private static final int BUFFER_SIZE = 1024;
    private static final long APPEND_DELAY_MILLIS = 100L;
    private static final MatchingOrderSender NO_OP_SENDER = (orderId, accountId, stockCode, side, price, quantity) -> {};

    private AccountEngine engine;

    @AfterEach
    void tearDown() {
        if (engine != null) {
            engine.shutdown();
        }
    }

    private void startEngineWithDelayingJournal() {
        AccountJournal slowJournal = new DelayingAccountJournal(new InMemoryAccountJournal());
        engine = new AccountEngine(BUFFER_SIZE, new BlockingWaitStrategy(), ProducerType.SINGLE, 0L,
            NO_OP_SENDER, mock(AccountResultListener.class), slowJournal);
        engine.start();
    }

    @Test
    @DisplayName("체결 컨슈머는 매수·매도 체결이 저널에 기록된 뒤에 ack 한다")
    void 체결_ack는_저널기록_뒤에() {
        startEngineWithDelayingJournal();
        AtomicInteger journalSizeAtAck = new AtomicInteger(-1);
        Acknowledgment ack = mock(Acknowledgment.class);
        doAnswer(inv -> {
            journalSizeAtAck.set(engine.journal().entries().size());
            return null;
        }).when(ack).acknowledge();

        AccountFillConsumer consumer = new AccountFillConsumer(engine);
        // (tradeId, stockCode, buyOrderId, buyAccountId, sellOrderId, sellAccountId, filledQuantity, matchPrice)
        TradeFilledEvent fill = new TradeFilledEvent(9001L, STOCK, 1L, 1L, 2L, 2L, 4, new BigDecimal("10000"));

        consumer.consume(fill, ack);

        verify(ack).acknowledge();
        assertEquals(2, journalSizeAtAck.get(), "ack 시점엔 매수·매도 체결 둘 다 이미 저널에 있어야 한다");
    }

    @Test
    @DisplayName("정산 컨슈머는 정산이 저널에 기록된 뒤에 ack 한다")
    void 정산_ack는_저널기록_뒤에() {
        startEngineWithDelayingJournal();
        AtomicInteger journalSizeAtAck = new AtomicInteger(-1);
        Acknowledgment ack = mock(Acknowledgment.class);
        doAnswer(inv -> {
            journalSizeAtAck.set(engine.journal().entries().size());
            return null;
        }).when(ack).acknowledge();

        AccountSettlementConsumer consumer = new AccountSettlementConsumer(engine);
        SettlementResultEvent settlement = new SettlementResultEvent(7001L, 1L, new BigDecimal("1000"));

        consumer.consume(settlement, ack);

        verify(ack).acknowledge();
        assertEquals(1, journalSizeAtAck.get(), "ack 시점엔 정산이 이미 저널에 있어야 한다");
    }

    /** append 를 일부러 늦추는 저널 — 대기하지 않으면 ack 시점에 아직 비어 있음이 드러난다. */
    private static final class DelayingAccountJournal implements AccountJournal {
        private final AccountJournal delegate;

        DelayingAccountJournal(AccountJournal delegate) {
            this.delegate = delegate;
        }

        @Override
        public void append(AccountJournalEntry entry) {
            try {
                Thread.sleep(APPEND_DELAY_MILLIS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            delegate.append(entry);
        }

        @Override
        public List<AccountJournalEntry> entries() {
            return delegate.entries();
        }
    }
}
