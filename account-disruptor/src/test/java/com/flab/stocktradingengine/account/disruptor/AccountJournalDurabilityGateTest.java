package com.flab.stocktradingengine.account.disruptor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.dsl.ProducerType;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.flab.stocktradingengine.codec.AccountJournalEntry;

/**
 * A안(계좌 유실 창 막기) — {@link AccountEngine#blockUntilJournaled}가 저널 기록이 끝날 때까지
 * 호출 스레드를 동기로 붙잡는지 검증한다.
 *
 * <p>배경: Kafka로 온 체결·정산을 링에 발행하자마자 컨슈머가 ack를 보내면, 저널러가 그 이벤트를
 * durable하게 기록하기 전에 Kafka 오프셋이 넘어간다. 그 사이 크래시하면 저널에도 Kafka에도 없어
 * 유실된다. 이를 막으려면 컨슈머가 "저널 기록 완료"를 확인한 뒤에 ack해야 하고, 그 대기 수단이
 * {@code blockUntilJournaled}다.</p>
 *
 * <p>검증 방법: append에 인위적 지연을 준 저널을 끼운다. 발행 직후엔 아직 기록 전이라 저널이
 * 비어 있고, {@code blockUntilJournaled} 뒤에는 기록돼 있어야 한다 — 이 메서드가 실제로 기다린다는
 * 증거다(단순히 저널이 빨라서 통과하는 게 아니다).</p>
 */
class AccountJournalDurabilityGateTest {

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

    @Test
    @DisplayName("blockUntilJournaled는 저널 기록이 끝날 때까지 호출 스레드를 붙잡는다")
    void blockUntilJournaled_는_저널기록을_기다린다() {
        DelayingAccountJournal slowJournal = new DelayingAccountJournal(new InMemoryAccountJournal());
        engine = new AccountEngine(BUFFER_SIZE, new BlockingWaitStrategy(), ProducerType.SINGLE, 0L,
            NO_OP_SENDER, NoOpAccountResultListener.INSTANCE, slowJournal);
        engine.seed(1L, new BigDecimal("1000000"), new BigDecimal("0.40"));
        engine.start();

        long sequence = engine.publishBuyFill(9001L, 1L, 1L, STOCK, new BigDecimal("10000"), 10);

        // 발행 직후: 저널러가 append 지연 중이라 아직 기록 전 — 비어 있어야 한다.
        assertTrue(engine.journal().entries().isEmpty(),
            "blockUntilJournaled 전에는 아직 저널에 기록되지 않았어야 한다");

        engine.blockUntilJournaled(sequence);

        // 대기 뒤: 그 체결이 저널에 있어야 한다.
        List<AccountJournalEntry> entries = engine.journal().entries();
        assertEquals(1, entries.size(), "blockUntilJournaled 뒤에는 그 체결이 저널에 기록돼 있어야 한다");
        assertEquals(9001L, entries.get(0).tradeId());
    }

    @Test
    @DisplayName("저널이 제때 기록 못 하면 blockUntilJournaled는 JournalUnavailableException을 던진다")
    void 타임아웃시_전용예외를_던진다() {
        CountDownLatch release = new CountDownLatch(1);
        AccountJournal blockingJournal = new AccountJournal() {
            @Override
            public void append(AccountJournalEntry entry) {
                try {
                    release.await(); // 풀어줄 때까지 기록을 막는다 → 시퀀스가 안 올라감
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }

            @Override
            public List<AccountJournalEntry> entries() {
                return List.of();
            }

            @Override
            public long position() {
                return 0L;
            }
        };
        engine = new AccountEngine(BUFFER_SIZE, new BlockingWaitStrategy(), ProducerType.SINGLE, 0L,
            NO_OP_SENDER, NoOpAccountResultListener.INSTANCE, blockingJournal);
        // 계좌를 시드하지 않는다 — release 후 처리될 때 예약검증(fail-fast)이 아니라 ACCOUNT_NOT_FOUND
        // (정상 거절)로 빠져 teardown이 깨끗하게 끝나게 한다.
        engine.start();

        long sequence = engine.publishBuyFill(9001L, 1L, 999L, STOCK, new BigDecimal("10000"), 10);
        try {
            // 저널이 막혀 시퀀스가 안 올라가므로 50ms 안에 도달 못 해 예외를 던져야 한다(전용 타입).
            assertThrows(JournalUnavailableException.class, () -> engine.blockUntilJournaled(sequence, 50L));
        } finally {
            release.countDown(); // 저널 스레드를 풀어 shutdown(teardown)이 멈추지 않게 한다
        }
    }

    /** append 를 일부러 늦추는 저널 — 대기하지 않으면 아직 비어 있음이 드러난다. */
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

        @Override
        public long position() {
            return delegate.position();
        }
    }
}
