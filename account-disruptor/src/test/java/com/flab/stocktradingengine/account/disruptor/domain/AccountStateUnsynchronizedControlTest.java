package com.flab.stocktradingengine.account.disruptor.domain;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * 대조 장치(A3-ctl). 처리 스레드를 하나로 만드는 장치(single-writer) 없이 {@link AccountState}에
 * 스레드 여러 개가 {@code tryReserve}를 직접 동시에 부르면 잔고를 넘는 통과가 실제로 관측되는지 센다.
 * 같은 주문 60건·잔고 1,000,000·주문 100,000을 쓰는 A3-a가 엔진의 단일 처리 스레드 덕에 통과하는지,
 * 아니면 동시성을 못 만들어서 통과하는지 가르는 용도다.
 *
 * <p>기대 결과가 "위반이 관측됨"이라 단언으로 CI 통과 기준을 삼을 수 없다. 관측 횟수는 단언 없이 출력으로만
 * 남기고, 그래서 테스트 자체는 항상 통과한다.</p>
 */
@Tag("evidence")
class AccountStateUnsynchronizedControlTest {

    private static final int ORDER_COUNT = 60;
    private static final int THREADS = 8;
    private static final int ROUNDS = 200;
    private static final long ACCOUNT_ID = 1L;
    private static final BigDecimal INITIAL_BALANCE = new BigDecimal("1000000");
    private static final BigDecimal MARGIN_RATE = new BigDecimal("1.00");
    private static final BigDecimal ORDER_PRICE = new BigDecimal("10000");
    private static final int ORDER_QUANTITY = 10;
    private static final int EXPECTED_ACCEPTED = 10;

    @Test
    void 동기화_없이_동시에_예약하면_위반이_관측되는_횟수를_센다() throws Exception {
        int acceptedOverLimitRounds = 0;
        int reservedSumOverBalanceRounds = 0;
        int exceptionRounds = 0;
        int anyViolationRounds = 0;

        for (int round = 0; round < ROUNDS; round++) {
            RoundResult result = runRound();
            boolean acceptedOverLimit = isAcceptedOverLimit(result);
            boolean reservedSumOverBalance = isReservedSumOverBalance(result);
            boolean exceptionOccurred = isExceptionOccurred(result);
            if (acceptedOverLimit) {
                acceptedOverLimitRounds++;
            }
            if (reservedSumOverBalance) {
                reservedSumOverBalanceRounds++;
            }
            if (exceptionOccurred) {
                exceptionRounds++;
            }
            if (acceptedOverLimit || reservedSumOverBalance || exceptionOccurred) {
                anyViolationRounds++;
            }
        }

        System.out.println("A3-CTL rounds=" + ROUNDS + " threads=" + THREADS
            + " 통과건수10초과=" + acceptedOverLimitRounds
            + " 예약합잔고초과=" + reservedSumOverBalanceRounds
            + " 예외발생=" + exceptionRounds
            + " 합산=" + anyViolationRounds);
    }

    private boolean isAcceptedOverLimit(RoundResult result) {
        return result.acceptedCount > EXPECTED_ACCEPTED;
    }

    private boolean isReservedSumOverBalance(RoundResult result) {
        return result.reservedSum.compareTo(INITIAL_BALANCE) > 0;
    }

    private boolean isExceptionOccurred(RoundResult result) {
        return result.exceptionCount > 0;
    }

    private RoundResult runRound() throws Exception {
        AccountState state = new AccountState(ACCOUNT_ID, INITIAL_BALANCE, MARGIN_RATE);
        AtomicInteger acceptedCount = new AtomicInteger();
        AtomicInteger exceptionCount = new AtomicInteger();
        List<BigDecimal> reservedMargins = new ArrayList<>();

        CountDownLatch ready = new CountDownLatch(THREADS);
        CountDownLatch start = new CountDownLatch(1);
        List<Thread> threads = new ArrayList<>();
        for (int thread = 0; thread < THREADS; thread++) {
            int threadIndex = thread;
            Thread worker = new Thread(() -> {
                ready.countDown();
                try {
                    start.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                for (int index = threadIndex; index < ORDER_COUNT; index += THREADS) {
                    try {
                        ReserveResult reserve = state.tryReserve(index + 1L, ORDER_PRICE, ORDER_QUANTITY);
                        if (reserve.accepted()) {
                            acceptedCount.incrementAndGet();
                            synchronized (reservedMargins) {
                                reservedMargins.add(reserve.reservedMargin());
                            }
                        }
                    } catch (RuntimeException e) {
                        exceptionCount.incrementAndGet();
                    }
                }
            });
            threads.add(worker);
            worker.start();
        }

        ready.await(10, TimeUnit.SECONDS);
        start.countDown();
        for (Thread worker : threads) {
            worker.join(30_000);
        }

        BigDecimal reservedSum = BigDecimal.ZERO;
        synchronized (reservedMargins) {
            for (BigDecimal reserved : reservedMargins) {
                reservedSum = reservedSum.add(reserved);
            }
        }
        return new RoundResult(acceptedCount.get(), reservedSum, exceptionCount.get());
    }

    private record RoundResult(int acceptedCount, BigDecimal reservedSum, int exceptionCount) {
    }
}
