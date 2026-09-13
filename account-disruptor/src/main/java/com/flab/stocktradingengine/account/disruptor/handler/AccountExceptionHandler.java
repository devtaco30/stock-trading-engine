package com.flab.stocktradingengine.account.disruptor.handler;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;

import com.flab.stocktradingengine.account.disruptor.domain.RejectReason;
import com.flab.stocktradingengine.account.disruptor.domain.ReserveResult;
import com.flab.stocktradingengine.account.disruptor.engine.AccountEvent;
import com.lmax.disruptor.ExceptionHandler;

/**
 * 계좌 워커 소비자에서 예상 못한 예외가 터졌을 때의 정책 — fail-fast.
 *
 * <p>계좌없음·수량이상·가용초과 같은 비즈니스 거부는 예외가 아니라 {@link ReserveResult}·
 * {@link RejectReason} 으로 리스너에 전달되므로 여기 오지 않는다. 여기 오는 건 NPE 같은
 * 예상 못한 오류뿐이고, 그건 버그이거나 계좌 상태가 오염됐다는 신호다. 오염된 상태 위에서
 * 이후 주문을 계속 검증하면 잘못된 예약이 누적되므로 소비자를 멈춘다(matching-disruptor 와 동일).</p>
 */
public class AccountExceptionHandler implements ExceptionHandler<AccountEvent> {

    private static final Logger log = System.getLogger(AccountExceptionHandler.class.getName());

    @Override
    public void handleEventException(Throwable ex, long sequence, AccountEvent event) {
        log.log(Level.ERROR, "[계좌] 예상 못한 오류로 소비자 중단: sequence=" + sequence
            + " accountId=" + event.getAccountId() + " requestId=" + event.getRequestId(), ex);
        throw new RuntimeException("계좌 워커 소비자에서 예상 못한 오류가 발생해 중단합니다", ex);
    }

    @Override
    public void handleOnStartException(Throwable ex) {
        log.log(Level.ERROR, "[계좌] 소비자 시작 중 오류", ex);
    }

    @Override
    public void handleOnShutdownException(Throwable ex) {
        log.log(Level.ERROR, "[계좌] 소비자 종료 중 오류", ex);
    }
}
