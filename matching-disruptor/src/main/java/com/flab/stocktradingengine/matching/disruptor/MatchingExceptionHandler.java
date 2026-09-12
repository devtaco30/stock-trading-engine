package com.flab.stocktradingengine.matching.disruptor;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;

import com.lmax.disruptor.ExceptionHandler;

/**
 * 링버퍼 소비자(저널러·매처)에서 예상 못한 예외가 터졌을 때의 처리 정책.
 *
 * <h3>왜 별도 처리기가 필요한가</h3>
 * <p>{@link MatchingEventHandler} 는 도메인 불변식 위반({@code IllegalArgumentException},
 * {@code IllegalStateException})만 잡아 해당 이벤트를 폐기하고 계속 돈다.
 * 그 두 예외를 제외한 나머지(NPE 등)는 핸들러 밖으로 그대로 전파되는데,
 * Disruptor 는 이 예외를 이 처리기로 넘긴다.</p>
 *
 * <h3>fail-fast 인 이유</h3>
 * <p>예상 못한 예외는 버그이거나 상태가 이미 오염됐다는 신호다. 로그만 남기고 다음 이벤트를
 * 계속 처리하면, 오염된 호가창 상태 위에서 이후 주문을 계속 매칭하게 되어 잘못된 체결이
 * 누적될 위험이 있다. 그래서 더 오염되기 전에 소비자를 멈춘다(fail-fast).
 * 복구는 프로세스 재시작 + 저널 리플레이로 처리한다(이후 단위에서 구현).</p>
 */
public class MatchingExceptionHandler implements ExceptionHandler<OrderEvent> {

    private static final Logger log = System.getLogger(MatchingExceptionHandler.class.getName());

    @Override
    public void handleEventException(Throwable ex, long sequence, OrderEvent event) {
        String eventType = event.getType() == null ? "UNKNOWN" : event.getType().toString();
        log.log(Level.ERROR, "[매칭] 예상 못한 오류로 소비자 중단: sequence=" + sequence
            + " type=" + eventType + " orderId=" + event.getOrderId(), ex);
        // 예상 못한 오류는 버그·상태 오염 신호이므로 더 오염되기 전에 멈춘다(fail-fast).
        // 복구는 재시작 + 저널 리플레이로 처리한다(이후 단위).
        throw new RuntimeException("매칭 소비자에서 예상 못한 오류가 발생해 중단합니다", ex);
    }

    @Override
    public void handleOnStartException(Throwable ex) {
        log.log(Level.ERROR, "[매칭] 소비자 시작 중 오류", ex);
    }

    @Override
    public void handleOnShutdownException(Throwable ex) {
        log.log(Level.ERROR, "[매칭] 소비자 종료 중 오류", ex);
    }
}
