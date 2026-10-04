package com.flab.stocktradingengine.matching.disruptor.io;

import com.flab.stocktradingengine.trading.matching.FillResult;
import com.flab.stocktradingengine.trading.matching.OrderBook;

/**
 * 체결 결과를 매칭 코어 밖으로 내보내는 출구.
 *
 * <p>매칭 스레드가 체결을 낼 때마다 {@link #onFill} 을 호출한다.
 * Unit 1 에서는 테스트가 이 인터페이스를 구현해 결과를 수집·검증한다.
 * 이후 단위에서 이 자리에 정산 발행(Kafka fills) 등이 연결된다.</p>
 */
public interface MatchListener {

    /**
     * 체결 1건을 전달받는다.
     *
     * @param stockCode 체결이 일어난 종목코드
     * @param fill      체결 결과(매수·매도 주문 ID, 수량, 체결가)
     */
    void onFill(String stockCode, FillResult fill);

    /**
     * 구간 분해 측정용 — 체결 방출 시각(W-P14)과 이 체결을 일으킨 주문(aggressor)의 발신 시각(W-P0)을
     * 함께 전달한다. 한 주문이 체결을 여러 건 내면 첫 체결에만 값이 있고 나머지는 0이다. 시각을 실지
     * 않는 구현은 기본 동작(시각을 버리고 기존 호출)을 쓴다.
     */
    default void onFill(String stockCode, FillResult fill, long emittedAtNanos, long aggressorOriginNanos) {
        onFill(stockCode, fill);
    }

    /**
     * 이벤트를 처리한 뒤 이 종목의 호가창이 바뀌었을 수 있음을 알린다. 매칭 핸들러가 배치의 마지막 이벤트에서 그 배치가 건드린 종목마다
     * 한 번씩 부른다(한가해서 배치 크기가 1이면 이벤트마다). 호가창은 매칭 스레드만 읽고 바꾸므로 구현은 이 호출 안에서 필요한 값을
     * 복사해야 하고, I/O를 하면 안 된다. 기본 구현은 아무것도 하지 않는다.
     */
    default void onBookChanged(String stockCode, OrderBook book) {
    }
}
