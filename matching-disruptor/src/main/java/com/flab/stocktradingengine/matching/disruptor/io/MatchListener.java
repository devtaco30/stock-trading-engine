package com.flab.stocktradingengine.matching.disruptor.io;

import com.flab.stocktradingengine.trading.matching.FillResult;

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
}
