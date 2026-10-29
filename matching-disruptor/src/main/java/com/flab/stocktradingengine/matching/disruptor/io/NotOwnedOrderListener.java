package com.flab.stocktradingengine.matching.disruptor.io;

import com.flab.stocktradingengine.codec.JournaledOrder;

/**
 * 담당이 아닌 종목 slot의 주문을 받았을 때 {@link AeronOrderReceiver}가 부르는 곳. 그 주문은 매칭 엔진에
 * 접수되지 않는다. 호출하는 thread는 주문 수신 폴링 thread 하나다.
 */
@FunctionalInterface
public interface NotOwnedOrderListener {

    /** 아무것도 하지 않는다. 거부 회신 경로가 연결되기 전의 기본값이다. */
    NotOwnedOrderListener NO_OP = order -> { };

    void onNotOwned(JournaledOrder order);
}
