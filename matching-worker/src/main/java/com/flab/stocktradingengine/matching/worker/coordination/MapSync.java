package com.flab.stocktradingengine.matching.worker.coordination;

/**
 * 맵 읽기가 맵 토픽의 끝(지금 시점의 end offset)까지 따라왔는지 기다린다. 배정을 받은 종목 slot이 이동으로 받는 것인지 첫 배정인지
 * 맵의 이전 담당 항목으로 판정하기 전에, 이전 담당이 쓴 이동 중 값이 읽는 쪽에 도착해 있어야 하기 때문이다.
 */
@FunctionalInterface
public interface MapSync {

    /** @return 시간 안에 따라왔으면 true, 못 따라왔으면 false */
    boolean awaitCaughtUp();

    /** 맵 읽기가 정상인지. 정상이 아니면 따라오기를 기다리지 않고 판정을 다음 간격으로 미룬다. */
    default boolean healthy() {
        return true;
    }
}
