package com.flab.stocktradingengine.matching.worker.handover;

import java.util.Optional;

/** 호가창 이동 요청을 보내는 쪽({@link HandoverRequester})을 {@link HandoverCoordinator}가 쓰는 모양. 시험에서는 가짜로 바꾼다. */
public interface HandoverRequestPort {

    /** 종목 slot의 호가창을 요청한다. 보냈으면 이동 번호를, 맵에 이동 중 항목이 없거나 전송에 실패했으면 빈 값을 돌려준다. */
    Optional<Long> request(int stockSlot);

    /** 진행 중인 같은 요청(같은 이동 번호)을 다시 보낸다. 진행 중이 아니거나 전송에 실패하면 false. */
    boolean resend(int stockSlot);

    /** 그 종목 slot의 요청이 아직 진행 중인지(응답이 완성되어 끝나지도, 취소되지도 않았는지). */
    boolean inFlight(int stockSlot);

    /** 진행 중인 요청을 취소한다. 이후 도착하는 그 요청의 응답은 버려진다. */
    void cancel(int stockSlot);
}
