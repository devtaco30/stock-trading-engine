package com.flab.stocktradingengine.account.worker.coordination;

import java.util.Optional;

/**
 * 담당 매칭 pod 표의 한 시점 모습. 한 번 받은 view는 그 뒤 표가 갱신돼도 바뀌지 않으므로, sender
 * thread가 한 회차 동안 같은 view로 판단하면 회차 안에서 서로 맞는 표를 본다(설계서 11-1절).
 */
@FunctionalInterface
public interface MatchingTargetView {

    /**
     * 종목코드의 담당 pod. 담당이 없으면 empty.
     *
     * @throws MatchingDestinationMisconfiguredException 표가 주소 풀에 없는 주소를 가리킬 때
     */
    Optional<MatchingSlotTarget> targetFor(String stockCode);
}
