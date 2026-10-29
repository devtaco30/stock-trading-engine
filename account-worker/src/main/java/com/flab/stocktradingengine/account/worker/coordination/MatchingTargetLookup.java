package com.flab.stocktradingengine.account.worker.coordination;

/** sender thread가 회차마다 표의 한 시점 모습({@link MatchingTargetView})을 받아 가는 곳. */
@FunctionalInterface
public interface MatchingTargetLookup {

    MatchingTargetView view();
}
