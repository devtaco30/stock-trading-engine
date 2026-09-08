package com.flab.stocktradingengine.settlement.worker;

import java.util.List;

import org.springframework.stereotype.Component;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * T+2 만기가 지난 PENDING 건을 스캔해 건마다 {@link PendingSettlementSettler}에 발행·정산완료
 * 처리를 위임한다. 정산 왕복(fill→미수금→settlement-requests→PENDING→여기→
 * account-settlements→applySettlement)의 마지막 구간이다.
 *
 * <p>이 클래스 자체는 트랜잭션이 아니다 — 스캔(조회)과 건별 발행+커밋을 분리해, 한 건의 발행
 * 실패가 다른 건의 처리를 막지 않게 한다(배치 트랜잭션으로 묶지 않는 이유).</p>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class SettlementDispatcher {

    private final PendingSettlementRepository pendingSettlementRepository;
    private final PendingSettlementSettler pendingSettlementSettler;

    /** @return 이번 호출에서 실제로 발행·정산완료 처리한 건수 */
    public int dispatchDue(long nowEpochMillis) {
        List<PendingSettlement> due = pendingSettlementRepository
            .findByStatusAndDueAtEpochMillisLessThanEqual(SettlementStatus.PENDING, nowEpochMillis);

        int dispatched = 0;
        for (PendingSettlement pendingSettlement : due) {
            Long settlementRef = pendingSettlement.getSettlementRef();
            try {
                pendingSettlementSettler.settle(settlementRef);
                dispatched++;
            } catch (Exception e) {
                // 이 건은 PENDING으로 남는다 — 다음 스캔이 재시도한다(at-least-once).
                log.error("정산 되돌림 발행 실패, 다음 스캔에서 재시도: settlementRef={}", settlementRef, e);
            }
        }
        return dispatched;
    }
}
