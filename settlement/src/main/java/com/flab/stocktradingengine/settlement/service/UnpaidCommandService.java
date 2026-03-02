package com.flab.stocktradingengine.settlement.service;

import org.springframework.lang.NonNull;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.flab.stocktradingengine.settlement.repository.UnpaidRepository;

import lombok.RequiredArgsConstructor;

/**
 * 미결제(Unpaid) 명령 전용 서비스. 변제(정산 완료) 처리 등.
 */
@Service
@RequiredArgsConstructor
public class UnpaidCommandService {

    private final UnpaidRepository unpaidRepository;

    /**
     * 미결제를 정산 완료(SETTLED) 처리.
     *
     * <p>dirty checking 에 의존하지 않고 단일 UPDATE 쿼리로 처리한다.
     * WHERE 절에 status = PENDING 조건을 포함하므로, 이미 정산된 건이거나 해당 계좌의 건이 아니면
     * affected rows = 0 이 되어 예외를 던진다. 이를 통해 동시 요청 시 중복 정산도 방지된다.</p>
     */
    @Transactional
    public void settleUnpaid(@NonNull String unpaidId, @NonNull Long accountId) {
        int updated = unpaidRepository.markSettledIfPending(unpaidId, accountId);
        if (updated == 0) {
            throw new IllegalStateException("이미 정산됐거나 해당 계좌의 건이 아닙니다.");
        }
    }
}
