package com.flab.stocktradingengine.settlement.service;

import org.springframework.lang.NonNull;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.flab.stocktradingengine.settlement.entity.Unpaid;
import com.flab.stocktradingengine.settlement.entity.UnpaidStatus;
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
     * 해당 계좌 소유·PENDING 상태만 허용.
     */
    @Transactional
    public Unpaid settleUnpaid(@NonNull String unpaidId, @NonNull Long accountId) {
        Unpaid unpaid = unpaidRepository.findByUnpaidId(unpaidId)
            .orElseThrow(() -> new IllegalArgumentException("미결제를 찾을 수 없음: " + unpaidId));
        if (!String.valueOf(accountId).equals(String.valueOf(unpaid.getAccount().getAccountId()))) {
            throw new IllegalArgumentException("해당 계좌의 미결제가 아님");
        }
        if (unpaid.getStatus() != UnpaidStatus.PENDING) {
            throw new IllegalStateException("이미 정산된 건입니다.");
        }
        unpaid.markSettled();
        return unpaidRepository.save(unpaid);
    }
}
