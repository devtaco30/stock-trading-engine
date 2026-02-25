package com.flab.stocktradingengine.settlement.service;

import java.math.BigDecimal;

import org.springframework.lang.NonNull;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.flab.stocktradingengine.settlement.entity.UnpaidStatus;
import com.flab.stocktradingengine.settlement.repository.UnpaidRepository;

import lombok.RequiredArgsConstructor;

/**
 * 미결제 조회 전용 서비스 (settlement 도메인).
 */
@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class UnpaidQueryService {

    private final UnpaidRepository unpaidRepository;

    /**
     * 계좌별 PENDING 미결제 금액 합계. 계좌 상세(미결제 합계) 계산용.
     */
    public BigDecimal getPendingUnpaidSumByAccountId(@NonNull Long accountId) {
        return unpaidRepository.findByAccount_AccountIdAndStatus(accountId, UnpaidStatus.PENDING)
            .stream()
            .map(u -> u.getAmount())
            .reduce(BigDecimal.ZERO, BigDecimal::add);
    }
}
