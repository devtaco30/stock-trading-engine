package com.flab.stocktradingengine.settlement.service;

import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;

import org.springframework.stereotype.Service;

import com.flab.stocktradingengine.settlement.entity.Unpaid;
import com.flab.stocktradingengine.settlement.entity.UnpaidStatus;
import com.flab.stocktradingengine.settlement.repository.UnpaidRepository;

import lombok.RequiredArgsConstructor;

/**
 * 미결제 조회 전용 서비스 (settlement 도메인).
 */
@Service
@RequiredArgsConstructor
public class UnpaidQueryService {

    private final UnpaidRepository unpaidRepository;

    /**
     * 계좌별 PENDING 미결제 금액 합계. 계좌 상세(미결제 합계) 계산용.
     */
    public BigDecimal getPendingUnpaidSumByAccountId(Long accountId) {
        return unpaidRepository.findByAccount_AccountIdAndStatus(accountId, UnpaidStatus.PENDING)
            .stream()
            .map(Unpaid::getAmount)
            .reduce(BigDecimal.ZERO, BigDecimal::add);
    }

    /**
     * 계좌별 PENDING 미결제 목록. API 조회·미수금 집계용.
     */
    public List<Unpaid> getPendingUnpaidsByAccountId(Long accountId) {
        return unpaidRepository.findByAccount_AccountIdAndStatus(accountId, UnpaidStatus.PENDING);
    }

    /**
     * 미결제 ID로 단건 조회. 변제 시 금액·검증용.
     */
    public Optional<Unpaid> getUnpaidByUnpaidId(String unpaidId) {
        return unpaidRepository.findByUnpaidId(unpaidId);
    }
}
