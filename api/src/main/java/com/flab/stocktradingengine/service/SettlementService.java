package com.flab.stocktradingengine.service;

import java.util.List;
import java.util.stream.Collectors;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.flab.stocktradingengine.dto.settlement.UnpaidSettlementDto;
import com.flab.stocktradingengine.settlement.entity.UnpaidStatus;
import com.flab.stocktradingengine.settlement.repository.UnpaidRepository;

import lombok.RequiredArgsConstructor;

/**
 * 정산 서비스 (시나리오 1: 미결제 내역 조회)
 */
@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class SettlementService {

    private final UnpaidRepository unpaidRepository;

    /**
     * 미결제 내역 조회
     */
    public List<UnpaidSettlementDto> getUnpaidSettlements(Long accountId) {
        return unpaidRepository.findByAccount_AccountIdAndStatus(accountId, UnpaidStatus.PENDING)
            .stream()
            .map(u -> UnpaidSettlementDto.builder()
                .settlementId(u.getUnpaidId())
                .stockCode(u.getOrder().getStockCode())
                .settlementDate(u.getSettlementDate())
                .amount(u.getAmount())
                .status(u.getStatus().name())
                .build())
            .collect(Collectors.toList());
    }
}
