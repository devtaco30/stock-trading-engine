package com.flab.stocktradingengine.service;

import java.math.BigDecimal;
import java.util.List;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.flab.stocktradingengine.dto.settlement.ArrearsDto;
import com.flab.stocktradingengine.dto.settlement.RepaymentRequest;
import com.flab.stocktradingengine.dto.settlement.RepaymentResponse;
import com.flab.stocktradingengine.dto.settlement.UnpaidSettlementDto;
import com.flab.stocktradingengine.exception.ForbiddenException;
import com.flab.stocktradingengine.facade.AccountCommandFacade;
import com.flab.stocktradingengine.resolver.AccountAccessResolver;
import com.flab.stocktradingengine.settlement.entity.Unpaid;
import com.flab.stocktradingengine.settlement.entity.UnpaidStatus;
import com.flab.stocktradingengine.settlement.repository.UnpaidRepository;

import lombok.RequiredArgsConstructor;

/**
 * 정산 API 서비스. 미결제 조회·미수금 합계·변제 처리.
 */
@Service
@RequiredArgsConstructor
public class SettlementApiService {

    private final AccountAccessResolver accountAccessResolver;
    private final UnpaidRepository unpaidRepository;
    private final AccountCommandFacade accountCommandFacade;

    @Transactional(readOnly = true)
    public List<UnpaidSettlementDto> getUnpaidSettlements(Long userId, Long accountId) {
        accountAccessResolver.resolveAccountOwnedBy(userId, accountId)
            .orElseThrow(ForbiddenException::notOwnerOfAccount);
        return unpaidRepository.findByAccount_AccountId(accountId).stream()
            .map(u -> UnpaidSettlementDto.builder()
                .settlementId(u.getUnpaidId())
                .stockCode(u.getOrder().getStockCode())
                .settlementDate(u.getSettlementDate())
                .amount(u.getAmount())
                .status(u.getStatus().name())
                .build())
            .toList();
    }

    @Transactional(readOnly = true)
    public ArrearsDto getArrears(Long userId, Long accountId) {
        accountAccessResolver.resolveAccountOwnedBy(userId, accountId)
            .orElseThrow(ForbiddenException::notOwnerOfAccount);
        List<Unpaid> pending = unpaidRepository.findByAccount_AccountIdAndStatus(accountId, UnpaidStatus.PENDING);
        BigDecimal total = pending.stream().map(Unpaid::getAmount).reduce(BigDecimal.ZERO, BigDecimal::add);
        String arrearsId = pending.isEmpty() ? null : pending.get(0).getUnpaidId();
        return ArrearsDto.builder()
            .arrearsId(arrearsId)
            .amount(total)
            .occurredDate(pending.isEmpty() ? null : pending.get(0).getSettlementDate())
            .overdueDays(0)
            .accumulatedInterest(BigDecimal.ZERO)
            .build();
    }

    /** 미수금 변제: 해당 Unpaid를 SETTLED 처리하고 계좌에서 출금. */
    @Transactional(readOnly = false)
    public RepaymentResponse repay(Long userId, Long accountId, String arrearsId, RepaymentRequest request) {
        accountAccessResolver.resolveAccountOwnedAndActive(userId, accountId);
        Unpaid unpaid = unpaidRepository.findByUnpaidId(arrearsId)
            .orElseThrow(() -> new IllegalArgumentException("미결제를 찾을 수 없음: " + arrearsId));
        if (!unpaid.getAccount().getAccountId().equals(accountId)) {
            throw new IllegalArgumentException("해당 계좌의 미결제가 아님");
        }
        if (unpaid.getStatus() != UnpaidStatus.PENDING) {
            throw new IllegalStateException("이미 정산된 건입니다.");
        }
        BigDecimal amount = unpaid.getAmount();
        accountCommandFacade.withdraw(accountId, amount);
        unpaid.markSettled();
        unpaidRepository.save(unpaid);
        return RepaymentResponse.builder()
            .remainingAmount(BigDecimal.ZERO)
            .repaymentAt(System.currentTimeMillis())
            .build();
    }
}
