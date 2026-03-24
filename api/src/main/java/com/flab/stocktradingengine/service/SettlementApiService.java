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
import com.flab.stocktradingengine.settlement.service.UnpaidCommandService;
import com.flab.stocktradingengine.settlement.service.UnpaidQueryService;

import lombok.RequiredArgsConstructor;

/**
 * 정산 API 서비스. 미결제 조회·미수금 합계·변제 처리.
 * <p>조회는 {@link UnpaidQueryService}, 변제(상태 변경)는 {@link UnpaidCommandService}에 위임. API 레이어에서는 repository를 직접 사용하지 않음.</p>
 */
@Service
@RequiredArgsConstructor
public class SettlementApiService {

    private final AccountAccessResolver accountAccessResolver;
    private final UnpaidQueryService unpaidQueryService;
    private final UnpaidCommandService unpaidCommandService;
    private final AccountCommandFacade accountCommandFacade;

    /**
     * 계좌별 미결제(미정산) 목록 조회. 본인 소유 계좌만 허용.
     * 한 메서드 내 여러 조회가 같은 스냅샷을 보도록 readOnly 트랜잭션 사용.
     */
    @Transactional(readOnly = true)
    public List<UnpaidSettlementDto> getUnpaidSettlements(Long userId, Long accountId) {
        accountAccessResolver.resolveAccountOwnedBy(userId, accountId)
            .orElseThrow(ForbiddenException::notOwnerOfAccount);
        List<Unpaid> pendingList = unpaidQueryService.getPendingUnpaidsByAccountId(accountId);
        return pendingList.stream()
            .map(u -> UnpaidSettlementDto.builder()
                .settlementId(u.getUnpaidId())
                .stockCode(u.getOrder().getStockCode())
                .settlementDate(u.getSettlementDate())
                .amount(u.getAmount())
                .status(u.getStatus().name())
                .build())
            .toList();
    }

    /**
     * 계좌별 미수금(연체) 요약. 본인 소유 계좌만 허용.
     * Lazy 관계 접근 없음. 기본 필드만 사용하므로 트랜잭션 불필요.
     */
    public ArrearsDto getArrears(Long userId, Long accountId) {
        accountAccessResolver.resolveAccountOwnedBy(userId, accountId)
            .orElseThrow(ForbiddenException::notOwnerOfAccount);
        List<Unpaid> pending = unpaidQueryService.getPendingUnpaidsByAccountId(accountId);
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

    /**
     * 미수금 변제: 해당 Unpaid를 SETTLED 처리하고 계좌에서 출금.
     * Resolver 검증과 쓰기(출금·상태 변경)를 한 트랜잭션으로 묶어 정합성(같은 스냅샷)과 효율(커넥션 1회 사용) 확보.
     */
    @Transactional
    public RepaymentResponse repay(Long userId, Long accountId, String arrearsId, RepaymentRequest request) {
        accountAccessResolver.resolveAccountOwnedAndActive(userId, accountId);
        Unpaid unpaid = unpaidQueryService.getUnpaidByUnpaidId(arrearsId)
            .orElseThrow(() -> new IllegalArgumentException("미결제를 찾을 수 없음: " + arrearsId));
        if (!unpaid.getAccount().getAccountId().equals(accountId)) {
            throw new IllegalArgumentException("해당 계좌의 미결제가 아님");
        }
        if (unpaid.getStatus() != UnpaidStatus.PENDING) {
            throw new IllegalStateException("이미 정산된 건입니다.");
        }
        BigDecimal amount = unpaid.getAmount();
        accountCommandFacade.withdraw(accountId, amount);
        unpaidCommandService.settleUnpaid(arrearsId, accountId);
        return RepaymentResponse.builder()
            .remainingAmount(BigDecimal.ZERO)
            .repaymentAt(System.currentTimeMillis())
            .build();
    }
}
