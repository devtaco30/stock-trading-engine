package com.flab.stocktradingengine.mapper;

import java.math.BigDecimal;
import java.math.RoundingMode;

import com.flab.stocktradingengine.account.entity.Account;
import com.flab.stocktradingengine.dto.account.response.AccountDetailDto;
import com.flab.stocktradingengine.dto.account.AccountDetailData;

/**
 * Account 엔티티 + 계좌 상세 데이터 → AccountDetailDto 변환.
 */
public final class AccountDetailDtoMapper {

    private AccountDetailDtoMapper() {
    }

    /**
     * 계좌 상세 조회용 DTO로 변환 (출금가능, 매수가능, 총자산 등 계산).
     */
    public static AccountDetailDto toDetailDto(Account account, AccountDetailData data) {
        BigDecimal withdrawableBalance = account.getBalance()
            .subtract(data.reservedMarginSum())
            .subtract(data.unpaidSum());
        BigDecimal marginRate = account.getMarginRate();
        BigDecimal buyLimit = withdrawableBalance.divide(marginRate, 0, RoundingMode.DOWN);
        BigDecimal totalAssets = account.getBalance().add(data.totalAssetsFromHoldings());
        return AccountDetailDto.builder()
            .accountId(account.getAccountId())
            .balance(account.getBalance())
            .withdrawableBalance(withdrawableBalance)
            .totalAssets(totalAssets)
            .unpaidAmount(data.unpaidSum())
            .pendingSellAmount(BigDecimal.ZERO)
            .buyLimit(buyLimit)
            .marginRate(account.getMarginRate())
            .status(account.getStatus().name())
            .build();
    }
}
