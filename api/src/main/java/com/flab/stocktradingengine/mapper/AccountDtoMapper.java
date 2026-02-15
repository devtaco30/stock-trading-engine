package com.flab.stocktradingengine.mapper;

import java.math.BigDecimal;

import com.flab.stocktradingengine.account.entity.Account;
import com.flab.stocktradingengine.dto.account.response.AccountDto;
import com.flab.stocktradingengine.dto.account.AccountDetailData;

/**
 * Account 엔티티 + 계좌 상세 데이터 → AccountDto 변환.
 */
public final class AccountDtoMapper {

    private AccountDtoMapper() {
    }

    /**
     * 계좌 엔티티와 상세 데이터(증거금 합계 등)를 받아 목록용 DTO로 변환.
     */
    public static AccountDto toAccountDto(Account account, AccountDetailData data) {
        BigDecimal withdrawableBalance = account.getBalance()
            .subtract(data.reservedMarginSum())
            .subtract(data.unpaidSum());
        return AccountDto.builder()
            .accountId(account.getAccountId())
            .balance(account.getBalance())
            .withdrawableBalance(withdrawableBalance)
            .marginRate(account.getMarginRate())
            .status(account.getStatus().name())
            .build();
    }
}
