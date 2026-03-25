package com.flab.stocktradingengine.api.mapper;

import com.flab.stocktradingengine.account.entity.Account;
import com.flab.stocktradingengine.api.dto.account.AccountDto;

/**
 * Account 엔티티 → AccountDto 변환.
 */
public final class AccountDtoMapper {

    private AccountDtoMapper() {
    }

    public static AccountDto toAccountDto(Account account) {
        return AccountDto.builder()
            .accountId(account.getAccountId())
            .balance(account.getBalance())
            .marginRate(account.getMarginRate())
            .status(account.getStatus().name())
            .build();
    }
}
