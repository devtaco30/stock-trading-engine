package com.flab.stocktradingengine.resolver;

import java.util.Optional;

import org.springframework.stereotype.Component;

import com.flab.stocktradingengine.account.entity.Account;
import com.flab.stocktradingengine.account.entity.AccountStatus;
import com.flab.stocktradingengine.account.service.AccountService;
import com.flab.stocktradingengine.exception.ForbiddenException;

import lombok.RequiredArgsConstructor;

/**
 * API 경계에서 계좌 접근 인가: 요청 사용자(userId)가 해당 계좌(accountId)에 대해
 * 소유(및 필요 시 ACTIVE) 검증 후 계좌를 반환. 실패 시 ForbiddenException.
 */
@Component
@RequiredArgsConstructor
public class AccountAccessResolver {

    private final AccountService accountService;

    /**
     * 계좌 소유만 검증. 조회용(ACTIVE 불요).
     * 계좌 없음 → empty, 있으면 소유 검사 후 반환 또는 403.
     */
    public Optional<Account> resolveAccountOwnedBy(Long userId, Long accountId) {
        return accountService.getAccount(accountId)
            .map(account -> {
                if (!account.isOwnedBy(userId)) {
                    throw ForbiddenException.notOwnerOfAccount();
                }
                return account;
            });
    }

    /**
     * 계좌 소유·ACTIVE 검증 후 계좌 반환. 실패 시 ForbiddenException.
     */
    public Account resolveAccountOwnedAndActive(Long userId, Long accountId) {
        Account account = accountService.getAccount(accountId).orElseThrow(ForbiddenException::notOwnerOfAccount);
        if (!account.isOwnedBy(userId)) {
            throw ForbiddenException.notOwnerOfAccount();
        }
        if (account.getStatus() != AccountStatus.ACTIVE) {
            throw ForbiddenException.accountNotActive();
        }
        return account;
    }
}
