package com.flab.stocktradingengine.api.service;

import java.util.List;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.flab.stocktradingengine.account.entity.Account;
import com.flab.stocktradingengine.api.dto.account.AccountStateView;
import com.flab.stocktradingengine.api.dto.account.HoldingView;
import com.flab.stocktradingengine.api.projection.entity.AccountProjection;
import com.flab.stocktradingengine.api.projection.entity.AccountProjectionHolding;
import com.flab.stocktradingengine.api.projection.repository.AccountProjectionHoldingRepository;
import com.flab.stocktradingengine.api.projection.repository.AccountProjectionRepository;
import com.flab.stocktradingengine.api.resolver.AccountAccessResolver;
import com.flab.stocktradingengine.exception.ResourceNotFoundException;

import lombok.RequiredArgsConstructor;

/**
 * 계좌 상태 프로젝션 트랙 U4 — v2 계좌 상태 read model(account_projection) 조회.
 * 잔고·보유는 항상 프로젝션이 authority다(account-worker가 발행한 full-state를
 * account-projection-worker가 upsert한 것) — v1 {@link Account}는 소유·활성 검증에만 쓴다.
 */
@Service
@RequiredArgsConstructor
public class AccountProjectionQueryService {

    private final AccountAccessResolver accountAccessResolver;
    private final AccountProjectionRepository accountProjectionRepository;
    private final AccountProjectionHoldingRepository accountProjectionHoldingRepository;

    @Transactional(readOnly = true)
    public AccountStateView getAccountState(Long userId, Long accountId) {
        Account account = accountAccessResolver.resolveAccountOwnedAndActive(userId, accountId);
        AccountProjection projection = accountProjectionRepository.findById(accountId)
            .orElseThrow(() -> new ResourceNotFoundException("아직 프로젝션에 없는 계좌: " + accountId));
        List<AccountProjectionHolding> holdings = accountProjectionHoldingRepository.findByAccountId(accountId);

        List<HoldingView> holdingViews = holdings.stream()
            .map(holding -> new HoldingView(holding.getStockCode(), holding.getQuantity()))
            .toList();
        return new AccountStateView(projection.getAccountId(), projection.getBalance(), projection.getSeq(), holdingViews);
    }
}
