package com.flab.stocktradingengine.api.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.flab.stocktradingengine.account.entity.Account;
import com.flab.stocktradingengine.api.dto.account.AccountStateView;
import com.flab.stocktradingengine.api.exception.ForbiddenException;
import com.flab.stocktradingengine.api.projection.entity.AccountProjection;
import com.flab.stocktradingengine.api.projection.entity.AccountProjectionHolding;
import com.flab.stocktradingengine.api.projection.repository.AccountProjectionHoldingRepository;
import com.flab.stocktradingengine.api.projection.repository.AccountProjectionRepository;
import com.flab.stocktradingengine.api.resolver.AccountAccessResolver;
import com.flab.stocktradingengine.exception.ResourceNotFoundException;

/**
 * 계좌 상태 프로젝션 트랙 U4 — read model(account_projection) 조회 서비스.
 * 소유·활성 검증(AccountAccessResolver)은 그대로 재사용하고, 잔고·보유는 프로젝션에서만 읽는다.
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("AccountProjectionQueryService - v2 계좌 상태 조회")
class AccountProjectionQueryServiceTest {

    @Mock AccountAccessResolver accountAccessResolver;
    @Mock AccountProjectionRepository accountProjectionRepository;
    @Mock AccountProjectionHoldingRepository accountProjectionHoldingRepository;

    @InjectMocks
    AccountProjectionQueryService accountProjectionQueryService;

    private static final Long USER_ID = 1L;
    private static final Long ACCOUNT_ID = 123L;

    @Test
    @DisplayName("프로젝션과 보유가 있으면 잔고·seq·보유 목록을 정확히 담아 반환한다")
    void 정상_조회() {
        when(accountAccessResolver.resolveAccountOwnedAndActive(USER_ID, ACCOUNT_ID)).thenReturn(mock(Account.class));
        AccountProjection projection = new AccountProjection(ACCOUNT_ID, new BigDecimal("500000"), 3L, Instant.now());
        when(accountProjectionRepository.findById(ACCOUNT_ID)).thenReturn(Optional.of(projection));
        List<AccountProjectionHolding> holdings = List.of(
            new AccountProjectionHolding(ACCOUNT_ID, "005930", 10),
            new AccountProjectionHolding(ACCOUNT_ID, "000660", 5));
        when(accountProjectionHoldingRepository.findByAccountId(ACCOUNT_ID)).thenReturn(holdings);

        AccountStateView view = accountProjectionQueryService.getAccountState(USER_ID, ACCOUNT_ID);

        assertThat(view.accountId()).isEqualTo(ACCOUNT_ID);
        assertThat(view.balance()).isEqualByComparingTo("500000");
        assertThat(view.seq()).isEqualTo(3L);
        assertThat(view.holdings()).hasSize(2);
        assertThat(view.holdings().get(0).stockCode()).isEqualTo("005930");
        assertThat(view.holdings().get(0).quantity()).isEqualTo(10);
    }

    @Test
    @DisplayName("보유가 없으면 빈 리스트를 담아 반환한다")
    void 보유_0건() {
        when(accountAccessResolver.resolveAccountOwnedAndActive(USER_ID, ACCOUNT_ID)).thenReturn(mock(Account.class));
        AccountProjection projection = new AccountProjection(ACCOUNT_ID, new BigDecimal("0"), 0L, Instant.now());
        when(accountProjectionRepository.findById(ACCOUNT_ID)).thenReturn(Optional.of(projection));
        when(accountProjectionHoldingRepository.findByAccountId(ACCOUNT_ID)).thenReturn(List.of());

        AccountStateView view = accountProjectionQueryService.getAccountState(USER_ID, ACCOUNT_ID);

        assertThat(view.holdings()).isEmpty();
    }

    @Test
    @DisplayName("프로젝션 행이 없으면 ResourceNotFoundException을 던진다(=404)")
    void 프로젝션_없음() {
        when(accountAccessResolver.resolveAccountOwnedAndActive(USER_ID, ACCOUNT_ID)).thenReturn(mock(Account.class));
        when(accountProjectionRepository.findById(ACCOUNT_ID)).thenReturn(Optional.empty());

        assertThatThrownBy(() -> accountProjectionQueryService.getAccountState(USER_ID, ACCOUNT_ID))
            .isInstanceOf(ResourceNotFoundException.class);
    }

    @Test
    @DisplayName("다른 유저의 계좌면 ForbiddenException을 그대로 전파한다(=403)")
    void 미소유_계좌() {
        when(accountAccessResolver.resolveAccountOwnedAndActive(USER_ID, ACCOUNT_ID))
            .thenThrow(ForbiddenException.notOwnerOfAccount());

        assertThatThrownBy(() -> accountProjectionQueryService.getAccountState(USER_ID, ACCOUNT_ID))
            .isInstanceOf(ForbiddenException.class);
    }
}
