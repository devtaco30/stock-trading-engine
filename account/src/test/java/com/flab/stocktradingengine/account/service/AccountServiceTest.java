package com.flab.stocktradingengine.account.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;

import com.flab.stocktradingengine.account.entity.Account;
import com.flab.stocktradingengine.account.entity.AccountStatus;
import com.flab.stocktradingengine.account.entity.AccountStatusHistory;
import com.flab.stocktradingengine.account.entity.Holding;
import com.flab.stocktradingengine.account.repository.AccountRepository;
import com.flab.stocktradingengine.user.entity.User;
import com.flab.stocktradingengine.account.repository.AccountStatusHistoryRepository;
import com.flab.stocktradingengine.account.repository.HoldingRepository;
import com.flab.stocktradingengine.dto.common.PagedResponse;

/**
 * account 모듈 {@link AccountService} 단위 테스트.
 * <p>Repository를 mock하여 서비스 로직만 검증한다.</p>
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("AccountService 단위 테스트")
class AccountServiceTest {

    @Mock
    AccountRepository accountRepository;
    
    @Mock
    AccountStatusHistoryRepository accountStatusHistoryRepository;
    
    @Mock
    HoldingRepository holdingRepository;

    @InjectMocks
    AccountService accountService;

    @Nested
    @DisplayName("getAccount")
    class GetAccount {

        @Test
        @DisplayName("계좌가 있으면 Optional에 담아 반환")
        void returnsOptionalWhenExists() {
            Long accountId = 1L;
            Account account = mockAccount(accountId);
            
            when(accountRepository.findByAccountId(accountId)).thenReturn(Optional.of(account));

            Optional<Account> result = accountService.getAccount(accountId);

            assertTrue(result.isPresent());
            assertSame(account, result.get());
            
            verify(accountRepository).findByAccountId(accountId);
        }

        @Test 
        @DisplayName("계좌가 없으면 empty 반환")
        void returnsEmptyWhenNotExists() {
            when(accountRepository.findByAccountId(1L)).thenReturn(Optional.empty());

            Optional<Account> result = accountService.getAccount(1L);

            assertTrue(result.isEmpty());
            verify(accountRepository).findByAccountId(1L);
        }
    }

    @Nested
    @DisplayName("getAccountsByUserId")
    class GetAccountsByUserId {

        @Test
        @DisplayName("해당 사용자 계좌 목록 반환")
        void returnsAccountList() {
            Long userId = 1L;
            Account account = mockAccount(1L);
            List<Account> accounts = List.of(account);
            when(accountRepository.findByUser_Id(userId)).thenReturn(accounts);

            List<Account> result = accountService.getAccountsByUserId(userId);

            assertEquals(1, result.size());
            assertSame(account, result.get(0));
            verify(accountRepository).findByUser_Id(userId);
        }

        @Test // Coverage 에 해당하지는 않지만, Spec 정의를 위한 테스트
        @DisplayName("계좌가 없으면 빈 목록 반환")
        void returnsEmptyListWhenNoAccounts() {
            when(accountRepository.findByUser_Id(2L)).thenReturn(List.of());

            List<Account> result = accountService.getAccountsByUserId(2L);

            assertTrue(result.isEmpty());
            verify(accountRepository).findByUser_Id(2L);
        }
    }

    @Nested
    @DisplayName("getHoldingsPage")
    class GetHoldingsPage {

        @Test
        @DisplayName("보유 페이지 반환")
        void returnsHoldingPage() {
            Long accountId = 1L;
            Pageable pageable = PageRequest.of(0, 10);
            Page<Holding> page = new PageImpl<>(List.of(), pageable, 0);
            when(holdingRepository.findByAccount_AccountId(accountId, pageable)).thenReturn(page);

            PagedResponse<Holding> result = accountService.getHoldingsPage(accountId, pageable);

            assertTrue(result.getData().isEmpty());
            assertEquals(0, result.getPagination().page());
            assertEquals(10, result.getPagination().size());
            assertEquals(0L, result.getPagination().totalElements());
            verify(holdingRepository).findByAccount_AccountId(accountId, pageable);
        }
    }

    @Nested
    @DisplayName("changeStatus")
    class ChangeStatus {

        private static final Long NOT_FOUND_ACCOUNT_ID = 999L;
        private static final Long SAMPLE_ACCOUNT_ID = 1L;

        @Test
        @DisplayName("계좌가 없으면 NoSuchElementException")
        void throwsWhenAccountNotFound() {
            when(accountRepository.findByAccountId(NOT_FOUND_ACCOUNT_ID)).thenReturn(Optional.empty());

            assertThrows(NoSuchElementException.class,
                () -> accountService.changeStatus(NOT_FOUND_ACCOUNT_ID, AccountStatus.IN_ARREARS, null, "reason"));
            verify(accountRepository).findByAccountId(NOT_FOUND_ACCOUNT_ID);
        }

        @Test
        @DisplayName("동일 상태로 변경 시 IllegalStateException")
        void throwsWhenSameStatus() {
            Account account = mockAccount(SAMPLE_ACCOUNT_ID, AccountStatus.ACTIVE);
            when(accountRepository.findByAccountId(SAMPLE_ACCOUNT_ID)).thenReturn(Optional.of(account));

            assertThrows(IllegalStateException.class,
                () -> accountService.changeStatus(SAMPLE_ACCOUNT_ID, AccountStatus.ACTIVE, null, "reason"));
            verify(accountRepository).findByAccountId(SAMPLE_ACCOUNT_ID);
        }

        @Test
        @DisplayName("허용되지 않은 전이 시 IllegalStateException (FROZEN -> IN_ARREARS)")
        void throwsWhenInvalidTransition() {
            Account account = mockAccount(SAMPLE_ACCOUNT_ID, AccountStatus.FROZEN);
            when(accountRepository.findByAccountId(SAMPLE_ACCOUNT_ID)).thenReturn(Optional.of(account));

            assertThrows(IllegalStateException.class,
                () -> accountService.changeStatus(SAMPLE_ACCOUNT_ID, AccountStatus.IN_ARREARS, null, "reason"));
        }

        @Test
        @DisplayName("ACTIVE -> IN_ARREARS 전이 시 changeStatus 호출 및 이력 저장")
        void activeToInArrears_savesHistory() {
            Account account = mockAccount(SAMPLE_ACCOUNT_ID, AccountStatus.ACTIVE);
            when(accountRepository.findByAccountId(SAMPLE_ACCOUNT_ID)).thenReturn(Optional.of(account));

            accountService.changeStatus(SAMPLE_ACCOUNT_ID, AccountStatus.IN_ARREARS, null, "미수 발생");

            verify(account).changeStatus(AccountStatus.IN_ARREARS);
            ArgumentCaptor<AccountStatusHistory> captor = ArgumentCaptor.forClass(AccountStatusHistory.class);
            verify(accountStatusHistoryRepository).save(captor.capture());
            AccountStatusHistory saved = captor.getValue();
            assertEquals(account, saved.getAccount());
            assertEquals(AccountStatus.ACTIVE, saved.getFromStatus());
            assertEquals(AccountStatus.IN_ARREARS, saved.getToStatus());
            assertEquals(null, saved.getChangedByUser());
            assertEquals("미수 발생", saved.getReason());
        }

        @Test
        @DisplayName("changedByUser가 있으면 이력에 저장")
        void savesChangedByUserInHistory() {
            Account account = mockAccount(SAMPLE_ACCOUNT_ID, AccountStatus.ACTIVE);
            User user = org.mockito.Mockito.mock(User.class);
            when(accountRepository.findByAccountId(SAMPLE_ACCOUNT_ID)).thenReturn(Optional.of(account));

            accountService.changeStatus(SAMPLE_ACCOUNT_ID, AccountStatus.FROZEN, user, "관리자 동결");

            ArgumentCaptor<AccountStatusHistory> captor = ArgumentCaptor.forClass(AccountStatusHistory.class);
            verify(accountStatusHistoryRepository).save(captor.capture());
            assertEquals(user, captor.getValue().getChangedByUser());
        }
    }

    @Nested
    @DisplayName("addHoldingOrIncreaseQuantity")
    class AddHoldingOrIncreaseQuantity {

        private static final Long NOT_FOUND_ACCOUNT_ID = 999L;
        private static final Long SAMPLE_ACCOUNT_ID = 1L;

        @Test
        @DisplayName("계좌가 없으면 NoSuchElementException")
        void throwsWhenAccountNotFound() {
            when(accountRepository.findByAccountId(NOT_FOUND_ACCOUNT_ID)).thenReturn(Optional.empty());

            assertThrows(NoSuchElementException.class,
                () -> accountService.addHoldingOrIncreaseQuantity(NOT_FOUND_ACCOUNT_ID, "005930", 10, new BigDecimal("70000")));
            verify(accountRepository).findByAccountId(NOT_FOUND_ACCOUNT_ID);
        }

        @Test
        @DisplayName("기존 보유가 있으면 applyExecution 호출")
        void increasesQuantityWhenHoldingExists() {
            Account account = mockAccount(SAMPLE_ACCOUNT_ID);
            Holding existing = new Holding(account, "005930", 50, new BigDecimal("68000"));
            when(accountRepository.findByAccountId(SAMPLE_ACCOUNT_ID)).thenReturn(Optional.of(account));
            when(holdingRepository.findByAccount_IdAndStockCodeForUpdate(SAMPLE_ACCOUNT_ID, "005930"))
                .thenReturn(Optional.of(existing));

            accountService.addHoldingOrIncreaseQuantity(SAMPLE_ACCOUNT_ID, "005930", 50, new BigDecimal("70000"));

            verify(holdingRepository).findByAccount_IdAndStockCodeForUpdate(SAMPLE_ACCOUNT_ID, "005930");
            assertEquals(100, existing.getQuantity());
            assertEquals(0, new BigDecimal("69000").compareTo(existing.getAveragePrice()));
            verify(holdingRepository, never()).save(any());
            verify(accountRepository).findByAccountId(SAMPLE_ACCOUNT_ID);
        }

        @Test
        @DisplayName("기존 보유가 없으면 새 Holding 저장")
        void savesNewHoldingWhenNotExists() {
            Account account = mockAccount(SAMPLE_ACCOUNT_ID);
            when(accountRepository.findByAccountId(SAMPLE_ACCOUNT_ID)).thenReturn(Optional.of(account));
            when(holdingRepository.findByAccount_IdAndStockCodeForUpdate(SAMPLE_ACCOUNT_ID, "005930")).thenReturn(Optional.empty());

            accountService.addHoldingOrIncreaseQuantity(SAMPLE_ACCOUNT_ID, "005930", 100, new BigDecimal("70000"));

            ArgumentCaptor<Holding> captor = ArgumentCaptor.forClass(Holding.class);
            verify(holdingRepository).save(captor.capture());
            Holding saved = captor.getValue();
            assertEquals(account, saved.getAccount());
            assertEquals("005930", saved.getStockCode());
            assertEquals(100, saved.getQuantity());
            assertEquals(0, new BigDecimal("70000").compareTo(saved.getAveragePrice()));
        }
    }

    private static Account mockAccount(Long accountId) {
        return mockAccount(accountId, AccountStatus.ACTIVE);
    }

    /**
     * id(PK), accountId(비즈니스 키) 모두 Long. 단위 테스트에서는 동일 값으로 모킹.
     */
    private static Account mockAccount(Long accountId, AccountStatus status) {
        Account account = org.mockito.Mockito.mock(Account.class);
        lenient().when(account.getId()).thenReturn(accountId);
        lenient().when(account.getAccountId()).thenReturn(accountId);
        lenient().when(account.getStatus()).thenReturn(status);
        return account;
    }
}
