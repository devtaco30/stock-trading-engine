package com.flab.stocktradingengine.api.controller.account;

import com.flab.stocktradingengine.api.dto.account.DepositRequest;
import com.flab.stocktradingengine.api.dto.account.WithdrawRequest;
import com.flab.stocktradingengine.api.dto.account.AccountDetailDto;
import com.flab.stocktradingengine.api.dto.account.AccountDto;
import com.flab.stocktradingengine.api.dto.account.HoldingDto;
import com.flab.stocktradingengine.api.dto.account.TransactionResponse;
import com.flab.stocktradingengine.dto.common.ApiResponse;
import com.flab.stocktradingengine.dto.common.PagedResponse;
import com.flab.stocktradingengine.api.resolver.CurrentUserId;
import com.flab.stocktradingengine.api.service.AccountApiService;
import jakarta.validation.Valid;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/accounts")
@RequiredArgsConstructor 
public class AccountController {

    private final AccountApiService accountApiService;

    /** 계좌 목록 조회 (인증된 사용자 소유 계좌만) */
    @GetMapping
    public ApiResponse<List<AccountDto>> accounts(@CurrentUserId Long userId) {
        return ApiResponse.of(accountApiService.getAccounts(userId));
    }

    /** 계좌 상세 조회 (본인 소유 계좌만) */
    @GetMapping("/{accountId}")
    public ApiResponse<AccountDetailDto> account(@CurrentUserId Long userId, @PathVariable Long accountId) {
        return ApiResponse.of(accountApiService.getAccountDetail(userId, accountId));
    }

    /**
     * 보유 종목 조회 (본인 소유 계좌만, DB 페이지네이션).
     */
    @GetMapping("/{accountId}/holdings")
    public ApiResponse<PagedResponse<HoldingDto>> holdings(
            @CurrentUserId Long userId,
            @PathVariable Long accountId,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "10") int size) {
        return ApiResponse.of(accountApiService.getHoldingsPaged(userId, accountId, page, size));
    }

    /** 입금 */
    @PostMapping("/{accountId}/deposit")
    public ApiResponse<TransactionResponse> deposit(
            @CurrentUserId Long userId,
            @PathVariable Long accountId,
            @Valid @RequestBody DepositRequest request) {
        return ApiResponse.of(accountApiService.deposit(userId, accountId, request.amount()));
    }

    /** 출금 */
    @PostMapping("/{accountId}/withdraw")
    public ApiResponse<TransactionResponse> withdraw(
            @CurrentUserId Long userId,
            @PathVariable Long accountId,
            @Valid @RequestBody WithdrawRequest request) {
        return ApiResponse.of(accountApiService.withdraw(userId, accountId, request.amount()));
    }
}
