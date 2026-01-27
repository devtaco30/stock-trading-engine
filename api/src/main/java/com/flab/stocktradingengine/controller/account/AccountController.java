package com.flab.stocktradingengine.controller.account;

import com.flab.stocktradingengine.dto.account.AccountDetailDto;
import com.flab.stocktradingengine.dto.account.AccountDto;
import com.flab.stocktradingengine.dto.account.DepositRequest;
import com.flab.stocktradingengine.dto.account.HoldingDto;
import com.flab.stocktradingengine.dto.account.TransactionResponse;
import com.flab.stocktradingengine.dto.account.WithdrawRequest;
import com.flab.stocktradingengine.dto.common.ApiResponse;
import com.flab.stocktradingengine.dto.common.PagedResponse;
import com.flab.stocktradingengine.dto.common.PaginationInfo;
import com.flab.stocktradingengine.dto.order.OrderDto;
import com.flab.stocktradingengine.dto.transaction.TransactionDto;
import com.flab.stocktradingengine.dummy.DummyAccountData;
import jakarta.validation.Valid;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/accounts")
public class AccountController {


    /**
     * 계좌 목록 조회
     */
    @GetMapping
    public ApiResponse<PagedResponse<AccountDto>> accounts() {
        var accounts = DummyAccountData.getAccounts();
        PaginationInfo pagination = PaginationInfo.builder()
            .page(0)
            .size(10)
            .totalElements((long) accounts.size())
            .totalPages(1)
            .hasNext(false)
            .hasPrevious(false)
            .build();
        return ApiResponse.of(PagedResponse.of(accounts, pagination));
    }

    @GetMapping("/{accountId}")
    public ApiResponse<AccountDetailDto> account(@PathVariable String accountId) {
        return ApiResponse.of(DummyAccountData.getAccountDetail(accountId));
    }

    @GetMapping("/{accountId}/holdings")
    public ApiResponse<PagedResponse<HoldingDto>> holdings(@PathVariable String accountId) {
        var holdings = DummyAccountData.getHoldings(accountId);
        PaginationInfo pagination = PaginationInfo.builder()
            .page(0)
            .size(10)
            .totalElements((long) holdings.size())
            .totalPages(1)
            .hasNext(false)
            .hasPrevious(false)
            .build();
        return ApiResponse.of(PagedResponse.of(holdings, pagination));
    }

    @PostMapping("/{accountId}/deposit")
    public ApiResponse<TransactionResponse> deposit(
            @PathVariable String accountId,
            @Valid @RequestBody DepositRequest request) {
        return ApiResponse.of(DummyAccountData.getDepositResponse(accountId, request.amount()));
    }

    @PostMapping("/{accountId}/withdraw")
    public ApiResponse<TransactionResponse> withdraw(
            @PathVariable String accountId,
            @Valid @RequestBody WithdrawRequest request) {
        return ApiResponse.of(DummyAccountData.getWithdrawResponse(accountId, request.amount()));
    }

    @GetMapping("/{accountId}/orders")
    public ApiResponse<PagedResponse<OrderDto>> orders(
            @PathVariable String accountId,
            @RequestParam(required = false) String status,
            @RequestParam(required = false) Long startAt,
            @RequestParam(required = false) Long endAt) {
        var orders = DummyAccountData.getOrders(accountId, status, startAt, endAt);
        PaginationInfo pagination = PaginationInfo.builder()
            .page(0)
            .size(10)
            .totalElements((long) orders.size())
            .totalPages(1)
            .hasNext(false)
            .hasPrevious(false)
            .build();
        return ApiResponse.of(PagedResponse.of(orders, pagination));
    }

    @GetMapping("/{accountId}/transactions")
    public ApiResponse<PagedResponse<TransactionDto>> transactions(
            @PathVariable String accountId,
            @RequestParam(required = false) Long startAt,
            @RequestParam(required = false) Long endAt) {
        var transactions = DummyAccountData.getTransactions(accountId, startAt, endAt);
        PaginationInfo pagination = PaginationInfo.builder()
            .page(0)
            .size(10)
            .totalElements((long) transactions.size())
            .totalPages(1)
            .hasNext(false)
            .hasPrevious(false)
            .build();
        return ApiResponse.of(PagedResponse.of(transactions, pagination));
    }
}
