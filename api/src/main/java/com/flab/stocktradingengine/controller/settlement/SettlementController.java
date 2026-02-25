package com.flab.stocktradingengine.controller.settlement;

import java.util.List;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.flab.stocktradingengine.dto.common.ApiResponse;
import com.flab.stocktradingengine.dto.common.PagedResponse;
import com.flab.stocktradingengine.dto.common.PaginationInfo;
import com.flab.stocktradingengine.dto.settlement.ArrearsDto;
import com.flab.stocktradingengine.dto.settlement.RepaymentRequest;
import com.flab.stocktradingengine.dto.settlement.RepaymentResponse;
import com.flab.stocktradingengine.dto.settlement.UnpaidSettlementDto;
import com.flab.stocktradingengine.resolver.CurrentUserId;
import com.flab.stocktradingengine.service.SettlementApiService;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;

@RestController
@RequestMapping("/api/v1/accounts")
@RequiredArgsConstructor
public class SettlementController {

    private final SettlementApiService settlementApiService;

    @GetMapping("/{accountId}/unpaid")
    public ApiResponse<PagedResponse<UnpaidSettlementDto>> unpaidSettlements(
            @CurrentUserId Long userId,
            @PathVariable Long accountId) {
        List<UnpaidSettlementDto> list = settlementApiService.getUnpaidSettlements(userId, accountId);
        PaginationInfo pagination = PaginationInfo.builder()
            .page(0)
            .size(Math.max(1, list.size()))
            .totalElements((long) list.size())
            .totalPages(list.isEmpty() ? 0 : 1)
            .hasNext(false)
            .hasPrevious(false)
            .build();
        return ApiResponse.of(PagedResponse.of(list, pagination));
    }

    @GetMapping("/{accountId}/arrears")
    public ApiResponse<ArrearsDto> arrears(
            @CurrentUserId Long userId,
            @PathVariable Long accountId) {
        return ApiResponse.of(settlementApiService.getArrears(userId, accountId));
    }

    @PostMapping("/{accountId}/arrears/{arrearsId}/repay")
    public ApiResponse<RepaymentResponse> repayArrears(
            @CurrentUserId Long userId,
            @PathVariable Long accountId,
            @PathVariable String arrearsId,
            @Valid @RequestBody RepaymentRequest request) {
        return ApiResponse.of(settlementApiService.repay(userId, accountId, arrearsId, request));
    }
}
