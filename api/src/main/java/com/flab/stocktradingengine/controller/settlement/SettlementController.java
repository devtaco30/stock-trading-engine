package com.flab.stocktradingengine.controller.settlement;

import com.flab.stocktradingengine.dto.common.ApiResponse;
import com.flab.stocktradingengine.dto.common.PagedResponse;
import com.flab.stocktradingengine.dto.common.PaginationInfo;
import com.flab.stocktradingengine.dto.settlement.ArrearsDto;
import com.flab.stocktradingengine.dto.settlement.RepaymentRequest;
import com.flab.stocktradingengine.dto.settlement.RepaymentResponse;
import com.flab.stocktradingengine.dto.settlement.UnpaidSettlementDto;
import com.flab.stocktradingengine.dummy.DummySettlementData;
import jakarta.validation.Valid;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/accounts")
public class SettlementController {

    @GetMapping("/{accountId}/unpaid")
    public ApiResponse<PagedResponse<UnpaidSettlementDto>> unpaidSettlements(
            @PathVariable String accountId) {
        var unpaidSettlements = DummySettlementData.getUnpaidSettlements(accountId);
        PaginationInfo pagination = PaginationInfo.builder()
            .page(0)
            .size(10)
            .totalElements((long) unpaidSettlements.size())
            .totalPages(1)
            .hasNext(false)
            .hasPrevious(false)
            .build();
        return ApiResponse.of(PagedResponse.of(unpaidSettlements, pagination));
    }

    @GetMapping("/{accountId}/arrears")
    public ApiResponse<ArrearsDto> arrears(@PathVariable String accountId) {
        return ApiResponse.of(DummySettlementData.getArrears(accountId));
    }

    @PostMapping("/{accountId}/arrears/{arrearsId}/repay")
    public ApiResponse<RepaymentResponse> repayArrears(
            @PathVariable String accountId,
            @PathVariable String arrearsId,
            @Valid @RequestBody RepaymentRequest request) {
        return ApiResponse.of(DummySettlementData.getRepaymentResponse(accountId, arrearsId, request));
    }
}
