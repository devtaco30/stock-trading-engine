package com.flab.stocktradingengine.api.controller.order;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.flab.stocktradingengine.dto.common.ApiResponse;
import com.flab.stocktradingengine.dto.common.PagedResponse;
import com.flab.stocktradingengine.api.dto.trade.TradeDto;
import com.flab.stocktradingengine.api.resolver.CurrentUserId;
import com.flab.stocktradingengine.api.service.OrderApiService;

import lombok.RequiredArgsConstructor;

/**
 * 체결(매매) 내역 API. Trade = 체결 1건.
 */
@RestController
@RequestMapping("/api/v1/trades")
@RequiredArgsConstructor
public class TradeController {

    private final OrderApiService orderApiService;

    /** 체결 내역 조회 (본인 소유 계좌만). accountId 쿼리 필수. */
    @GetMapping
    public ApiResponse<PagedResponse<TradeDto>> trades(
            @CurrentUserId Long userId,
            @RequestParam Long accountId,
            @RequestParam(required = false) Long startAt,
            @RequestParam(required = false) Long endAt) {
        return ApiResponse.of(orderApiService.getTradesPaged(userId, accountId, startAt, endAt));
    }
}
