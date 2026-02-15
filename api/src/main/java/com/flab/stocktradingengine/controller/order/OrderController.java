package com.flab.stocktradingengine.controller.order;

import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.flab.stocktradingengine.dto.common.ApiResponse;
import com.flab.stocktradingengine.dto.common.PagedResponse;
import com.flab.stocktradingengine.dto.order.BuyOrderRequest;
import com.flab.stocktradingengine.dto.order.CancelOrderResponse;
import com.flab.stocktradingengine.dto.order.OrderDto;
import com.flab.stocktradingengine.dto.order.OrderResponse;
import com.flab.stocktradingengine.dto.order.SellOrderRequest;
import com.flab.stocktradingengine.resolver.CurrentUserId;
import com.flab.stocktradingengine.service.OrderApiService;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;

/**
 * 주문 API.
 * <p>인증은 Interceptor(토큰 → userId), 계좌 소유·상태 검증은 OrderApiService에서 수행.</p>
 */
@RestController
@RequestMapping("/api/v1/orders")
@RequiredArgsConstructor
public class OrderController {

    private final OrderApiService orderApiService;

    /** 매수 주문 접수. 계좌 소유·ACTIVE 검증 후 접수. */
    @PostMapping("/buy")
    public ApiResponse<OrderResponse> buy(@CurrentUserId Long userId, @Valid @RequestBody BuyOrderRequest request) {
        return ApiResponse.of(orderApiService.placeBuyOrder(userId, request));
    }

    /** 매도 주문 접수. 계좌 소유·ACTIVE 검증 후 접수. */
    @PostMapping("/sell")
    public ApiResponse<OrderResponse> sell(@CurrentUserId Long userId, @Valid @RequestBody SellOrderRequest request) {
        return ApiResponse.of(orderApiService.placeSellOrder(userId, request));
    }

    /** 주문 취소. 해당 주문의 계좌 소유·ACTIVE 검증 후 취소. */
    @DeleteMapping("/{orderId}")
    public ApiResponse<CancelOrderResponse> cancel(@CurrentUserId Long userId, @PathVariable Long orderId) {
        return ApiResponse.of(orderApiService.cancelOrder(userId, orderId));
    }

    /** 주문 내역 조회 (본인 소유 계좌만). accountId 쿼리 필수. */
    @GetMapping
    public ApiResponse<PagedResponse<OrderDto>> orders(
            @CurrentUserId Long userId,
            @RequestParam String accountId,
            @RequestParam(required = false) String status,
            @RequestParam(required = false) Long startAt,
            @RequestParam(required = false) Long endAt) {
        return ApiResponse.of(orderApiService.getOrdersPaged(userId, accountId, status, startAt, endAt));
    }
}
