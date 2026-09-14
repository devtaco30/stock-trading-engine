package com.flab.stocktradingengine.api.controller.order;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.flab.stocktradingengine.dto.common.ApiResponse;
import com.flab.stocktradingengine.api.dto.order.BuyOrderRequest;
import com.flab.stocktradingengine.api.dto.order.OrderAcceptedResponse;
import com.flab.stocktradingengine.api.dto.order.SellOrderRequest;
import com.flab.stocktradingengine.api.resolver.CurrentUserId;
import com.flab.stocktradingengine.api.service.OrderV2ApiService;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;

/**
 * fork5, U1a — v2 주문 게이트웨이 뼈대. 인증·검증(계좌 소유·가격 제한폭)만 하고 202를 반환한다
 * — Aeron 발신은 U1b에서 붙는다. v1 {@link OrderController}와 같은 결이다.
 */
@RestController
@RequestMapping("/api/v2/orders")
@RequiredArgsConstructor
public class OrderV2Controller {

    private final OrderV2ApiService orderV2ApiService;

    /** 매수 주문 접수. 202 Accepted 반환. */
    @PostMapping("/buy")
    public ResponseEntity<ApiResponse<OrderAcceptedResponse>> buy(
            @CurrentUserId Long userId,
            @Valid @RequestBody BuyOrderRequest request) {
        orderV2ApiService.placeBuyOrder(userId, request);
        return ResponseEntity.accepted().body(ApiResponse.of(new OrderAcceptedResponse("주문 접수")));
    }

    /** 매도 주문 접수. 202 Accepted 반환. */
    @PostMapping("/sell")
    public ResponseEntity<ApiResponse<OrderAcceptedResponse>> sell(
            @CurrentUserId Long userId,
            @Valid @RequestBody SellOrderRequest request) {
        orderV2ApiService.placeSellOrder(userId, request);
        return ResponseEntity.accepted().body(ApiResponse.of(new OrderAcceptedResponse("주문 접수")));
    }
}
