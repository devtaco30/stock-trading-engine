package com.flab.stocktradingengine.controller.order;

import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.flab.stocktradingengine.dto.common.ApiResponse;
import com.flab.stocktradingengine.dto.order.BuyOrderRequest;
import com.flab.stocktradingengine.dto.order.CancelOrderResponse;
import com.flab.stocktradingengine.dto.order.OrderResponse;
import com.flab.stocktradingengine.dto.order.SellOrderRequest;
import com.flab.stocktradingengine.dummy.DummyOrderData;

import jakarta.validation.Valid;

@RestController
@RequestMapping("/api/v1/orders")
public class OrderController {

    @PostMapping("/buy")
    public ApiResponse<OrderResponse> buy(@Valid @RequestBody BuyOrderRequest request) {
        return ApiResponse.of(DummyOrderData.getBuyOrderResponse(request));
    }

    @PostMapping("/sell")
    public ApiResponse<OrderResponse> sell(@Valid @RequestBody SellOrderRequest request) {
        return ApiResponse.of(DummyOrderData.getSellOrderResponse(request));
    }

    @DeleteMapping("/{orderId}")
    public ApiResponse<CancelOrderResponse> cancel(@PathVariable String orderId) {
        return ApiResponse.of(DummyOrderData.getCancelOrderResponse(orderId));
    }

    // 주문 내역 조회는 AccountController에 있음
}
