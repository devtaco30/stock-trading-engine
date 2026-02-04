package com.flab.stocktradingengine.dummy;

import com.flab.stocktradingengine.dto.order.BuyOrderRequest;
import com.flab.stocktradingengine.dto.order.CancelOrderResponse;
import com.flab.stocktradingengine.dto.order.OrderResponse;
import com.flab.stocktradingengine.dto.order.SellOrderRequest;

import java.math.BigDecimal;

/**
 * 주문 관련 더미 데이터
 * 
 * <p>주문 생성, 취소에 대한 더미 응답을 제공합니다.
 */
public class DummyOrderData {

    /**
     * 매수 주문 더미 응답
     * 
     * <p>주문 금액의 40%를 증거금으로 계산하여 반환합니다.
     * 지정가 주문의 경우 주문가 × 수량으로 계산하고,
     * 시장가 주문의 경우 0으로 설정합니다 (실제로는 체결가 기준으로 계산됨).
     * 
     * @param request 매수 주문 요청 (계좌 ID, 종목 코드, 주문 유형, 가격, 수량)
     * @return 주문 응답 (주문 ID, 상태 PENDING, 주문 시각, 홀딩된 증거금)
     */
    public static OrderResponse getBuyOrderResponse(BuyOrderRequest request) {
        // 주문 금액 계산 (지정가: 주문가 × 수량, 시장가: 0)
        BigDecimal orderAmount = request.price() != null 
            ? request.price().multiply(new BigDecimal(request.quantity()))
            : new BigDecimal("0"); // 시장가는 나중에 체결가로 계산
        
        // 증거금 계산 (주문 금액 × 40%)
        BigDecimal heldMargin = orderAmount.multiply(new BigDecimal("0.4"));
        
        return OrderResponse.builder()
            .orderId("order-" + System.currentTimeMillis())
            .status("PENDING")  // 주문 대기 상태 (실제로는 체결 처리 후 FILLED로 변경됨)
            .orderAt(System.currentTimeMillis())
            .heldMargin(heldMargin)  // 홀딩된 증거금
            .build();
    }

    /**
     * 매도 주문 더미 응답
     * 
     * <p>매도 주문은 증거금이 필요하지 않으므로 heldMargin은 null입니다.
     * 
     * @param request 매도 주문 요청 (계좌 ID, 종목 코드, 주문 유형, 가격, 수량)
     * @return 주문 응답 (주문 ID, 상태 PENDING, 주문 시각, 증거금 null)
     */
    public static OrderResponse getSellOrderResponse(SellOrderRequest request) {
        return OrderResponse.builder()
            .orderId("order-" + System.currentTimeMillis())
            .status("PENDING")  // 주문 대기 상태
            .orderAt(System.currentTimeMillis())
            .heldMargin(null)        // 매도는 증거금 없음
            .build();
    }

    /**
     * 주문 취소 더미 응답
     * 
     * <p>매수 주문 취소 시 홀딩되어 있던 증거금을 반환합니다.
     * 더미 데이터로는 고정값 2,820,000원을 반환합니다.
     * (실제로는 주문 시 홀딩된 증거금 금액을 반환해야 함)
     * 
     * @param orderId 취소할 주문 ID
     * @return 취소 응답 (주문 ID, 반환된 증거금)
     */
    public static CancelOrderResponse getCancelOrderResponse(String orderId) {
        // 매수 주문 취소 시 증거금 반환 (더미로 2,820,000원)
        // 실제로는 주문 시 홀딩된 증거금 금액을 조회하여 반환해야 함
        return CancelOrderResponse.builder()
            .orderId(orderId)
            .returnedMargin(new BigDecimal("2820000"))  // 반환된 증거금 (더미 값)
            .build();
    }
}
