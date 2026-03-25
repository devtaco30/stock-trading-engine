package com.flab.stocktradingengine.account.exception;

import com.flab.stocktradingengine.exception.BusinessException;

/**
 * 계좌 자원(잔고·보유 수량)이 부족할 때 (400).
 * 매수 가능 금액 초과, 매도 수량 초과, 출금 잔액 부족 등에 사용한다.
 */
public class InsufficientResourceException extends BusinessException {

    public InsufficientResourceException(String message) {
        super("INSUFFICIENT_RESOURCE", message, 400);
    }
}
