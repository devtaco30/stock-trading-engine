package com.flab.stocktradingengine.exception;

/**
 * 요청한 리소스를 찾을 수 없을 때 (404).
 * Order, Account, Holding, Stock 등 모든 도메인에서 공통으로 사용한다.
 */
public class ResourceNotFoundException extends BusinessException {

    public ResourceNotFoundException(String message) {
        super("RESOURCE_NOT_FOUND", message, 404);
    }
}
