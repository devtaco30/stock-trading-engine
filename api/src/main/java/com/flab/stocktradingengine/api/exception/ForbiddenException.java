package com.flab.stocktradingengine.api.exception;

import com.flab.stocktradingengine.exception.BusinessException;

/**
 * 권한 없음 (403).
 * 인증은 되었으나 해당 리소스에 대한 접근 권한이 없는 경우 발생한다.
 */
public class ForbiddenException extends BusinessException {

    public ForbiddenException(String message) {
        super("FORBIDDEN", message, 403);
    }

    public static ForbiddenException notOwnerOfAccount() {
        return new ForbiddenException("본인의 계좌만 조회할 수 있습니다.");
    }

    public static ForbiddenException accountNotActive() {
        return new ForbiddenException("거래가 중지되거나 정지된 계좌입니다.");
    }
}
