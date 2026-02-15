package com.flab.stocktradingengine.exception;

/**
 * 권한 없음 (403).
 * <p>인증은 되었으나 해당 리소스에 대한 접근 권한이 없는 경우 발생한다.</p>
 */
public class ForbiddenException extends RuntimeException {

    private final String errorCode;
    private final int httpStatus;

    public ForbiddenException(String errorCode, String message, int httpStatus) {
        super(message);
        this.errorCode = errorCode;
        this.httpStatus = httpStatus;
    }

    public String getErrorCode() {
        return errorCode;
    }

    public int getHttpStatus() {
        return httpStatus;
    }

    public static ForbiddenException notOwnerOfAccount() {
        return new ForbiddenException("FORBIDDEN", "본인의 계좌만 조회할 수 있습니다.", 403);
    }

    /** 계좌가 정상(ACTIVE)이 아니라 거래 불가 상태일 때 */
    public static ForbiddenException accountNotActive() {
        return new ForbiddenException("FORBIDDEN", "거래가 중지되거나 정지된 계좌입니다.", 403);
    }
}
