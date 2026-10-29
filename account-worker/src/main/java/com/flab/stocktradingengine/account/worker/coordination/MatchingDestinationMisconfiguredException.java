package com.flab.stocktradingengine.account.worker.coordination;

/**
 * 맵 토픽이 주소 풀에 없는 매칭 pod 주소를 가리킬 때 {@link MatchingDestinationResolver}가 던진다.
 * 재시도해도 나아지지 않는 설정 어긋남이라 즉시 fail-fast로 다뤄야 한다.
 *
 * <p>{@link IllegalArgumentException}·{@link IllegalStateException}이 아닌 별도 타입인 이유 —
 * 호출하는 쪽이 도메인 불변식 위반(이벤트 하나 폐기)을 그 둘로 잡아 계속 도는 경로가 있으면,
 * 이 예외가 같은 catch에 묻혀 설정 오류가 조용히 지나간다.</p>
 */
public class MatchingDestinationMisconfiguredException extends RuntimeException {

    public MatchingDestinationMisconfiguredException(String message) {
        super(message);
    }
}
