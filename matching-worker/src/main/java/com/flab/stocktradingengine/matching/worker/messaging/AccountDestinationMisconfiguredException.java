package com.flab.stocktradingengine.matching.worker.messaging;

/**
 * 계좌 샤딩 U6 — account-shard-map이 가리키는 endpoint가 shard-routing.endpoints 풀에 없을 때
 * 던진다. 재시도해도 나아지지 않는 설정 오류라 즉시 fail-fast로 다뤄야 한다.
 *
 * <p>{@link IllegalArgumentException}·{@link IllegalStateException}이 아닌 별도 타입인 이유 —
 * {@code MatchingEventHandler.onEvent}는 그 둘만 잡아 "도메인 불변식 위반이니 이 이벤트만
 * 폐기"하고 계속 돈다. 이 예외를 그 둘로 던지면 설정 오류가 조용히 이벤트 하나 버림으로
 * 끝나버린다 — 체결은 매칭에서 이미 확정된 사실이라 계좌로 못 보내면 그대로 유실인데, 계속
 * 돌면 같은 오류로 유실이 반복된다. 이 타입은 그 catch를 피해 {@code MatchingExceptionHandler}
 * (진짜 fail-fast)까지 올라간다.</p>
 */
public class AccountDestinationMisconfiguredException extends RuntimeException {

    public AccountDestinationMisconfiguredException(String message) {
        super(message);
    }
}
