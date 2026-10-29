package com.flab.stocktradingengine.account.worker.coordination;

/**
 * 맵 토픽 {@code matching-shard-map}의 값 하나 — 종목 slot을 담당하는 매칭 pod의 주문 입구 주소와
 * 그 종목 slot이 지금 이동 중인지. 둘을 한 객체에 담아야 sender thread가 volatile 읽기 한 번으로
 * 서로 맞는 주소와 표시를 본다.
 *
 * <p>값 형식: 평상시 {@code 주소}, 이동 중 {@code 주소;moving}. 주소는 호스트:포트가 아니라 Aeron 채널
 * 문자열 전체이다(예: {@code aeron:udp?endpoint=localhost:20020}). Aeron 채널 문자열은 구분자로
 * {@code |}·{@code =}·{@code ?}를 쓰고 {@code ;}는 쓰지 않아, 값이 {@code ;moving}으로 끝나는지만 본다.</p>
 */
public record MatchingSlotTarget(String endpoint, boolean moving) {

    private static final String MOVING_SUFFIX = ";moving";

    public MatchingSlotTarget {
        if (endpoint == null || endpoint.isBlank()) {
            throw new IllegalArgumentException("매칭 pod 주소가 비어 있다");
        }
    }

    public String encode() {
        return moving ? endpoint + MOVING_SUFFIX : endpoint;
    }

    public static MatchingSlotTarget parse(String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("배정 값이 비어 있다");
        }
        if (value.endsWith(MOVING_SUFFIX)) {
            String endpoint = value.substring(0, value.length() - MOVING_SUFFIX.length());
            return new MatchingSlotTarget(endpoint, true);
        }
        return new MatchingSlotTarget(value, false);
    }
}
