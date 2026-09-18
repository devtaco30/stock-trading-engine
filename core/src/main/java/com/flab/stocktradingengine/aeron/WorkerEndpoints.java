package com.flab.stocktradingengine.aeron;

/**
 * 계좌 워커 한 대가 가진 Aeron 주소 묶음. 주문을 받는 주소와 체결을 받는 주소가 서로 다르기
 * 때문에 둘을 같이 싣는다.
 *
 * <p>배정 결과를 알리는 {@code account-shard-map} 토픽의 값이 이 형식이다. 주소를 하나만 싣던
 * 때에는 매칭 워커가 그 값(주문 주소)으로 체결을 보내서, 체결 수신 주소에서 기다리는 워커가
 * 아무것도 못 받았다.</p>
 *
 * <h3>형식</h3>
 * <pre>
 * order=aeron:udp?endpoint=localhost:20040
 * fill=aeron:udp?endpoint=localhost:20060
 * </pre>
 *
 * <p>줄 단위로 나누고 각 줄의 첫 등호까지만 이름으로 읽는다 — Aeron 채널 문자열 자체가 등호와
 * {@code |}를 포함하므로(예: {@code aeron:udp?endpoint=host:1|term-length=64k}) 그 문자들을
 * 구분자로 쓸 수 없다. 줄바꿈은 채널 문자열에 들어갈 수 없다.</p>
 */
public record WorkerEndpoints(String orderEndpoint, String fillEndpoint) {

    private static final String ORDER_KEY = "order=";
    private static final String FILL_KEY = "fill=";

    public WorkerEndpoints {
        if (orderEndpoint == null || orderEndpoint.isBlank()) {
            throw new IllegalArgumentException("주문 수신 주소가 비어 있다");
        }
        if (fillEndpoint == null || fillEndpoint.isBlank()) {
            throw new IllegalArgumentException("체결 수신 주소가 비어 있다");
        }
    }

    public String encode() {
        return ORDER_KEY + orderEndpoint + "\n" + FILL_KEY + fillEndpoint;
    }

    /**
     * 토픽에 실린 값을 읽는다. 주소가 하나만 적혀 있으면(주소를 둘로 싣기 전에 발행된 값) 주문·체결
     * 둘 다 그 주소로 본다 — compacted 토픽이라 옛 값이 남아 있을 수 있고, 그때의 동작을 그대로
     * 재현하는 것이 값을 못 읽어 목적지를 잃는 것보다 낫다.
     */
    public static WorkerEndpoints parse(String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("배정 값이 비어 있다");
        }
        String order = null;
        String fill = null;
        for (String line : value.split("\n")) {
            if (line.startsWith(ORDER_KEY)) {
                order = line.substring(ORDER_KEY.length());
            } else if (line.startsWith(FILL_KEY)) {
                fill = line.substring(FILL_KEY.length());
            }
        }
        if (order == null && fill == null) {
            return new WorkerEndpoints(value, value);
        }
        if (order == null || fill == null) {
            throw new IllegalArgumentException("배정 값에 주소 하나가 빠져 있다: " + value);
        }
        return new WorkerEndpoints(order, fill);
    }
}
