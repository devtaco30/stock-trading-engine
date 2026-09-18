package com.flab.stocktradingengine.aeron;

import java.util.Optional;

/**
 * 계좌 샤딩 U1 — accountId를 맡은 워커의 Aeron endpoint를 찾는 조회 창구.
 *
 * <p>보내는 쪽({@code AeronAccountOrderSender})이 이 인터페이스에만 의존하게 해, 조회 방식이
 * {@link StaticShardDestinationResolver}(설정 파일의 슬롯 표)에서 U5의 동적 구현(워커들이 Kafka
 * 배정 결과를 발행하는 {@code account-shard-map} 토픽 구독)으로 바뀌어도 보내는 쪽 코드를 고치지
 * 않게 하는 것이 이 인터페이스의 유일한 존재 이유다.</p>
 */
public interface AccountDestinationResolver {

    /**
     * accountId를 맡은 워커가 <b>주문</b>을 받는 Aeron 채널. 맡은 워커가 없으면(아직 배정을 못
     * 받았거나 설정이 어긋난 상태) {@link Optional#empty()}.
     */
    Optional<String> orderEndpointFor(long accountId);

    /**
     * accountId를 맡은 워커가 <b>체결</b>을 받는 Aeron 채널. 워커는 주문과 체결을 서로 다른
     * 주소에서 받으므로 주문 채널과 다를 수 있다.
     */
    Optional<String> fillEndpointFor(long accountId);
}
