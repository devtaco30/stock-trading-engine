package com.flab.stocktradingengine.matching.worker.coordination;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * 호가창 이동(X→Z)에서 매칭 shard들이 서로 보내는 주소 풀. 설정 {@code matching-shard.handover.endpoints}의 값으로,
 * 매칭 pod의 주문 입구 주소(맵 토픽에 올라가는 문자열과 같은 값, {@code transport.matching-intake.channel})를 모두 적는다.
 * 자기 주소를 포함해도 되고, 자기 자신에게는 Publication을 열지 않는다. 최대 {@link #MAX_ENDPOINTS}개이고, 풀에 없는
 * 주소가 맵이나 요청에 나오면 바로 실패한다(10-2절, 11-13절 3번).
 */
public final class HandoverEndpointPool {

    public static final int MAX_ENDPOINTS = 8;

    private final Set<String> endpoints;
    private final String ownEndpoint;

    public HandoverEndpointPool(List<String> configuredEndpoints, String ownEndpoint) {
        if (configuredEndpoints.size() > MAX_ENDPOINTS) {
            throw new IllegalStateException("matching-shard.handover.endpoints는 최대 " + MAX_ENDPOINTS + "개다: "
                + configuredEndpoints.size());
        }
        Set<String> unique = new LinkedHashSet<>();
        for (String endpoint : configuredEndpoints) {
            if (endpoint == null || endpoint.isBlank()) {
                throw new IllegalStateException("matching-shard.handover.endpoints에 빈 주소가 있다");
            }
            if (!unique.add(endpoint)) {
                throw new IllegalStateException("matching-shard.handover.endpoints에 같은 주소가 두 번 있다: " + endpoint);
            }
        }
        this.endpoints = Set.copyOf(unique);
        this.ownEndpoint = ownEndpoint;
    }

    /** Publication을 열 상대 주소들. 자기 주소는 뺀다. */
    public List<String> peers() {
        return endpoints.stream().filter(endpoint -> !endpoint.equals(ownEndpoint)).sorted().toList();
    }

    /**
     * 풀에 있는 주소를 돌려주고, 없으면 실패한다.
     *
     * @throws IllegalStateException 풀에 없는 주소일 때
     */
    public String requireInPool(String endpoint) {
        if (!endpoints.contains(endpoint)) {
            throw new IllegalStateException("호가창 이동 주소 풀에 없는 주소다: " + endpoint + " 풀=" + endpoints);
        }
        return endpoint;
    }

    public boolean isOwn(String endpoint) {
        return endpoint.equals(ownEndpoint);
    }
}
