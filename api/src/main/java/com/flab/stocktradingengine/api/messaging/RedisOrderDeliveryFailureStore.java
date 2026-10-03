package com.flab.stocktradingengine.api.messaging;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.data.redis.core.StringRedisTemplate;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * 전달에 끝내 실패한 주문을 Redis에 {@code order-delivery-failed:{requestId}} 키로 24시간 보관한다.
 *
 * <p>이 저장은 전달에 끝내 실패한 경로에서만 실행되고 정상 경로에는 영향이 없다. 읽는 코드와 조회 API는 두지 않는다(운영자가 Redis에서 직접
 * 보는 용도).</p>
 */
public class RedisOrderDeliveryFailureStore implements OrderDeliveryFailureStore {

    static final String KEY_PREFIX = "order-delivery-failed:";
    static final Duration TTL = Duration.ofHours(24);

    private final StringRedisTemplate stringRedisTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public RedisOrderDeliveryFailureStore(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    @Override
    public void put(PendingOrder order, String reason, int attempts, long failedAtEpochMillis) {
        Map<String, Object> value = new LinkedHashMap<>();
        value.put("accountId", order.accountId());
        value.put("stockCode", order.stockCode());
        value.put("side", order.side().name());
        value.put("price", order.price().toPlainString());
        value.put("quantity", order.quantity());
        value.put("reason", reason);
        value.put("attempts", attempts);
        value.put("failedAtEpochMillis", failedAtEpochMillis);
        try {
            stringRedisTemplate.opsForValue().set(KEY_PREFIX + order.requestId(), objectMapper.writeValueAsString(value), TTL);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("실패 기록 직렬화 실패: requestId=" + order.requestId(), e);
        }
    }
}
