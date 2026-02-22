package com.flab.stocktradingengine.config;

import java.util.regex.Pattern;

import com.flab.stocktradingengine.support.SnowflakeIdGenerator;

/**
 * Snowflake 노드 ID(0~1023) 결정.
 * <p>설정값이 있으면 파싱, 없으면 호스트명/POD 이름 끝 숫자 추론 후 클램프.</p>
 */
public final class SnowflakeNodeIdResolver {

    private static final Pattern TRAILING_DIGITS = Pattern.compile("(\\d+)$");

    private SnowflakeNodeIdResolver() {
    }

    /**
     * 설정 문자열에서 노드 ID 결정.
     * <ul>
     *   <li>비어 있지 않고 숫자면 파싱 후 0~1023 클램프</li>
     *   <li>비어 있거나 비숫자면 HOSTNAME → POD_NAME → 로컬 호스트명 순으로 조회 후 끝 숫자 추론, 없으면 0</li>
     * </ul>
     */
    public static long resolve(String nodeIdConfig) {
        if (nodeIdConfig != null && !nodeIdConfig.isBlank()) {
            try {
                return clamp(Long.parseLong(nodeIdConfig.trim()));
            } catch (NumberFormatException ignored) {
                // "auto" 등 비숫자면 아래 자동 추론으로
            }
        }
        String host = System.getenv("HOSTNAME");
        if (host == null || host.isBlank()) {
            host = System.getenv("POD_NAME");
        }
        if (host == null || host.isBlank()) {
            try {
                host = java.net.InetAddress.getLocalHost().getHostName();
            } catch (Exception ignored) {
                host = "";
            }
        }
        var matcher = TRAILING_DIGITS.matcher(host);
        if (matcher.find()) {
            return clamp(Long.parseLong(matcher.group(1)));
        }
        return 0L;
    }

    private static long clamp(long value) {
        if (value < 0) return 0;
        return Math.min(value, SnowflakeIdGenerator.MAX_NODE_ID);
    }
}
