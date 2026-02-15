package com.flab.stocktradingengine.support;

import com.relops.snowflake.Snowflake;

/**
 * Snowflake 스타일 64비트 ID 생성기 (com.relops:snowflake 라이브러리 위임).
 * <p>분산 환경에서 노드별 고유 ID 보장, 시간순 정렬 가능.</p>
 * <p>노드 ID는 0~1023. 단일 서버는 0, 다중 서버 시 서버별로 고유 값 설정.</p>
 */
public class SnowflakeIdGenerator {

    /** Snowflake 스펙상 노드 ID 최대값 (10비트 → 1023). */
    public static final int MAX_NODE_ID = 1023;

    private final Snowflake snowflake;

    /**
     * @param nodeId 노드/서버 구분자 (0 ~ 1023). 범위 밖이면 0 또는 1023으로 클램프.
     */
    public SnowflakeIdGenerator(long nodeId) {
        // 노드 ID가 0보다 작으면 0, 1023보다 크면 1023, 그 외에는 노드 ID로 설정
        int node = nodeId < 0 ? 0 : (nodeId > MAX_NODE_ID ? MAX_NODE_ID : (int) nodeId);
        this.snowflake = new Snowflake(node);
    }

    /**
     * 스레드 안전하게 다음 ID 발급.
     * <p>relops Snowflake가 동시 호출에 안전하다는 문서가 없어, 동기화로 보장.</p>
     */
    public synchronized long nextId() {
        return snowflake.next();
    }
}
