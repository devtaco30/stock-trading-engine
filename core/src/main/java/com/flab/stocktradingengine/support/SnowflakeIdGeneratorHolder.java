package com.flab.stocktradingengine.support;

/**
 * Hibernate Generator가 Spring 빈 {@link SnowflakeIdGenerator}를 참조하기 위한 홀더.
 * <p>애플리케이션 기동 시(예: {@code SnowflakeConfig})에서 빈 생성 후 {@link #set(SnowflakeIdGenerator)}로 등록한다.</p>
 */
public final class SnowflakeIdGeneratorHolder {

    private static volatile SnowflakeIdGenerator instance;

    private SnowflakeIdGeneratorHolder() {
    }

    public static void set(SnowflakeIdGenerator generator) {
        instance = generator;
    }

    public static SnowflakeIdGenerator get() {
        SnowflakeIdGenerator g = instance;
        if (g == null) {
            throw new IllegalStateException(
                "SnowflakeIdGenerator not set. Ensure SnowflakeConfig runs and calls SnowflakeIdGeneratorHolder.set(...).");
        }
        return g;
    }
}
