package com.flab.stocktradingengine.support;

import java.util.EnumSet;

import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.generator.BeforeExecutionGenerator;
import org.hibernate.generator.EventType;
import org.hibernate.generator.EventTypeSets;

/**
 * Hibernate 식별자 생성기. INSERT 직전에 {@link SnowflakeIdGeneratorHolder}를 통해 Snowflake ID를 발급한다.
 */
public class SnowflakeGenerator implements BeforeExecutionGenerator {

    @Override
    public Object generate(SharedSessionContractImplementor session, Object owner, Object currentValue, EventType eventType) {
        return SnowflakeIdGeneratorHolder.get().nextId();
    }

    @Override
    public EnumSet<EventType> getEventTypes() {
        return EventTypeSets.INSERT_ONLY;
    }
}
