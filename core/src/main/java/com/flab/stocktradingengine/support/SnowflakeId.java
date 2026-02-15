package com.flab.stocktradingengine.support;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import org.hibernate.annotations.ValueGenerationType;

/**
 * PK가 아닌 필드에 Snowflake ID 생성 적용.
 * <p>Hibernate {@link org.hibernate.annotations.ValueGenerationType}으로, INSERT 시 {@link SnowflakeGenerator}가 Long 값을 채운다.</p>
 * <p>id(시퀀스 PK)와 별도로 두는 비즈니스 식별자(accountId 등)에 붙인다.</p>
 */
@ValueGenerationType(generatedBy = SnowflakeGenerator.class)
@Retention(RUNTIME)
@Target({ FIELD, METHOD })
public @interface SnowflakeId {
}
