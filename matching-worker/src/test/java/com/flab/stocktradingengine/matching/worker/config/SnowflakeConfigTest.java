package com.flab.stocktradingengine.matching.worker.config;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import com.flab.stocktradingengine.matching.worker.id.MonotonicSnowflakeIdGenerator;
import com.flab.stocktradingengine.support.SnowflakeIdGenerator;
import com.flab.stocktradingengine.support.SnowflakeIdGeneratorHolder;

class SnowflakeConfigTest {

    @Test
    void 매칭_프로그램은_시계가_뒤로_가도_죽지_않는_발급기를_빈으로_등록한다() {
        SnowflakeIdGenerator generator = new SnowflakeConfig().snowflakeIdGenerator("2");

        assertThat(generator).isInstanceOf(MonotonicSnowflakeIdGenerator.class);
        assertThat(SnowflakeIdGeneratorHolder.get()).isSameAs(generator);
    }
}
