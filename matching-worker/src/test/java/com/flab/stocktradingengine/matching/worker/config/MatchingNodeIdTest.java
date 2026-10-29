package com.flab.stocktradingengine.matching.worker.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import com.flab.stocktradingengine.support.SnowflakeIdGenerator;

/** 매칭 shard의 서버 번호(Snowflake nodeId)는 신원 이름 끝 순번에 6을 더한 값이다. */
class MatchingNodeIdTest {

    private static final int NODE_SHIFT = 12;
    private static final long NODE_MASK = 0x3FF;

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
        .withUserConfiguration(SnowflakeConfig.class);

    @Test
    void 신원_이름_끝_순번에_6을_더한_값이_서버_번호다() {
        assertThat(SnowflakeConfig.nodeIdFromInstanceId("matching-0")).isEqualTo(6L);
        assertThat(SnowflakeConfig.nodeIdFromInstanceId("matching-3")).isEqualTo(9L);
        assertThat(SnowflakeConfig.nodeIdFromInstanceId("matching-7")).isEqualTo(13L);
    }

    @Test
    void 끝에_숫자가_없는_이름이면_기동_예외다() {
        assertThatThrownBy(() -> SnowflakeConfig.nodeIdFromInstanceId("matching-worker"))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("matching-worker");
        contextRunner
            .withPropertyValues("matching-worker.instance-id=matching-worker")
            .run(context -> assertThat(context).hasFailed());
    }

    @Test
    void 서버_번호는_group_instance_id로_쓰이는_값과_같은_설정에서_나온다() {
        contextRunner
            .withPropertyValues("matching-worker.instance-id=matching-3")
            .run(context -> {
                SnowflakeIdGenerator generator = context.getBean(SnowflakeIdGenerator.class);
                long id = generator.nextId();
                assertThat((id >> NODE_SHIFT) & NODE_MASK).isEqualTo(9L);
            });
    }

    @Test
    void 신원_이름이_없으면_예전처럼_snowflake_node_id_설정을_쓴다() {
        contextRunner
            .withPropertyValues("snowflake.node-id=11")
            .run(context -> {
                long id = context.getBean(SnowflakeIdGenerator.class).nextId();
                assertThat((id >> NODE_SHIFT) & NODE_MASK).isEqualTo(11L);
            });
    }

    @Test
    void 조정_모드가_켜져_있는데_신원_이름이_없으면_기동_예외다() {
        contextRunner
            .withPropertyValues("matching-shard.coordination.enabled=true", "snowflake.node-id=11")
            .run(context -> assertThat(context).hasFailed());
    }

    @Test
    void 조정_모드가_켜져_있고_신원_이름이_있으면_정상_기동한다() {
        contextRunner
            .withPropertyValues("matching-shard.coordination.enabled=true", "matching-worker.instance-id=matching-1")
            .run(context -> assertThat(context).hasNotFailed());
    }

    @Test
    void 조정_모드가_꺼져_있으면_신원_이름이_없어도_기존_동작이다() {
        contextRunner
            .withPropertyValues("matching-shard.coordination.enabled=false", "snowflake.node-id=11")
            .run(context -> assertThat(context).hasNotFailed());
    }

    @Test
    void application_yml에_node_id_기본값이_남아_있지_않다() throws Exception {
        try (InputStream stream = getClass().getResourceAsStream("/application.yml")) {
            String yml = new String(stream.readAllBytes(), StandardCharsets.UTF_8);
            assertThat(yml).doesNotContain("node-id");
        }
    }
}
