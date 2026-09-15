package com.flab.stocktradingengine.matching.worker.config;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import com.flab.stocktradingengine.aeron.ShardRoutingTable;

/**
 * fork3, Unit 1 — {@code shard-routing.*} 설정이 {@link ShardRoutingTable} 빈으로 정확히
 * 바인딩되는지, 슬롯 커버리지가 깨진 설정이 컨텍스트 기동 자체를 막는지(fail-fast) 확인한다.
 */
class ShardRoutingConfigTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
        .withUserConfiguration(ShardRoutingConfig.class);

    @Test
    void 완전히_커버된_설정은_ShardRoutingTable_빈을_만든다() {
        contextRunner
            .withPropertyValues(
                "shard-routing.slot-count=2",
                "shard-routing.shards[0].endpoint=aeron:udp?endpoint=localhost:6001",
                "shard-routing.shards[0].slot-from=0",
                "shard-routing.shards[0].slot-to=0",
                "shard-routing.shards[1].endpoint=aeron:udp?endpoint=localhost:6002",
                "shard-routing.shards[1].slot-from=1",
                "shard-routing.shards[1].slot-to=1"
            )
            .run(context -> {
                assertThat(context).hasSingleBean(ShardRoutingTable.class);
                ShardRoutingTable table = context.getBean(ShardRoutingTable.class);
                assertThat(table.endpointFor(90001L)).isIn(
                    "aeron:udp?endpoint=localhost:6001", "aeron:udp?endpoint=localhost:6002");
            });
    }

    @Test
    void shards_설정이_아예_없으면_기존_단일_채널로_폴백한다() {
        contextRunner.run(context -> {
            assertThat(context).hasSingleBean(ShardRoutingTable.class);
            ShardRoutingTable table = context.getBean(ShardRoutingTable.class);
            assertThat(table.endpointFor(90001L)).isEqualTo(ShardRoutingConfig.DEFAULT_FILL_CHANNEL);
            assertThat(table.endpointFor(1L)).isEqualTo(ShardRoutingConfig.DEFAULT_FILL_CHANNEL);
        });
    }

    @Test
    void 슬롯에_갭이_있는_설정은_기동을_실패시킨다() {
        contextRunner
            .withPropertyValues(
                "shard-routing.slot-count=256",
                "shard-routing.shards[0].endpoint=aeron:udp?endpoint=localhost:6001",
                "shard-routing.shards[0].slot-from=0",
                "shard-routing.shards[0].slot-to=100",
                "shard-routing.shards[1].endpoint=aeron:udp?endpoint=localhost:6002",
                "shard-routing.shards[1].slot-from=150",
                "shard-routing.shards[1].slot-to=255"
            )
            .run(context -> assertThat(context).hasFailed());
    }
}
