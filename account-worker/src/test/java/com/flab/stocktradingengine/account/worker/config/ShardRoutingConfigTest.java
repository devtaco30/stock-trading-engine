package com.flab.stocktradingengine.account.worker.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Set;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import com.flab.stocktradingengine.aeron.ShardRoutingTable;

/**
 * I8 U2 — {@code shard-routing.*}가 {@link ShardRoutingTable} 빈으로 바인딩되는지, 이 워커
 * 자신의 인테이크 채널({@code transport.account-intake.channel})과 일치하는 슬롯 범위를
 * {@link ShardRoutingConfig.OwnedShard}로 확정하는지, 일치하는 범위가 없으면(설정 어긋남)
 * 기동을 막는지(fail-fast) 확인한다.
 */
class ShardRoutingConfigTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
        .withUserConfiguration(ShardRoutingConfig.class);

    @Test
    void shards_설정이_없으면_전체를_담당한다() {
        contextRunner.run(context -> {
            assertThat(context).hasSingleBean(ShardRoutingTable.class);
            ShardRoutingConfig.OwnedShard owned = context.getBean(ShardRoutingConfig.OwnedShard.class);
            assertThat(owned.slots()).isEqualTo(Set.of(0));
        });
    }

    @Test
    void 자기_인테이크_채널과_일치하는_슬롯_범위를_담당으로_확정한다() {
        contextRunner
            .withPropertyValues(
                "transport.account-intake.channel=aeron:udp?endpoint=localhost:20040",
                "shard-routing.slot-count=2",
                "shard-routing.shards[0].endpoint=aeron:udp?endpoint=localhost:20040",
                "shard-routing.shards[0].slot-from=0",
                "shard-routing.shards[0].slot-to=0",
                "shard-routing.shards[1].endpoint=aeron:udp?endpoint=localhost:20041",
                "shard-routing.shards[1].slot-from=1",
                "shard-routing.shards[1].slot-to=1"
            )
            .run(context -> {
                ShardRoutingConfig.OwnedShard owned = context.getBean(ShardRoutingConfig.OwnedShard.class);
                assertThat(owned.slots()).isEqualTo(Set.of(0));
            });
    }

    @Test
    void 슬롯_범위가_여러개면_그_범위_전체가_담당_슬롯_집합이_된다() {
        contextRunner
            .withPropertyValues(
                "transport.account-intake.channel=aeron:udp?endpoint=localhost:20040",
                "shard-routing.slot-count=4",
                "shard-routing.shards[0].endpoint=aeron:udp?endpoint=localhost:20040",
                "shard-routing.shards[0].slot-from=0",
                "shard-routing.shards[0].slot-to=2",
                "shard-routing.shards[1].endpoint=aeron:udp?endpoint=localhost:20041",
                "shard-routing.shards[1].slot-from=3",
                "shard-routing.shards[1].slot-to=3"
            )
            .run(context -> {
                ShardRoutingConfig.OwnedShard owned = context.getBean(ShardRoutingConfig.OwnedShard.class);
                assertThat(owned.slots()).isEqualTo(Set.of(0, 1, 2));
            });
    }

    @Test
    void 자기_인테이크_채널이_shard_설정에_없으면_기동을_실패시킨다() {
        contextRunner
            .withPropertyValues(
                "transport.account-intake.channel=aeron:udp?endpoint=localhost:29999",
                "shard-routing.slot-count=2",
                "shard-routing.shards[0].endpoint=aeron:udp?endpoint=localhost:20040",
                "shard-routing.shards[0].slot-from=0",
                "shard-routing.shards[0].slot-to=0",
                "shard-routing.shards[1].endpoint=aeron:udp?endpoint=localhost:20041",
                "shard-routing.shards[1].slot-from=1",
                "shard-routing.shards[1].slot-to=1"
            )
            .run(context -> assertThat(context).hasFailed());
    }
}
