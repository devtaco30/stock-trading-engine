package com.flab.stocktradingengine.account.worker.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.function.IntPredicate;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.LifecycleProcessor;

import com.flab.stocktradingengine.account.worker.coordination.KafkaShardAssignment;

/**
 * 조정 모드({@code account-shard.coordination.enabled=true})로 계좌 워커의 샤딩 설정이 실제로
 * 컨텍스트에 올라오는지 확인한다.
 *
 * <p>이 테스트가 없어서 놓쳤던 것: 담당 슬롯 판정({@link IntPredicate})을 {@link
 * KafkaShardAssignment} 자신과 그것을 감싸기만 하는 빈이 둘 다 제공해, 이 타입을 주입받는
 * {@link AccountEngineConfig}에서 Spring이 어느 것을 넣을지 정하지 못하고 기동이 멈췄다. 앱을
 * 손으로 띄워 보고서야 찾았다. 그래서 여기서는 빈 개수를 직접 확인한다 — 후보가 둘이면
 * {@code hasSingleBean}이 깨진다.</p>
 *
 * <p>확인하지 않는 것: 실제 Kafka 컨슈머 그룹 조인은 여기서 일으키지 않는다. 생명주기 처리기를
 * 아무것도 안 하는 것으로 바꿔 끼워 {@link
 * com.flab.stocktradingengine.account.worker.lifecycle.KafkaShardAssignmentLifecycle}이 안 돌게
 * 한다 — 그룹 {@code account-shard-owners}에 테스트가 조인하면 돌고 있는 계좌 워커의 배정이
 * 흔들린다. 조인과 슬롯 분배는 {@code KafkaShardAssignmentIntegrationTest}가 확인한다.</p>
 */
class AccountShardOwnershipConfigTest {

    private static final String ORDER_CHANNEL = "aeron:udp?endpoint=localhost:29040";
    private static final String FILL_CHANNEL = "aeron:udp?endpoint=localhost:29060";

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
        .withUserConfiguration(ShardRoutingConfig.class, AccountShardOwnershipConfig.class)
        .withBean("lifecycleProcessor", LifecycleProcessor.class, NoOpLifecycleProcessor::new);

    @Test
    void 조정_모드로_뜨면_담당_슬롯_판정_빈이_하나만_남는다() {
        contextRunner
            .withPropertyValues(
                "account-shard.coordination.enabled=true",
                "shard-routing.slot-count=256",
                "spring.kafka.bootstrap-servers=localhost:9092",
                "account-worker.instance-id=test-context-boot",
                "transport.account-intake.channel=" + ORDER_CHANNEL,
                "transport.fill.channel=" + FILL_CHANNEL)
            .run(context -> {
                assertThat(context).hasNotFailed();
                assertThat(context).hasSingleBean(IntPredicate.class);
                assertThat(context.getBean(IntPredicate.class)).isInstanceOf(KafkaShardAssignment.class);
            });
    }

    @Test
    void 조정_모드는_칸_개수를_설정값_그대로_쓴다() {
        contextRunner
            .withPropertyValues(
                "account-shard.coordination.enabled=true",
                "shard-routing.slot-count=256",
                "spring.kafka.bootstrap-servers=localhost:9092",
                "account-worker.instance-id=test-context-boot",
                "transport.account-intake.channel=" + ORDER_CHANNEL,
                "transport.fill.channel=" + FILL_CHANNEL)
            .run(context -> assertThat(context.getBean(com.flab.stocktradingengine.aeron.SlotHasher.class).slotCount())
                .isEqualTo(256));
    }

    @Test
    void 정적_모드로_뜨면_설정에_적힌_담당_슬롯을_쓴다() {
        contextRunner
            .withPropertyValues(
                "shard-routing.slot-count=2",
                "shard-routing.shards[0].endpoint=" + ORDER_CHANNEL,
                "shard-routing.shards[0].slot-from=0",
                "shard-routing.shards[0].slot-to=0",
                "shard-routing.shards[1].endpoint=aeron:udp?endpoint=localhost:29041",
                "shard-routing.shards[1].slot-from=1",
                "shard-routing.shards[1].slot-to=1",
                "transport.account-intake.channel=" + ORDER_CHANNEL)
            .run(context -> {
                assertThat(context).hasSingleBean(IntPredicate.class);
                IntPredicate ownedSlots = context.getBean(IntPredicate.class);
                assertThat(ownedSlots.test(0)).isTrue();
                assertThat(ownedSlots.test(1)).isFalse();
            });
    }

    /** 컨텍스트가 뜰 때 SmartLifecycle을 아무것도 시작하지 않게 하는 처리기. */
    private static final class NoOpLifecycleProcessor implements LifecycleProcessor {

        @Override
        public void onRefresh() {
        }

        @Override
        public void onClose() {
        }

        @Override
        public void start() {
        }

        @Override
        public void stop() {
        }

        @Override
        public boolean isRunning() {
            return false;
        }
    }
}
