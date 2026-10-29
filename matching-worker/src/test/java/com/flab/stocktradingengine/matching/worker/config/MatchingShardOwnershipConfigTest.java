package com.flab.stocktradingengine.matching.worker.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.function.IntPredicate;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.LifecycleProcessor;

import com.flab.stocktradingengine.aeron.StockSlotHasher;
import com.flab.stocktradingengine.matching.worker.coordination.KafkaMatchingShardAssignment;

/**
 * 매칭 워커의 담당 종목 slot 판정 빈이 스위치({@code matching-shard.coordination.enabled})에 따라
 * 정확히 하나만 등록되는지 확인한다.
 *
 * <p>조정 모드에서도 Kafka 그룹 조인은 일으키지 않는다. 생명주기 처리기를 아무것도 시작하지 않는
 * 것으로 바꿔 끼워, 돌고 있는 매칭 워커의 배정에 테스트가 끼어들지 않게 한다. 조인과 종목 slot
 * 분배는 {@code KafkaMatchingShardAssignmentIntegrationTest}가 확인한다.</p>
 */
class MatchingShardOwnershipConfigTest {

    private static final int STOCK_SLOT_COUNT = 64;

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
        .withUserConfiguration(MatchingShardOwnershipConfig.class)
        .withBean("lifecycleProcessor", LifecycleProcessor.class, NoOpLifecycleProcessor::new);

    @Test
    void 스위치가_꺼져_있으면_모든_종목_slot이_담당이다() {
        contextRunner.run(context -> {
            assertThat(context).hasNotFailed();
            IntPredicate ownedSlots = context.getBean(IntPredicate.class);
            for (int slot = 0; slot < STOCK_SLOT_COUNT; slot++) {
                assertThat(ownedSlots.test(slot)).as("slot=" + slot).isTrue();
            }
            assertThat(context.getBean(StockSlotHasher.class).stockSlotCount()).isEqualTo(STOCK_SLOT_COUNT);
        });
    }

    @Test
    void 스위치가_켜져_있고_배정_전이면_모든_종목_slot이_담당이_아니다() {
        contextRunner
            .withPropertyValues(
                "matching-shard.coordination.enabled=true",
                "spring.kafka.bootstrap-servers=localhost:9092",
                "matching-worker.instance-id=test-context-boot")
            .run(context -> {
                assertThat(context).hasNotFailed();
                IntPredicate ownedSlots = context.getBean(IntPredicate.class);
                assertThat(ownedSlots).isInstanceOf(KafkaMatchingShardAssignment.class);
                for (int slot = 0; slot < STOCK_SLOT_COUNT; slot++) {
                    assertThat(ownedSlots.test(slot)).as("slot=" + slot).isFalse();
                }
                assertThat(context.getBean(StockSlotHasher.class).stockSlotCount()).isEqualTo(STOCK_SLOT_COUNT);
            });
    }

    @Test
    void 스위치와_상관없이_담당_판정_빈은_하나만_등록된다() {
        contextRunner.run(context -> assertThat(context).hasSingleBean(IntPredicate.class));
        contextRunner
            .withPropertyValues(
                "matching-shard.coordination.enabled=true",
                "spring.kafka.bootstrap-servers=localhost:9092",
                "matching-worker.instance-id=test-context-boot")
            .run(context -> assertThat(context).hasSingleBean(IntPredicate.class));
    }

    @Test
    void instance_id가_비어_있으면_자기_주문_입구_주소가_신원으로_쓰인다() {
        String orderEndpoint = "aeron:udp?endpoint=localhost:20020";

        assertThat(MatchingShardOwnershipConfig.resolveInstanceId("", orderEndpoint)).isEqualTo(orderEndpoint);
        assertThat(MatchingShardOwnershipConfig.resolveInstanceId("  ", orderEndpoint)).isEqualTo(orderEndpoint);
        assertThat(MatchingShardOwnershipConfig.resolveInstanceId("matching-worker-0", orderEndpoint))
            .isEqualTo("matching-worker-0");
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
