package com.flab.stocktradingengine.matching.worker.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.LifecycleProcessor;

import com.flab.stocktradingengine.matching.disruptor.io.SlotEventListener;
import com.flab.stocktradingengine.matching.worker.coordination.HandoverEndpointPool;
import com.flab.stocktradingengine.matching.worker.handover.HandoverBookStore;
import com.flab.stocktradingengine.matching.worker.handover.HandoverResponder;

import io.aeron.Aeron;
import io.aeron.Subscription;

/** 호가창 이동 구성요소는 조정 모드에서만 만들어진다(U15a). 생명주기는 시작하지 않는다. */
class MatchingHandoverConfigTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
        .withUserConfiguration(MatchingShardOwnershipConfig.class, MatchingHandoverConfig.class)
        .withBean("lifecycleProcessor", LifecycleProcessor.class, MatchingHandoverConfigTest::noOpProcessor)
        .withBean(Aeron.class, () -> {
            Aeron aeron = mock(Aeron.class);
            org.mockito.Mockito.when(aeron.addSubscription(org.mockito.ArgumentMatchers.anyString(), org.mockito.ArgumentMatchers.anyInt()))
                .thenReturn(mock(Subscription.class));
            return aeron;
        });

    @Test
    void 조정_모드가_꺼져_있으면_호가창_이동_구성요소가_없다() {
        contextRunner.run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context).doesNotHaveBean(HandoverResponder.class);
            assertThat(context).doesNotHaveBean(HandoverBookStore.class);
        });
    }

    @Test
    void 조정_모드가_켜지면_풀과_보관소와_응답_thread가_만들어지고_풀은_설정_목록을_따른다() {
        contextRunner
            .withPropertyValues(
                "matching-shard.coordination.enabled=true",
                "spring.kafka.bootstrap-servers=localhost:9092",
                "matching-worker.instance-id=matching-1",
                "transport.matching-intake.channel=aeron:udp?endpoint=localhost:20021",
                "matching-shard.handover.endpoints[0]=aeron:udp?endpoint=localhost:20020",
                "matching-shard.handover.endpoints[1]=aeron:udp?endpoint=localhost:20021")
            .run(context -> {
                assertThat(context).hasNotFailed();
                HandoverEndpointPool pool = context.getBean(HandoverEndpointPool.class);
                assertThat(pool.peers()).containsExactly("aeron:udp?endpoint=localhost:20020");
                assertThat(context).hasSingleBean(HandoverResponder.class);
                assertThat(context).hasSingleBean(HandoverBookStore.class);
            });
    }

    @Test
    void 넘김_통지는_보관소에_담고_다시_받으면_지운다() {
        contextRunner
            .withPropertyValues(
                "matching-shard.coordination.enabled=true",
                "spring.kafka.bootstrap-servers=localhost:9092",
                "matching-worker.instance-id=matching-1",
                "transport.matching-intake.channel=aeron:udp?endpoint=localhost:20021",
                "matching-shard.handover.endpoints[0]=aeron:udp?endpoint=localhost:20020",
                "matching-shard.handover.endpoints[1]=aeron:udp?endpoint=localhost:20021")
            .run(context -> {
                SlotEventListener listener = context.getBean(SlotEventListener.class);
                HandoverBookStore store = context.getBean(HandoverBookStore.class);

                listener.onSlotHandedOver(7, java.util.List.of(new byte[] {1, 2}));
                assertThat(store.get(7)).isPresent();

                listener.onSlotAcquiredJournaled(7);
                assertThat(store.get(7)).isEmpty();
            });
    }

    @Test
    void 정리_통지는_보관소의_호가창_바이트를_지운다() {
        contextRunner
            .withPropertyValues(
                "matching-shard.coordination.enabled=true",
                "spring.kafka.bootstrap-servers=localhost:9092",
                "matching-worker.instance-id=matching-1",
                "transport.matching-intake.channel=aeron:udp?endpoint=localhost:20021",
                "matching-shard.handover.endpoints[0]=aeron:udp?endpoint=localhost:20020",
                "matching-shard.handover.endpoints[1]=aeron:udp?endpoint=localhost:20021")
            .run(context -> {
                SlotEventListener listener = context.getBean(SlotEventListener.class);
                HandoverBookStore store = context.getBean(HandoverBookStore.class);
                listener.onSlotHandedOver(7, java.util.List.of(new byte[] {1, 2}));

                listener.onSlotCleanedUp(7);

                assertThat(store.get(7)).isEmpty();
            });
    }

    private static LifecycleProcessor noOpProcessor() {
        return new LifecycleProcessor() {
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
        };
    }
}
