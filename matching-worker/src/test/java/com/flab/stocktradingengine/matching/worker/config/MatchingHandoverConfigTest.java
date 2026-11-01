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
                assertThat(context).hasSingleBean(MatchingHandoverConfig.StuckHandoverReporterRunner.class);
            });
    }

    @Test
    @SuppressWarnings("unchecked")
    void 재시작_규칙은_스냅샷과_저널로_복원한_담당_목록을_엔진에도_알린다() {
        com.flab.stocktradingengine.matching.disruptor.engine.MatchingEngine engine =
            mock(com.flab.stocktradingengine.matching.disruptor.engine.MatchingEngine.class);
        com.flab.stocktradingengine.matching.worker.coordination.MatchingSlotMapReader reader =
            mock(com.flab.stocktradingengine.matching.worker.coordination.MatchingSlotMapReader.class);
        org.springframework.beans.factory.ObjectProvider<com.flab.stocktradingengine.matching.disruptor.engine.MatchingEngine> engineProvider =
            mock(org.springframework.beans.factory.ObjectProvider.class);
        org.springframework.beans.factory.ObjectProvider<java.util.Optional<com.flab.stocktradingengine.matching.worker.recovery.StoredMatchingSnapshot>> snapshotProvider =
            mock(org.springframework.beans.factory.ObjectProvider.class);
        org.springframework.beans.factory.ObjectProvider<java.util.List<com.flab.stocktradingengine.codec.JournaledOrder>> journalProvider =
            mock(org.springframework.beans.factory.ObjectProvider.class);
        org.mockito.Mockito.when(engineProvider.getIfAvailable()).thenReturn(engine);
        org.mockito.Mockito.when(snapshotProvider.getIfAvailable(org.mockito.ArgumentMatchers.any()))
            .thenReturn(java.util.Optional.empty());
        // 저널에 받음만 있고 넘김이 없는 종목 slot 5
        org.mockito.Mockito.when(journalProvider.getIfAvailable())
            .thenReturn(java.util.List.of(com.flab.stocktradingengine.codec.JournaledOrder.slotAcquired(5)));

        org.springframework.context.SmartLifecycle lifecycle = new MatchingHandoverConfig().handoverRestartRuleLifecycle(
            reader, new HandoverBookStore(), engineProvider, snapshotProvider, journalProvider, 100L,
            "aeron:udp?endpoint=localhost:20021");
        lifecycle.start();

        org.mockito.Mockito.verify(engine).seedAcquiredSlots(java.util.Set.of(5));
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

    @Test
    void 조정_모드에서_호가창_이동_주소_풀이_비어_있으면_기동이_실패하고_메시지에_필요한_키가_있다() {
        contextRunner
            .withPropertyValues(
                "matching-shard.coordination.enabled=true",
                "spring.kafka.bootstrap-servers=localhost:9092",
                "matching-worker.instance-id=matching-1",
                "transport.matching-intake.channel=aeron:udp?endpoint=localhost:20021")
            .run(context -> {
                assertThat(context).hasFailed();
                assertThat(context.getStartupFailure()).hasStackTraceContaining("matching-shard.handover.endpoints");
            });
    }

    @Test
    void 조정_모드에서_풀에_자기_주소가_없으면_기동이_실패한다() {
        contextRunner
            .withPropertyValues(
                "matching-shard.coordination.enabled=true",
                "spring.kafka.bootstrap-servers=localhost:9092",
                "matching-worker.instance-id=matching-1",
                "transport.matching-intake.channel=aeron:udp?endpoint=localhost:20021",
                "matching-shard.handover.endpoints[0]=aeron:udp?endpoint=localhost:20020")
            .run(context -> {
                assertThat(context).hasFailed();
                assertThat(context.getStartupFailure()).hasStackTraceContaining("matching-shard.handover.endpoints");
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
