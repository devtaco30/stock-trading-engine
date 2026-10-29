package com.flab.stocktradingengine.account.worker.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import com.flab.stocktradingengine.account.worker.coordination.MatchingDestinationResolver;
import com.flab.stocktradingengine.account.worker.messaging.AeronMatchingOrderSender;
import com.flab.stocktradingengine.account.worker.support.SleepingMockConsumer;
import com.flab.stocktradingengine.aeron.AeronStreamIds;
import com.flab.stocktradingengine.aeron.StockSlotHasher;

import io.aeron.Aeron;
import io.aeron.ConcurrentPublication;
import io.aeron.Subscription;

/**
 * 매칭 샤딩 U9 — 매칭 pod 주소 풀({@code matching-shard-routing.endpoints})이 없으면 지금처럼 Publication
 * 1개 경로이고, 있을 때만 resolver·풀 Publication·초기 읽기 대기가 켜지는지 확인한다. 맵 토픽 컨슈머는
 * 브로커 없이 대역으로 갈아 끼운다.
 */
class MatchingOrderSenderConfigTest {

    private static final String ADDRESS_ONE = "aeron:udp?endpoint=localhost:20020";
    private static final String ADDRESS_TWO = "aeron:udp?endpoint=localhost:20021";
    private static final String MAP_TOPIC = MatchingDestinationResolver.MAP_TOPIC;

    private final Aeron aeron = mock(Aeron.class);

    /** 주소 풀 설정이 있으면 거부 회신 스트림 구독을 여는데, Aeron 대역은 구독을 돌려주지 않으므로 대역 구독을 준다. */
    @BeforeEach
    void stubRejectReplySubscription() {
        when(aeron.addSubscription(anyString(), eq(AeronStreamIds.REJECT_REPLY))).thenReturn(mock(Subscription.class));
    }

    private ApplicationContextRunner contextRunner() {
        return contextRunnerWith(new SleepingMockConsumer(MAP_TOPIC, 64));
    }

    /** 컨텍스트는 종료할 때 컨슈머를 닫으므로 컨텍스트마다 새 대역을 쓴다. */
    private ApplicationContextRunner contextRunnerWith(SleepingMockConsumer mapConsumer) {
        when(aeron.addPublication(anyString(), anyInt())).thenAnswer(invocation -> mock(ConcurrentPublication.class));
        return new ApplicationContextRunner()
            .withUserConfiguration(MatchingOrderSenderConfig.class, MatchingShardRoutingConfig.class)
            .withBean(Aeron.class, () -> aeron)
            // 설정 클래스를 읽기 전에 같은 이름의 싱글톤을 먼저 등록해 @ConditionalOnMissingBean이 실제 Kafka 컨슈머를 만들지 않게 한다.
            .withInitializer(context -> context.getBeanFactory().registerSingleton("matchingShardMapConsumer", mapConsumer));
    }

    @Test
    void 주소_풀_설정이_없으면_지금처럼_Publication_하나로_보낸다() {
        contextRunner().run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context).hasBean("matchingOrderPublication");
            assertThat(context).hasSingleBean(AeronMatchingOrderSender.class);
            assertThat(context).doesNotHaveBean(MatchingDestinationResolver.class);
            verify(aeron, times(1)).addPublication(anyString(), eq(AeronStreamIds.MATCHING_INTAKE));
            verify(aeron, never()).addSubscription(anyString(), eq(AeronStreamIds.REJECT_REPLY));
        });
    }

    @Test
    void 주소_풀_설정이_있으면_주소마다_Publication을_열고_resolver를_켠다() {
        contextRunner()
            .withPropertyValues(
                "matching-shard-routing.endpoints[0]=" + ADDRESS_ONE,
                "matching-shard-routing.endpoints[1]=" + ADDRESS_TWO)
            .run(context -> {
                assertThat(context).hasNotFailed();
                assertThat(context).doesNotHaveBean("matchingOrderPublication");
                assertThat(context).hasSingleBean(MatchingDestinationResolver.class);
                assertThat(context).hasSingleBean(AeronMatchingOrderSender.class);
                verify(aeron).addPublication(ADDRESS_ONE, AeronStreamIds.MATCHING_INTAKE);
                verify(aeron).addPublication(ADDRESS_TWO, AeronStreamIds.MATCHING_INTAKE);
                verify(aeron, times(1)).addSubscription(anyString(), eq(AeronStreamIds.REJECT_REPLY));
                verify(aeron, times(2)).addPublication(anyString(), eq(AeronStreamIds.MATCHING_INTAKE));
            });
    }

    @Test
    void 주소_풀_설정이_있으면_초기_읽기가_끝난_뒤에야_컨텍스트가_시작을_마친다() {
        String stockCode = "005930";
        SleepingMockConsumer mapConsumer = new SleepingMockConsumer(MAP_TOPIC, 64);
        int slot = new StockSlotHasher(64).slotFor(stockCode);
        Map<TopicPartition, Long> endOffsets = new HashMap<>();
        for (int partition = 0; partition < 64; partition++) {
            endOffsets.put(new TopicPartition(MAP_TOPIC, partition), partition == slot ? 1L : 0L);
        }
        mapConsumer.updateEndOffsets(endOffsets);
        mapConsumer.schedulePollTask(() ->
            mapConsumer.addRecord(new ConsumerRecord<>(MAP_TOPIC, slot, 0L, slot, ADDRESS_TWO)));

        contextRunnerWith(mapConsumer)
            .withPropertyValues("matching-shard-routing.endpoints[0]=" + ADDRESS_ONE, "matching-shard-routing.endpoints[1]=" + ADDRESS_TWO)
            .run(context -> {
                MatchingDestinationResolver resolver = context.getBean(MatchingDestinationResolver.class);
                // 대기 없이 바로 확인 — 컨텍스트 시작이 끝났다면 초기 읽기는 이미 끝나 있어야 한다.
                assertThat(resolver.endpointFor(stockCode)).isEqualTo(Optional.of(ADDRESS_TWO));
            });
    }

    @Test
    void 주소_풀이_8개를_넘으면_기동이_실패한다() {
        ApplicationContextRunner runner = contextRunner();
        String[] properties = new String[9];
        for (int index = 0; index < 9; index++) {
            properties[index] = "matching-shard-routing.endpoints[" + index + "]=aeron:udp?endpoint=localhost:" + (20020 + index);
        }

        runner.withPropertyValues(properties).run(context -> assertThat(context).hasFailed());
    }

    @Test
    void 보관_목록_상한은_설정으로_바꿀_수_있고_기본은_65536이다() {
        contextRunner()
            .withPropertyValues("matching-shard-routing.endpoints[0]=" + ADDRESS_ONE, "matching-order.holding-capacity-per-slot=0")
            .run(context -> assertThat(context.getBean(AeronMatchingOrderSender.class).hasHoldingRoom("005930")).isFalse());
        contextRunner()
            .withPropertyValues("matching-shard-routing.endpoints[0]=" + ADDRESS_ONE)
            .run(context -> assertThat(context.getBean(AeronMatchingOrderSender.class).hasHoldingRoom("005930")).isTrue());
    }

    @Test
    void 종목_slot_개수는_설정으로_바꿀_수_있고_기본은_64개다() {
        contextRunner()
            .withPropertyValues("matching-shard-routing.endpoints[0]=" + ADDRESS_ONE)
            .run(context -> assertThat(context.getBean(StockSlotHasher.class).stockSlotCount()).isEqualTo(64));
        contextRunner()
            .withPropertyValues("matching-shard-routing.endpoints[0]=" + ADDRESS_ONE, "matching-shard-routing.stock-slot-count=8")
            .run(context -> assertThat(context.getBean(StockSlotHasher.class).stockSlotCount()).isEqualTo(8));
    }
}
