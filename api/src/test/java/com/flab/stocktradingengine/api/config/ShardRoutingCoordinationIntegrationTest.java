package com.flab.stocktradingengine.api.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.LifecycleProcessor;

import com.flab.stocktradingengine.aeron.AccountDestinationResolver;
import com.flab.stocktradingengine.aeron.AssignmentDestinationResolver;
import com.flab.stocktradingengine.aeron.ShardSlotCountSource;
import com.flab.stocktradingengine.aeron.SlotHasher;
import com.flab.stocktradingengine.aeron.StaticShardDestinationResolver;

/**
 * 조정 모드({@code account-shard.coordination.enabled=true})로 api 의 샤딩 설정이 실제로
 * 컨텍스트에 올라오는지 확인한다. 로컬 브로커(docker-compose, localhost:9092)가 떠 있어야 한다 —
 * 조정 모드는 칸 개수를 설정 파일이 아니라 조정 토픽의 파티션 개수에서 읽기 때문이다.
 *
 * <p>이 테스트가 없어서 놓쳤던 것 둘. 하나는 목적지 판정({@link AccountDestinationResolver})을
 * {@link AssignmentDestinationResolver} 자신과 그것을 감싸기만 하는 빈이 둘 다 제공해 Spring이
 * 어느 것을 넣을지 정하지 못하고 기동이 멈춘 것이고, 다른 하나는 조정 모드인데도 칸이 1개로
 * 계산돼 모든 계좌가 0번 칸으로 가던 것이다(워커를 여러 대 띄워도 한 대로만 주문이 간다).
 * 앱을 손으로 띄워 보고서야 찾았다.</p>
 *
 * <p>확인하지 않는 것: {@code account-shard-map} 구독 스레드는 시작하지 않는다. 생명주기 처리기를
 * 아무것도 안 하는 것으로 바꿔 끼운다 — 여기서 보려는 것은 빈 구성이고, 구독과 목적지 갱신은
 * {@code AssignmentDestinationResolverIntegrationTest}가 확인한다.</p>
 */
class ShardRoutingCoordinationIntegrationTest {

    private static final String BOOTSTRAP = "localhost:9092";
    private static final int SLOT_COUNT = 256;

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
        .withUserConfiguration(ShardRoutingConfig.class)
        .withBean("lifecycleProcessor", LifecycleProcessor.class, NoOpLifecycleProcessor::new);

    @BeforeAll
    static void ensureAssignmentTopic() {
        try (Admin admin = Admin.create(adminProps())) {
            admin.createTopics(List.of(new NewTopic(ShardSlotCountSource.ASSIGNMENT_TOPIC, SLOT_COUNT, (short) 1))).all().get();
        } catch (ExecutionException e) {
            if (!(e.getCause() instanceof TopicExistsException)) {
                throw new IllegalStateException("조정 토픽 준비 실패", e);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("조정 토픽 준비 중 인터럽트됨", e);
        }
    }

    @Test
    void 조정_모드로_뜨면_목적지_판정_빈이_하나만_남는다() {
        contextRunner
            .withPropertyValues(
                "account-shard.coordination.enabled=true",
                "spring.kafka.bootstrap-servers=" + BOOTSTRAP)
            .run(context -> {
                assertThat(context).hasNotFailed();
                assertThat(context).hasSingleBean(AccountDestinationResolver.class);
                assertThat(context.getBean(AccountDestinationResolver.class))
                    .isInstanceOf(AssignmentDestinationResolver.class);
            });
    }

    @Test
    void 조정_모드는_칸_개수를_조정_토픽의_파티션_개수에서_읽는다() {
        int partitionCount = readAssignmentTopicPartitionCount();
        contextRunner
            .withPropertyValues(
                "account-shard.coordination.enabled=true",
                "spring.kafka.bootstrap-servers=" + BOOTSTRAP)
            .run(context -> {
                SlotHasher slotHasher = context.getBean(SlotHasher.class);
                assertThat(slotHasher.slotCount()).isEqualTo(partitionCount);
                // 칸이 1개면 모든 계좌가 0번 칸으로 가 워커를 여러 대 띄워도 한 대로만 주문이 간다.
                assertThat(slotHasher.slotCount()).isGreaterThan(1);
            });
    }

    @Test
    void 정적_모드로_뜨면_설정에_적힌_표로_목적지를_정한다() {
        contextRunner
            .withPropertyValues(
                "shard-routing.slot-count=2",
                "shard-routing.shards[0].endpoint=aeron:udp?endpoint=localhost:29040",
                "shard-routing.shards[0].slot-from=0",
                "shard-routing.shards[0].slot-to=0",
                "shard-routing.shards[1].endpoint=aeron:udp?endpoint=localhost:29041",
                "shard-routing.shards[1].slot-from=1",
                "shard-routing.shards[1].slot-to=1")
            .run(context -> {
                assertThat(context).hasSingleBean(AccountDestinationResolver.class);
                assertThat(context.getBean(AccountDestinationResolver.class))
                    .isInstanceOf(StaticShardDestinationResolver.class);
            });
    }

    private int readAssignmentTopicPartitionCount() {
        try (Admin admin = Admin.create(adminProps())) {
            return ShardSlotCountSource.readSlotCount(admin, Duration.ofSeconds(10));
        }
    }

    private static Properties adminProps() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
        return props;
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
