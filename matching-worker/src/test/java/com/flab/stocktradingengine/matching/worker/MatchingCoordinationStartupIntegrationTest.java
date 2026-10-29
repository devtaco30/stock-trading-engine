package com.flab.stocktradingengine.matching.worker;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

import com.flab.stocktradingengine.matching.worker.coordination.KafkaMatchingShardAssignment;
import com.flab.stocktradingengine.matching.worker.coordination.MatchingSlotMapReader;
import com.flab.stocktradingengine.matching.worker.handover.HandoverBookStore;

/**
 * 조정 모드로 실제 Kafka와 Aeron을 붙여 앱이 끝까지 기동하는지 확인하는 기동 시험(U16c까지의 배선 전체: 토픽 준비 → 맵 읽기 →
 * 재시작 규칙 → 배정 → 호가창 이동 thread). 브로커 주소는 환경변수 KAFKA_BOOTSTRAP(기본 localhost:9092)이다. 배정이 오기까지 기다려
 * 담당이 켜지는 것까지 확인한다.
 */
class MatchingCoordinationStartupIntegrationTest {

    private static final String BOOTSTRAP = System.getenv().getOrDefault("KAFKA_BOOTSTRAP", "localhost:9092");

    @TempDir
    Path archiveDir;

    @Test
    void 조정_모드로_끝까지_기동하고_첫_배정을_받으면_저널_기록_확인_뒤에_담당이_켜진다() throws Exception {
        String suffix = String.valueOf(System.nanoTime());
        ConfigurableApplicationContext context = new SpringApplicationBuilder(MatchingWorkerApplication.class)
            .web(WebApplicationType.NONE)
            .properties(
                "matching.worker.archive-dir=" + archiveDir.toAbsolutePath(),
                "matching-shard.coordination.enabled=true",
                "matching-worker.instance-id=matching-0",
                "transport.matching-intake.channel=aeron:udp?endpoint=localhost:20740",
                "matching-shard.handover.endpoints[0]=aeron:udp?endpoint=localhost:20740",
                "matching-shard.handover.endpoints[1]=aeron:udp?endpoint=localhost:20741")
            // application.yml의 spring.kafka.bootstrap-servers를 덮어야 해서 명령행 인자(가장 높은 우선순위)로 준다.
            .run("--spring.kafka.bootstrap-servers=" + BOOTSTRAP);
        try {
            KafkaMatchingShardAssignment assignment = context.getBean(KafkaMatchingShardAssignment.class);
            assertThat(context.getBean(MatchingSlotMapReader.class)).isNotNull();
            assertThat(context.getBean(HandoverBookStore.class)).isNotNull();

            long deadline = System.nanoTime() + 60_000_000_000L;
            while (assignment.currentSlotsSnapshot().size() < 64 && System.nanoTime() < deadline) {
                Thread.sleep(100);
            }

            assertThat(assignment.currentSlotsSnapshot()).as("단독 기동이라 종목 slot 64개 전부 담당").hasSize(64);
        } finally {
            context.close();
        }
    }
}
