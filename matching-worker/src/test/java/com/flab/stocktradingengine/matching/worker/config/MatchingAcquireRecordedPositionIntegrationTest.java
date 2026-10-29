package com.flab.stocktradingengine.matching.worker.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;

import com.flab.stocktradingengine.codec.EventType;
import com.flab.stocktradingengine.codec.JournaledOrder;
import com.flab.stocktradingengine.matching.disruptor.engine.MatchingEngine;
import com.flab.stocktradingengine.matching.disruptor.io.SlotEventListener;
import com.flab.stocktradingengine.matching.disruptor.io.SlotEventRequests;
import com.flab.stocktradingengine.matching.disruptor.journal.AeronArchiveMatchingJournal;

import io.aeron.archive.client.AeronArchive;

/**
 * 실제 Archive에서 받음 항목의 담당 켜기가 Archive 기록 위치 확인을 거친다(11-12절 보장 강화). 받음 요청을 넣으면 주문 수신
 * thread가 이벤트로 발행하고, 저널 핸들러가 append한 뒤 같은 thread가 Archive 기록 위치를 폴링해 닿으면 통지한다. 통지 시점에
 * Archive 녹화는 그 항목의 끝 위치까지 기록되어 있다.
 */
class MatchingAcquireRecordedPositionIntegrationTest {

    private final List<Integer> notices = new CopyOnWriteArrayList<>();

    @TempDir
    private Path archiveDir;

    @TestConfiguration
    static class RecordingListener {
        static final List<Integer> NOTICES = new CopyOnWriteArrayList<>();
        static final List<long[]> POSITIONS_AT_NOTICE = new CopyOnWriteArrayList<>();

        @Bean
        SlotEventListener matchingSlotEventListener(
                ObjectProvider<AeronArchive> archive, ObjectProvider<AeronArchiveMatchingJournal> journal,
                @Qualifier("matchingJournalRecordingId") ObjectProvider<Long> recordingId) {
            return new SlotEventListener() {
                @Override
                public void onSlotAcquiredJournaled(int stockSlot) {
                    // 통지는 주문 수신 thread에서 나가므로 같은 thread에서 Archive 기록 위치를 읽어도 된다.
                    POSITIONS_AT_NOTICE.add(new long[] {
                        archive.getObject().getRecordingPosition(recordingId.getObject()), journal.getObject().position()});
                    NOTICES.add(stockSlot);
                }
            };
        }
    }

    @Test
    void 받음_요청을_넣으면_Archive가_기록한_뒤에_통지가_나가고_그_항목이_저널에_있다() throws Exception {
        RecordingListener.NOTICES.clear();
        RecordingListener.POSITIONS_AT_NOTICE.clear();
        ConfigurableApplicationContext context = new SpringApplicationBuilder(
                com.flab.stocktradingengine.matching.worker.MatchingWorkerApplication.class, RecordingListener.class)
            .web(WebApplicationType.NONE)
            .properties(
                "matching.worker.archive-dir=" + archiveDir.toAbsolutePath(),
                "spring.main.allow-bean-definition-overriding=true")
            .run();
        try {
            SlotEventRequests requests = context.getBean(SlotEventRequests.class);
            MatchingEngine engine = context.getBean(MatchingEngine.class);

            requests.requestAcquire(5);

            long deadline = System.nanoTime() + 8_000_000_000L;
            while (RecordingListener.NOTICES.isEmpty() && System.nanoTime() < deadline) {
                Thread.sleep(5);
            }
            assertThat(RecordingListener.NOTICES).containsExactly(5);
            long[] positions = RecordingListener.POSITIONS_AT_NOTICE.get(0);
            assertThat(positions[1]).as("받음 항목이 저널에 append되어 발행 위치가 0보다 크다").isGreaterThan(0L);
            assertThat(positions[0]).as("통지 시점의 Archive 기록 위치가 항목 끝 위치 이상이다").isGreaterThanOrEqualTo(positions[1]);
            assertThat(engine.journal()).isNotNull();
        } finally {
            context.close();
        }
    }
}
