package com.flab.stocktradingengine.account.worker;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;

import com.flab.stocktradingengine.account.disruptor.engine.AccountEngine;
import com.flab.stocktradingengine.account.worker.config.AccountJournalArchiveConfig;

import io.aeron.archive.client.AeronArchive;

/**
 * account-worker 앱을 실제로 띄우고, 계좌 엔진에 들어온 입력이 저널 스트림(4005)으로 Aeron Archive에
 * 실제로 durable 녹화되는지 확인하는 end-to-end 테스트(2b-1b). {@code AccountIntakeRecordingIntegrationTest}
 * (2a)와 같은 결이지만, 녹화 대상이 인테이크(4004)에서 저널(4005)로 옮겨갔다 — 그래서 발신도
 * 외부 Aeron Publication이 아니라 {@link AccountEngine}(계좌 엔진 자체)을 직접 호출한다: 저널은
 * 인테이크를 거치지 않는 체결·정산까지 기록하는 지점이라, "엔진에 들어온 입력이 곧 저널 스트림에
 * 나간다"를 검증하려면 엔진을 직접 건드리는 쪽이 실제 배선과 더 가깝다.
 *
 * <p>"녹화가 된다"까지만 증명한다 — 재시작 사이 카탈로그 보존은
 * {@link AccountJournalDurabilityAcrossRestartIntegrationTest}, 리플레이는 2b-2.</p>
 */
@SpringBootTest(
    classes = AccountWorkerApplication.class,
    properties = {
        "account-worker.seed-accounts[0].account-id=1",
        "account-worker.seed-accounts[0].balance=1000000",
        "account-worker.seed-accounts[0].margin-rate=0.40"
    }
)
@DirtiesContext
class AccountJournalArchiveRecordingIntegrationTest {

    private static final String STOCK = "005930";
    private static final long TIMEOUT_NANOS = 5_000_000_000L;
    private static final long NOT_FOUND = -1L;

    @Autowired
    private AeronArchive aeronArchive;

    @Autowired
    private AccountEngine accountEngine;

    @Test
    void 계좌_엔진_입력이_저널_스트림에_durable_녹화된다() {
        long recordingId = awaitRecordingId();
        assertThat(recordingId).as("저널 스트림 녹화가 시작돼 recordingId가 있어야 한다").isNotEqualTo(NOT_FOUND);
        long positionBefore = aeronArchive.getRecordingPosition(recordingId);

        // orderId는 엔진이 결정론적 발급기(2b-0)로 직접 내는 큰 packed 값이라(예측 불가) 여기서
        // 체결·정산까지는 안 섞는다 — BUY 접수 하나만으로도 저널 기록·녹화 위치 증가는 충분히 증명된다.
        // 5타입 전부의 저널 기록은 account-disruptor의 AccountJournalGatingTest(nodeId 고정)가 이미 검증한다.
        accountEngine.publishBuy(1L, STOCK, new BigDecimal("10000"), 10, "journal-recording-r1");

        awaitPositionAdvance(recordingId, positionBefore);
    }

    /** 저널 채널·스트림으로 진행 중인 녹화가 나타날 때까지 기다려 recordingId를 돌려준다. */
    private long awaitRecordingId() {
        long deadline = System.nanoTime() + TIMEOUT_NANOS;
        long recordingId;
        while ((recordingId = findRecordingId()) == NOT_FOUND) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("5초 안에 저널 스트림 녹화가 시작되지 않음");
            }
            Thread.yield();
        }
        return recordingId;
    }

    private long findRecordingId() {
        long[] found = {NOT_FOUND};
        aeronArchive.listRecordingsForUri(0, 10,
            AccountJournalArchiveConfig.JOURNAL_CHANNEL, AccountJournalArchiveConfig.JOURNAL_STREAM_ID,
            (controlSessionId, correlationId, recordingId, startTimestamp, stopTimestamp,
             startPosition, stopPosition, initialTermId, segmentFileLength, termBufferLength,
             mtuLength, sessionId, streamId, strippedChannel, originalChannel, sourceIdentity) ->
                found[0] = recordingId);
        return found[0];
    }

    /** 녹화 위치(디스크에 실제로 쓴 바이트 오프셋)가 기준값보다 늘어날 때까지 기다린다. */
    private void awaitPositionAdvance(long recordingId, long positionBefore) {
        long deadline = System.nanoTime() + TIMEOUT_NANOS;
        while (aeronArchive.getRecordingPosition(recordingId) <= positionBefore) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("5초 안에 녹화 위치가 늘지 않음 — 저널 엔트리가 디스크에 기록되지 않은 것으로 보임");
            }
            Thread.yield();
        }
    }
}
