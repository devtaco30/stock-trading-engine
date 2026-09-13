package com.flab.stocktradingengine.settlement.worker.scheduler;

import java.time.Instant;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.flab.stocktradingengine.settlement.worker.service.SettlementDispatcher;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * T+2 스캔을 주기적으로 트리거하는 얇은 래퍼. 로직은 {@link SettlementDispatcher#dispatchDue}에
 * 다 있다 — 이 클래스는 "언제 부를지"만 안다(영업일·달력 계산 없음, 단순 epochMillis 비교).
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class SettlementScanScheduler {

    private final SettlementDispatcher settlementDispatcher;

    @Scheduled(fixedDelayString = "${settlement-worker.scan-interval-ms:60000}")
    public void scan() {
        int dispatched = settlementDispatcher.dispatchDue(Instant.now().toEpochMilli());
        if (dispatched > 0) {
            log.info("[정산 워커] T+2 스캔 완료: {}건 발행", dispatched);
        }
    }
}
