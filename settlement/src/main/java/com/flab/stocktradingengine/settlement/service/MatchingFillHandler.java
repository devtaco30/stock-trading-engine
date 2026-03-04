package com.flab.stocktradingengine.settlement.service;

import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.flab.stocktradingengine.trading.matching.FillResult;
import com.flab.stocktradingengine.trading.matching.event.OrderFilledEvent;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * 체결 이벤트를 받아 DB 에 반영하는 핸들러.
 *
 * <h3>패턴: Event-Driven Consumer</h3>
 * <p>매칭 스레드(MatchingEngine)와 DB 쓰기 스레드를 분리한다.
 * 매칭 스레드는 {@link OrderFilledEvent} 만 발행하고 즉시 다음 매칭으로 이동하며,
 * 이 핸들러가 별도 스레드에서 DB 를 갱신한다.
 * 결과적으로 매칭 처리량이 DB 지연에 영향을 받지 않는다.</p>
 *
 * <h3>@Async 적용 이유</h3>
 * <p>{@code @Async} 없이 {@code @EventListener} 만 쓰면 이벤트 발행 스레드(매칭 스레드)
 * 가 직접 이 메서드를 실행한다. 그러면 DB IO 동안 매칭 스레드가 블로킹되어
 * 인메모리 매칭의 이점이 사라진다.
 * {@code @Async("matchingFillExecutor")} 로 AsyncConfig 에 정의한 전용 풀에서 실행한다.</p>
 *
 * <h3>@Transactional 범위</h3>
 * <p>매수 DB 반영(주문 갱신 + 보유 추가 + 미결제 생성)과
 * 매도 DB 반영(주문 갱신 + 보유 차감)을 각각 독립 트랜잭션으로 처리한다.
 * 하나가 실패해도 다른 쪽에 영향을 주지 않도록 {@code fillBuyOrderPartially}·
 * {@code fillSellOrderPartially} 각각의 {@code @Transactional} 경계를 따른다.</p>
 *
 * <h3>장애 처리 한계 (Level 1)</h3>
 * <p>DB 반영에 실패해도 현재는 에러 로그만 남긴다.
 * 프로덕션 수준에서는 Dead Letter Queue(DLQ) 나 재시도 메커니즘이 필요하다.
 * 서버가 재시작되면 PENDING 상태 주문을 DB 에서 다시 로드해 호가창을 복원한다.</p>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class MatchingFillHandler {

    private final OrderSettlementService orderSettlementService;

    @Async("matchingFillExecutor")
    @EventListener
    public void handle(OrderFilledEvent event) {
        FillResult fill = event.fillResult();
        try {
            // 매수 주문 DB 반영: 주문 filledQuantity 갱신, 보유 추가, 미결제 생성
            orderSettlementService.fillBuyOrderPartially(
                fill.buyOrderId(), fill.filledQuantity(), fill.matchPrice());

            // 매도 주문 DB 반영: 주문 filledQuantity 갱신, 보유 차감
            orderSettlementService.fillSellOrderPartially(
                fill.sellOrderId(), fill.filledQuantity());

            log.debug("[체결 DB 반영 완료] 종목={} 매수={} 매도={} 수량={} 가격={}",
                event.stockCode(), fill.buyOrderId(), fill.sellOrderId(),
                fill.filledQuantity(), fill.matchPrice());

        } catch (Exception e) {
            // Level 1: 로그 기록. 추후 DLQ 또는 재시도 정책 추가 필요.
            log.error("[체결 DB 반영 실패] 종목={} fill={}", event.stockCode(), fill, e);
        }
    }
}
