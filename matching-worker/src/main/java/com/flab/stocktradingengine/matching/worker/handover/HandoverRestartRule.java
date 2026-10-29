package com.flab.stocktradingengine.matching.worker.handover;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.Optional;
import java.util.Set;
import java.util.function.IntFunction;

import com.flab.stocktradingengine.aeron.MatchingSlotTarget;
import com.flab.stocktradingengine.matching.disruptor.engine.MatchingEngine;

/**
 * 호가창을 넘긴 쪽(X)의 재시작 규칙(11-14절 3번). 저널 재적용은 넘긴 종목 slot의 호가창도 되살린다. 엔진을 시작하기 전에, 담당
 * 목록에 없는데 호가창이 있는 종목 slot을 맵 값으로 가른다.
 * <ul>
 * <li>맵의 담당이 다른 pod이면(평상시 값이든 이동 중 값이든) 낡은 복사본이므로 호가창을 지운다.</li>
 * <li>자기 주소의 {@code ;moving}이면 이동이 끝나지 않았으므로 호가창을 두고, 새 담당의 재요청에 응답할 수 있게 보관
 * ({@link HandoverBookStore})을 호가창에서 다시 만들며, 그 종목 slot을 넘기는 중으로 표시한다.</li>
 * <li>값이 없거나 자기 주소의 평상시 값이면 건드리지 않는다.</li>
 * </ul>
 * 담당 목록에 오른 종목 slot은 맵과 상관없이 건드리지 않는다(재시작해 그 종목 slot을 그대로 돌려받는 경우는 U15b의 재시작 처리가
 * 맡는다). 복구는 Kafka의 배정에 의존하지 않는다 — 접수 가능 여부는 저널과 스냅샷으로 정해지고, 이 규칙은 맵을 읽을 수 있을 때만
 * 호가창을 정리한다.
 */
public final class HandoverRestartRule {

    private static final Logger log = System.getLogger(HandoverRestartRule.class.getName());

    private final String ownEndpoint;
    private final IntFunction<Optional<MatchingSlotTarget>> mapLookup;
    private final HandoverBookStore store;

    public HandoverRestartRule(String ownEndpoint, IntFunction<Optional<MatchingSlotTarget>> mapLookup, HandoverBookStore store) {
        this.ownEndpoint = ownEndpoint;
        this.mapLookup = mapLookup;
        this.store = store;
    }

    /**
     * 맵 읽기가 따라잡을 때까지 기다린 뒤 규칙을 적용한다. 기다리다 실패하면 아무것도 지우지 않고 경고만 남긴다.
     *
     * @param awaitMapCatchUp 맵 읽기가 따라잡을 때까지 기다리는 동작. 시간 안에 못 끝나면 {@link IllegalStateException}을 던진다.
     * @return 규칙을 적용했으면 true
     */
    public boolean applyWhenMapReady(Runnable awaitMapCatchUp, MatchingEngine engine, Set<Integer> ownedSlots) {
        try {
            awaitMapCatchUp.run();
        } catch (IllegalStateException e) {
            log.log(Level.WARNING, "[재시작] 맵 읽기가 끝나지 않아 넘긴 종목 slot의 호가창을 정리하지 않음: " + e.getMessage());
            return false;
        }
        apply(engine, ownedSlots);
        return true;
    }

    /** 시작 전 복구 단계(단일 thread)에서만 부른다. */
    public void apply(MatchingEngine engine, Set<Integer> ownedSlots) {
        for (int stockSlot : engine.stockSlotsWithBooks()) {
            if (ownedSlots.contains(stockSlot)) {
                continue;
            }
            Optional<MatchingSlotTarget> target = mapLookup.apply(stockSlot);
            if (target.isEmpty()) {
                continue;
            }
            MatchingSlotTarget value = target.get();
            if (!value.endpoint().equals(ownEndpoint)) {
                // 맵의 담당이 다른 pod이면(평상시 값이든 그 pod가 넘기는 중인 값이든) 이 shard의 호가창은 낡은 복사본이다.
                engine.removeBooksOfSlot(stockSlot);
                store.remove(stockSlot);
                log.log(Level.INFO, "[재시작] 맵의 담당이 다른 pod인 종목 slot의 호가창을 지움: 종목 slot=" + stockSlot + " 맵 값=" + value);
            } else if (value.moving() && value.endpoint().equals(ownEndpoint)) {
                store.put(stockSlot, engine.encodeBooksOfSlot(stockSlot));
                engine.markSlotHandedOver(stockSlot);
                log.log(Level.INFO, "[재시작] 이동이 끝나지 않은 종목 slot의 보관을 다시 만듦: 종목 slot=" + stockSlot);
            }
        }
    }
}
