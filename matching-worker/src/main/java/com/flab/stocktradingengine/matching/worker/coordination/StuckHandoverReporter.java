package com.flab.stocktradingengine.matching.worker.coordination;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * 호가창 이동 중 어느 단계에든 정한 시간 넘게 머문 종목 slot을 찾아 사람이 알아챌 ERROR 신호로 남기고 조회할 수 있게 한다. 이동 중
 * 대기 상태(판정 대기, 호가창 적재 대기, 받음 기록 확인 대기, 맵 전환 대기, 포기한 맵 쓰기)에 처음 관찰된 시각부터 센다. 재요청을 계속해도
 * 이전 담당(X)이 응답하지 않으면 이 쪽에서 X를 다시 띄울 수 없으므로 사람이 X를 확인해야 한다. 같은 종목 slot의 신호는 반복 간격 안에는 다시
 * 남기지 않는다. 주기 호출은 {@link #check}를 부르는 쪽(예약 실행)이 한다. 한 thread에서만 부른다.
 */
public final class StuckHandoverReporter {

    private static final Logger log = System.getLogger(StuckHandoverReporter.class.getName());

    private final Supplier<Set<Integer>> waitingSlots;
    private final LongSupplier abandonedRequestCount;
    private final LongSupplier nowMillis;
    private final long thresholdMillis;
    private final long repeatMillis;

    // 종목 slot이 대기 상태에 처음 관찰된 시각, 마지막으로 신호를 남긴 시각.
    private final Map<Integer, Long> firstSeenAt = new HashMap<>();
    private final Map<Integer, Long> lastSignaledAt = new HashMap<>();
    private volatile Set<Integer> stuck = Set.of();

    /**
     * @param waitingSlots 지금 이동 중 어느 대기 상태에든 있는 종목 slot
     * @param abandonedRequestCount 호가창 요청을 포기한 누적 횟수(신호에 함께 적는다)
     * @param thresholdMillis 이 시간 넘게 머물면 멈춘 것으로 본다(근거 없는 임의 값, 미측정)
     * @param repeatMillis 같은 종목 slot의 신호를 다시 남기는 간격(근거 없는 임의 값, 미측정)
     */
    public StuckHandoverReporter(Supplier<Set<Integer>> waitingSlots, LongSupplier abandonedRequestCount, LongSupplier nowMillis,
                                 long thresholdMillis, long repeatMillis) {
        this.waitingSlots = waitingSlots;
        this.abandonedRequestCount = abandonedRequestCount;
        this.nowMillis = nowMillis;
        this.thresholdMillis = thresholdMillis;
        this.repeatMillis = repeatMillis;
    }

    /** 정한 시간 넘게 이동 중 대기 상태에 머문 종목 slot. */
    public Set<Integer> stuckHandoverSlots() {
        return stuck;
    }

    public void check() {
        long now = nowMillis.getAsLong();
        Set<Integer> waiting = waitingSlots.get();
        firstSeenAt.keySet().removeIf(slot -> !waiting.contains(slot));
        lastSignaledAt.keySet().removeIf(slot -> !waiting.contains(slot));
        Set<Integer> nowStuck = new TreeSet<>();
        Set<Integer> toSignal = new TreeSet<>();
        for (int slot : waiting) {
            long first = firstSeenAt.computeIfAbsent(slot, key -> now);
            if (now - first < thresholdMillis) {
                continue;
            }
            nowStuck.add(slot);
            Long last = lastSignaledAt.get(slot);
            if (last == null || now - last >= repeatMillis) {
                toSignal.add(slot);
                lastSignaledAt.put(slot, now);
            }
        }
        stuck = Set.copyOf(nowStuck);
        if (!toSignal.isEmpty()) {
            log.log(Level.ERROR, "[호가창 이동] 이동이 " + thresholdMillis + "ms 넘게 멈춘 종목 slot " + toSignal
                + " — 이전 담당이 응답하지 않거나 맵·저널 단계가 막혀 있을 수 있으니 사람이 이전 담당과 맵을 확인해야 한다(포기한 요청 누적 "
                + abandonedRequestCount.getAsLong() + "건, 멈춘 종목 slot 전체 " + nowStuck + ")");
        }
    }
}
