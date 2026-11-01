package com.flab.stocktradingengine.matching.disruptor.handover;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * 받은 응답 메시지를 전송 id별로 모아, 순번 0부터 총 개수-1까지 모두 도착했을 때만 종목별 바이트를 순번 순서로 돌려준다
 * (11-6절). 같은 순번이 두 번 오면 먼저 온 것을 남기고 나중 것을 버린다. 모으는 것은 한 thread에서만 하고, {@link #discard}는 다른 thread에서도 부를 수 있다.
 *
 * <p>전송 id가 현재 진행 중인 요청과 다른 메시지를 버리는 판단은 요청을 보낸 쪽이 한다. 이 클래스는 전송 id마다
 * 따로 모으고, 버릴 전송은 {@link #discard}로 지운다.</p>
 */
public final class HandoverAssembler {

    private static final class Progress {
        private final int total;
        private final byte[][] bodies;
        private int received;

        private Progress(int total) {
            this.total = total;
            this.bodies = new byte[total][];
        }
    }

    private final Map<Long, Progress> inProgress = new java.util.concurrent.ConcurrentHashMap<>();

    /**
     * @return 이 메시지로 전송이 모두 모였으면 종목별 바이트(순번 순서), 아니면 빈 값. 모인 전송은 진행 상태에서 지운다.
     */
    public Optional<List<byte[]>> accept(HandoverMessage message) {
        if (message.total() == 0) {
            return Optional.of(List.of());
        }
        if (message.index() < 0 || message.index() >= message.total()) {
            return Optional.empty();
        }
        Progress progress = inProgress.computeIfAbsent(message.transferId(), id -> new Progress(message.total()));
        if (progress.total != message.total() || progress.bodies[message.index()] != null) {
            return Optional.empty();
        }
        progress.bodies[message.index()] = message.body();
        progress.received++;
        if (progress.received < progress.total) {
            return Optional.empty();
        }
        inProgress.remove(message.transferId());
        List<byte[]> assembled = new ArrayList<>(progress.total);
        for (byte[] body : progress.bodies) {
            assembled.add(body);
        }
        return Optional.of(assembled);
    }

    /** 모으는 중인 전송의 수. */
    public int pendingCount() {
        return inProgress.size();
    }

    public boolean isPending(long transferId) {
        return inProgress.containsKey(transferId);
    }

    /** 이 전송 id의 진행 상태를 버린다. 중간에 끊긴 이동의 옛 응답이 새 요청과 섞이지 않게 할 때 쓴다. */
    public void discard(long transferId) {
        inProgress.remove(transferId);
    }
}
