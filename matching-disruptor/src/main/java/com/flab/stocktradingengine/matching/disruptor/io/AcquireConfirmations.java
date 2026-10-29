package com.flab.stocktradingengine.matching.disruptor.io;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.LongSupplier;

/**
 * 종목 slot 받음 항목이 Archive에 기록되었는지 확인하는 대기열. 저널 핸들러 thread가 받음 항목을 append한 뒤 그 항목의 끝
 * 위치(발행 위치)를 {@link #awaitRecorded}로 넣고, 주문 수신 thread가 반복마다 {@link #confirmRecorded}로 Archive의 기록
 * 위치({@code AeronArchive#getRecordingPosition})가 그 위치에 닿았는지 확인해 닿은 것부터 통지한다.
 *
 * <p>append가 성공했다는 것(Aeron offer 성공)은 항목이 발행 로그 버퍼에 들어갔다는 뜻이고 Archive가 녹화 파일에 쓴 뒤가
 * 아니다. 그 사이에 프로세스가 죽으면 받음 항목이 사라지고, 담당을 이미 켜 받아 둔 주문이 복구 때 목록에 없어 걸러진다. 기록
 * 위치 확인은 그 구간을 닫는다. Archive의 파일 동기화 수준이 0(기본값)이라 기록 위치가 올랐다는 것은 파일에 썼다는 뜻이고
 * fsync 보장은 아니다. 그래서 프로세스 장애는 막지만 정전이나 커널 장애는 막지 못한다.</p>
 *
 * <p>기다리는 일은 저널 핸들러 thread가 아니라 확인을 부르는 thread에서 한다. Archive 클라이언트는 thread 안전하지 않아
 * 기록 위치를 읽는 thread를 하나로 정해야 한다.</p>
 */
public final class AcquireConfirmations {

    private static final Logger log = System.getLogger(AcquireConfirmations.class.getName());

    // 대기 항목이 이 시간보다 오래 남으면 경고한다(5초, 근거 없는 임의 값 — 앞서 잰 지연이 p99 3.7ms였으므로 훨씬 길게 잡았다).
    private static final long STUCK_WARNING_AFTER_NANOS = 5_000_000_000L;
    // 경고를 남기는 최소 간격(10초, 근거 없는 임의 값). 폴링 반복마다 남기지 않는다.
    private static final long WARNING_INTERVAL_NANOS = 10_000_000_000L;

    private record Pending(int stockSlot, long journalPosition, long enqueuedAtNanos) {
    }

    private final LongSupplier recordedPosition;
    private final LongSupplier nanoClock;
    private final Queue<Pending> pending = new ConcurrentLinkedQueue<>();
    private long lastWarningAtNanos;
    private boolean warnedOnce;
    private long warningsLogged;

    /** @param recordedPosition Archive가 지금까지 기록한 위치. 기록이 없거나 확인할 수 없으면 음수를 돌려준다. */
    public AcquireConfirmations(LongSupplier recordedPosition) {
        this(recordedPosition, System::nanoTime);
    }

    /** 시각을 주입받는 생성자(시험용). */
    public AcquireConfirmations(LongSupplier recordedPosition, LongSupplier nanoClock) {
        this.recordedPosition = recordedPosition;
        this.nanoClock = nanoClock;
    }

    /** 저널 핸들러 thread가 받음 항목을 append한 직후 그 항목의 끝 위치와 함께 부른다. */
    public void awaitRecorded(int stockSlot, long journalPosition) {
        pending.add(new Pending(stockSlot, journalPosition, nanoClock.getAsLong()));
    }

    /**
     * 기록 위치가 닿은 받음 항목을 넣은 순서대로 통지한다. 앞 항목이 아직이면 뒤 항목도 기다린다(위치는 단조 증가한다).
     * 기다리는 항목이 없으면 Archive에 묻지 않는다.
     *
     * @return 통지한 건수
     */
    public int confirmRecorded(SlotEventListener listener) {
        Pending head = pending.peek();
        if (head == null) {
            return 0;
        }
        long recorded = recordedPosition.getAsLong();
        int confirmed = 0;
        while (head != null && recorded >= head.journalPosition()) {
            pending.poll();
            listener.onSlotAcquiredJournaled(head.stockSlot());
            confirmed++;
            head = pending.peek();
        }
        if (head != null) {
            warnIfStuck(head, recorded);
        }
        return confirmed;
    }

    /** 기록 위치를 읽을 수 없거나(음수) 대기 항목이 한도보다 오래 남았으면, 간격을 두고 경고한다. 확인을 부르는 thread 하나에서만 호출한다. */
    private void warnIfStuck(Pending head, long recorded) {
        long now = nanoClock.getAsLong();
        boolean unreadable = recorded < 0;
        boolean waitedTooLong = now - head.enqueuedAtNanos() > STUCK_WARNING_AFTER_NANOS;
        if (!unreadable && !waitedTooLong) {
            return;
        }
        if (warnedOnce && now - lastWarningAtNanos < WARNING_INTERVAL_NANOS) {
            return;
        }
        warnedOnce = true;
        lastWarningAtNanos = now;
        warningsLogged++;
        log.log(Level.WARNING, "[매칭] 종목 slot 받음 항목의 Archive 기록 확인이 끝나지 않음: 종목 slot=" + head.stockSlot()
            + " 항목 끝 위치=" + head.journalPosition() + " Archive 기록 위치=" + recorded
            + (unreadable ? "(읽을 수 없음)" : "") + " 기다린 시간(ms)=" + (now - head.enqueuedAtNanos()) / 1_000_000L);
    }

    /** 남긴 경고 횟수(시험·점검용). 확인을 부르는 thread에서 읽는다. */
    public long warningsLogged() {
        return warningsLogged;
    }
}
