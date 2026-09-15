package com.flab.stocktradingengine.account.disruptor.io;

/**
 * 소비자 스레드(single-writer)가 러닝 중 스냅샷을 디스크에 내보내는 통로(1-3,
 * {@code docs/_tradeid_snapshot_prune.html}). {@link MatchingOrderSender}·{@code AccountResultListener}와
 * 같은 결 — account-disruptor는 인터페이스만 두고, 실제 파일 I/O(별도 쓰기 스레드 + fsync)는
 * account-worker가 구현한다(ADR-024, "single-writer는 I/O 안 함").
 *
 * <p>구현체는 {@link #offer}를 받은 바이트를 그대로(재인코딩 없이) 디스크에 쓴다 — 인코딩은
 * 소비자 스레드가 이미 끝냈다. 쓰기가 실제로 durable(fsync)하게 끝난 뒤에만 {@link #durableSeq()}가
 * 그 시점의 appliedSeq로 올라가야 한다 — 소비자 스레드가 이 값을 보고 옛 tradeId 세대를 가지치기할
 * 시점을 판단하므로(durable-before-prune), 아직 안 durable한데 먼저 durable이라 보고하면 크래시 시
 * dedup이 뚫린다(돈).</p>
 */
public interface AccountSnapshotSink {

    /**
     * 스냅샷 바이트를 쓰기 큐에 넣는다. 큐가 가득 차면 false를 돌려주고(그 회차는 스킵, 다음
     * 스냅샷 주기에 다시 시도), 예외를 던지지 않는다 — 소비자 스레드를 막으면 안 된다.
     *
     * @param snapshotBytes {@code AccountSnapshotCodec}로 이미 인코딩된 스냅샷(저널 위치 포함)
     * @param fillPosition  이 스냅샷 시점의 체결 수신 위치({@code AccountEventHandler#lastAppliedFillPosition()})
     * @param appliedSeq    이 스냅샷을 만든 시점의 저널 적용 순번 — durable해지면 이 값이 {@link #durableSeq()}로 보고된다
     */
    boolean offer(byte[] snapshotBytes, long fillPosition, long appliedSeq);

    /**
     * 지금까지 디스크에 durable하게(fsync 완료) 쓰인 가장 최근 스냅샷의 appliedSeq. 아직 아무것도
     * 안 썼으면(또는 구현체가 없으면) 0 — "아직 아무것도 durable하지 않다"는 뜻으로, 소비자 스레드는
     * 이 값보다 오래된 세대를 절대 가지치기하지 않는다.
     */
    long durableSeq();
}
