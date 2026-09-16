package com.flab.stocktradingengine.matching.disruptor.io;

import java.util.Map;

/**
 * 소비자 스레드(single-writer)가 러닝 중 스냅샷을 디스크에 내보내는 통로(I6 U1,
 * account-disruptor {@code AccountSnapshotSink}와 같은 결). matching-disruptor는 인터페이스만
 * 두고, 실제 파일 I/O(별도 쓰기 스레드 + fsync)는 matching-worker가 구현한다(ADR-024,
 * "single-writer는 I/O 안 함").
 *
 * <p>I6 시점엔 계좌 쪽 {@code AccountSnapshotSink}와 달리 발행자별 수신 위치 맵을 받지 않았다
 * ("매칭은 호가창만 찍으면 된다") — I2 U1에서 매칭도 주문 인테이크를 REMOTE 녹화하게 되면서
 * 그 개념(발행자=계좌 샤드별 수신 위치)이 매칭에도 생겨, 계좌와 같은 시그니처로 맞춘다.</p>
 */
public interface MatchingSnapshotSink {

    /**
     * 스냅샷 바이트를 쓰기 큐에 넣는다. 큐가 가득 차면 false를 돌려주고(그 회차는 스킵, 다음
     * 스냅샷 주기에 다시 시도), 예외를 던지지 않는다 — 소비자 스레드를 막으면 안 된다.
     *
     * @param snapshotBytes {@code MatchingSnapshotCodec}로 이미 인코딩된 스냅샷(저널 위치 포함)
     * @param orderIntakePositions 이 스냅샷 시점의 주문 인테이크 수신 위치를 발행자(계좌 샤드
     *                      Aeron sessionId)별로 담은 맵({@code MatchingEventHandler
     *                      #lastAppliedOrderIntakePositions()}, I2 U1)
     * @param appliedSeq    이 스냅샷을 만든 시점의 저널 적용 순번 — durable해지면 이 값이 {@link #durableSeq()}로 보고된다
     */
    boolean offer(byte[] snapshotBytes, Map<Integer, Long> orderIntakePositions, long appliedSeq);

    /**
     * 지금까지 디스크에 durable하게(fsync 완료) 쓰인 가장 최근 스냅샷의 appliedSeq. 아직 아무것도
     * 안 썼으면(또는 구현체가 없으면) 0. 매칭은 이 값으로 가지치기를 하지 않는다(I6 범위 밖, D3) —
     * 나중에 매칭 쪽 디스크 회수 작업의 경계로만 쓰인다.
     */
    long durableSeq();
}
