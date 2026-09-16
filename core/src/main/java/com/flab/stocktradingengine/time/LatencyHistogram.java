package com.flab.stocktradingengine.time;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;

import org.HdrHistogram.Histogram;
import org.HdrHistogram.Recorder;

/**
 * 주문 하나가 발행된 뒤 소비자가 그 주문을 접수·예약하기까지 걸린 시간을 재는 지연 측정기(끝점①,
 * decision_records/v1-v2-e2e-measurement.md). v1({@code OrderRequestConsumer})·v2({@code
 * AccountEventHandler}) 양쪽 소비자가 같은 클래스를 쓴다 — 끝점이 앱 내부(로컬 wall-clock 뺄셈)라
 * 프록시·게이트웨이 로그로는 볼 수 없고, 초 단위 카운터 로그로는 p99 같은 꼬리 분포를 못 보여줘
 * 제품 코드에 상시 남겨 둔다.
 *
 * <h3>기본 off — 켜져 있을 때만 비용을 낸다</h3>
 * <p>{@code enabled=false}(기본값)면 {@link #record}가 분기 하나만 타고 즉시 반환한다 — {@link
 * Recorder} 인스턴스 자체를 만들지도 않는다. 핫패스(주문 접수)에 얹는 비용이 꺼져 있을 때 실질적으로
 * 0이어야 한다는 요구(39 설계)를 이 분기 하나로 만족한다.</p>
 *
 * <h3>{@link Recorder} — 여러 스레드가 동시에 기록하고, 실행 중에도 구간별로 꺼내 쓴다</h3>
 * <p>소비자가 스레드 하나뿐이라는 보장이 없고(v1은 Kafka 리스너 컨테이너 동시성에 따라 여럿일 수
 * 있다), 부하 측정은 앱을 재기동하지 않고 한 프로세스 안에서 워밍업·측정 패스 여러 개를 연달아
 * 돌리며 패스 경계마다 그 구간의 분포만 따로 뽑아야 한다(재기동해도 계좌 워커·주문 엔진이 스냅샷·DB
 * 상태를 복구해 리셋이 안 되기 때문). 평범한 {@link Histogram}은 동시 기록·조회가 안전하지 않고,
 * 조회 시점에 수동으로 reset하면 그 사이 다른 스레드의 record와 경합한다. {@link Recorder}는 이
 * 두 요구를 라이브러리 차원에서 해결한다 — {@link Recorder#recordValue}는 여러 스레드가 동시에
 * 불러도 안전하고, {@link #snapshotAndReset()}(내부적으로 {@link Recorder#getIntervalHistogram()})은
 * "마지막으로 꺼낸 뒤부터 지금까지"의 구간 히스토그램을 원자적으로 반환하면서 내부 상태를 다음
 * 구간을 위해 비운다 — 기록 스레드를 막지 않는다.</p>
 */
public final class LatencyHistogram {

    private static final Logger log = System.getLogger(LatencyHistogram.class.getName());

    private static final int SIGNIFICANT_VALUE_DIGITS = 3;

    private final boolean enabled;
    private final Recorder recorder;

    public LatencyHistogram(boolean enabled) {
        this.enabled = enabled;
        // 오토리사이징(상한 없이 필요한 만큼 커짐) — 값이 얼마나 크게 나오든 recordValue가
        // 예외를 던지지 않게 한다. 계측 코드가 핫패스를 죽이면 안 된다.
        this.recorder = enabled ? new Recorder(SIGNIFICANT_VALUE_DIGITS) : null;
    }

    /**
     * {@code publishedAtEpochNanos}부터 지금까지 걸린 시간을 기록한다. 여러 스레드가 동시에 불러도
     * 안전하다.
     *
     * <p>v2는 이 메서드를 계좌 엔진 single-writer 소비자 스레드의 핫패스에서 직접 부른다(2b 리뷰
     * 지적) — HdrHistogram 내부에서 예상 못한 예외가 나더라도(라이브러리 버그 등) 여기서 삼키고
     * 계측을 포기할 뿐, 절대 호출자에게 전파하지 않는다. 전파되면 계좌 엔진의 fail-fast 예외
     * 핸들러가 소비자 스레드를 죽이거나(주문 처리 전체 정지), 이 호출이 accept 처리 중간(리스너
     * 통지·매칭 발신 전)에 있어 이미 예약된 주문이 통지도 매칭 발신도 못 받고 붕 뜨는(돈) 결과로
     * 이어질 수 있다 — 계측 부가 기능이 본 기능을 해치면 안 된다.</p>
     */
    public void record(long publishedAtEpochNanos) {
        if (!enabled) {
            return;
        }
        try {
            long elapsedNanos = EpochNanos.now() - publishedAtEpochNanos;
            recorder.recordValue(Math.max(elapsedNanos, 0L));
        } catch (RuntimeException e) {
            log.log(Level.WARNING, "[접수 지연 측정] record 실패(계측만 스킵, 처리는 계속됨)", e);
        }
    }

    /**
     * 마지막으로 이 메서드를 부른 뒤(처음이면 측정 시작 뒤)부터 지금까지 기록된 값들의 백분위
     * 스냅샷을 만들고, 내부 상태를 다음 구간을 위해 비운다 — 부하 측정 패스 경계(워밍업 끝·패스N
     * 끝)와 앱 종료 시(그때까지 안 꺼낸 나머지) 양쪽에서 이 메서드 하나로 쓴다.
     *
     * <p>{@link #record}와 같은 이유로 내부 예외를 삼킨다 — 이 메서드는 폴링 스레드(부하 측정
     * 트리거 처리)에서 불려 주문 처리 핫패스와는 무관하지만, 예외가 그대로 새면 그 폴링 스레드
     * 자체가 죽어 이후 패스 경계를 영영 못 잡는다. 실패하면 빈 스냅샷을 돌려준다 — 호출자(트리거
     * 처리기)가 "이번 구간은 0건"으로 보고, 다음 트리거는 정상적으로 다시 시도된다.</p>
     */
    public LatencySnapshot snapshotAndReset() {
        if (!enabled) {
            return LatencySnapshot.empty();
        }
        try {
            Histogram interval = recorder.getIntervalHistogram();
            if (interval.getTotalCount() == 0) {
                return LatencySnapshot.empty();
            }
            return new LatencySnapshot(
                interval.getTotalCount(),
                interval.getValueAtPercentile(50.0),
                interval.getValueAtPercentile(95.0),
                interval.getValueAtPercentile(99.0),
                interval.getMaxValue()
            );
        } catch (RuntimeException e) {
            log.log(Level.WARNING, "[접수 지연 측정] snapshotAndReset 실패(빈 스냅샷으로 대체)", e);
            return LatencySnapshot.empty();
        }
    }
}
