package com.flab.stocktradingengine.aeron;

/**
 * 프로세스 경계를 넘는 데이터 스트림 ID(fork1 LLD §3-1). api·account-worker·matching-worker가
 * 발신·수신 양쪽에서 반드시 같은 값을 써야 해(스트림 ID가 다르면 서로 다른 스트림이 된다) core에
 * 한 번만 선언해 공유한다. 저널·리플레이·스냅샷 스트림(2005·2006·4005·4006·6002)은 워커 내부
 * 전용이라 여기 포함하지 않는다.
 */
public final class AeronStreamIds {

    private AeronStreamIds() {}

    public static final int ACCOUNT_INTAKE = 4004;
    public static final int MATCHING_INTAKE = 2002;
    public static final int FILL = 6001;
}
