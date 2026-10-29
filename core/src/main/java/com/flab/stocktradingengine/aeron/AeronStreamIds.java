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
    public static final int ORDER_RESULT = 4007;

    /**
     * 매칭 pod가 담당이 아닌 주문을 계좌 shard로 되돌리는 거부 회신. 체결(6001)과 같은 방향이지만
     * {@code FillCodec}에 메시지 종류 바이트가 없어 같은 번호에 섞을 수 없다. Archive 녹화를 걸지 않는다.
     */
    public static final int REJECT_REPLY = 6003;

    /** 종목 slot 이동 때 새 담당 pod(Z)가 옛 담당 pod(X)에게 호가창을 요청한다(Z → X). */
    public static final int ORDER_BOOK_REQUEST = 2008;

    /** 옛 담당 pod(X)가 호가창을 나눠 응답한다(X → Z). 조각 메시지라 FragmentAssembler가 필요하다. */
    public static final int ORDER_BOOK_RESPONSE = 2009;
}
