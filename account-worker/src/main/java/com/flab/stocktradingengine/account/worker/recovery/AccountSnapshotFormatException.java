package com.flab.stocktradingengine.account.worker.recovery;

import java.io.File;

/**
 * 계좌 스냅샷 파일의 형식 번호가 이 코드가 기대하는 값과 다를 때 던지는 예외(ADR-032 I1 D2).
 *
 * <p>구 형식 파일(발행자별 체결 위치 맵이 없던 시절)을 "스냅샷이 없는 셈" 치고 넘기지 않는다 —
 * 그 파일에는 잔고·보유·예약이 그대로 들어 있는데 못 읽는 것이므로, 조용히 넘기면 복구가 저널을
 * 처음부터 다시 훑느라 오래 걸린 이유를 아무도 모른 채 지나간다. 여기서 던져 프로세스가 뜨지
 * 않게 하고, 사람이 파일을 지우든 옮기든 정하게 한다.</p>
 */
public class AccountSnapshotFormatException extends RuntimeException {

    public AccountSnapshotFormatException(File file, int actualVersion, int expectedVersion) {
        super("계좌 스냅샷 파일 형식이 다릅니다: " + file
            + " (읽은 버전=" + actualVersion + ", 기대 버전=" + expectedVersion + ")");
    }
}
