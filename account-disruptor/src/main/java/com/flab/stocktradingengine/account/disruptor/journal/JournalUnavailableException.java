package com.flab.stocktradingengine.account.disruptor.journal;

import com.flab.stocktradingengine.account.disruptor.engine.AccountEngine;

/**
 * 저널이 주어진 시간 안에 이벤트를 durable하게 기록하지 못했을 때({@link AccountEngine#blockUntilJournaled}
 * 타임아웃) 던지는 예외.
 *
 * <p>전용 타입으로 두는 이유는 호스트(account-worker)의 Kafka 에러 핸들러가 이 예외만 골라
 * 다르게 다루기 위해서다 — 이건 "이 메시지가 나쁘다(poison)"가 아니라 "저널(durable 저장소)이
 * 지금 못 쓴다"는 인프라 신호다. 그래서 드롭(DLQ)이 아니라, offset을 커밋하지 않은 채 컨테이너를
 * 멈춰(backpressure) 유실을 막고, 재시작 시 저널 replay로 복구하게 한다. Kafka Streams가 changelog
 * 기록 실패를 fatal로 다루고 재기동 시 changelog에서 복원하는 것과 같은 모델이다.</p>
 */
public class JournalUnavailableException extends RuntimeException {

    public JournalUnavailableException(String message) {
        super(message);
    }
}
