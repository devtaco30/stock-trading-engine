package com.flab.stocktradingengine.codec;

import java.math.BigDecimal;

/**
 * 계좌 엔진에 들어온 이벤트 하나의 불변 스냅샷(2b-1) — matching {@link JournaledOrder}의 계좌판.
 * 매칭과 달리 주문(BUY·SELL)만이 아니라 체결·정산까지 전 타입({@link AccountEventType}의 다섯 값 전부)을
 * 담는다 — 계좌 상태를 만드는 모든 입력을 순서대로 남겨야 복구(2b-2 리플레이)가 성립하기 때문이다.
 *
 * <p>2b-1b에서 core로 옮겼다 — 이 저널을 Aeron Archive로 durable화하는
 * {@code AccountJournalEntryCodec}이 core 모듈에서 이 타입을 인코딩/디코딩해야 하기 때문이다
 * ({@link JournaledOrder}가 같은 이유로 core에 있는 것과 같은 결).</p>
 *
 * <p>{@code account.disruptor.AccountEvent}는 링버퍼가 재사용하는 가변 슬롯이라, 다음 발행 때
 * 필드가 덮어써진다. 그 참조를 그대로 저널에 담으면 나중에 값이 바뀌어 기록이 훼손된다 — 그래서
 * 발행 시점의 값을 이 불변 record로 복사해 담는다({@code account.disruptor.AccountJournalEventHandler}
 * 참고).</p>
 *
 * <p><b>BUY·SELL 엔트리는 orderId=0이다</b> — 저널러가 비즈니스 핸들러보다 먼저 돌아 orderId
 * 발급 전(입력 그대로) 시점을 기록한다. 리플레이(2b-2)는 이 입력을 같은 결정론적 발급기(2b-0)에
 * 다시 넣어 orderId를 재생성한다. BUY_FILL·SELL_FILL·SETTLEMENT의 orderId·tradeId는 계좌가
 * 발급하는 값이 아니라 매칭·정산이 넘겨준 입력이라 저널 시점에 이미 확정돼 있다.</p>
 */
public record AccountJournalEntry(
    AccountEventType type,
    long orderId,
    long accountId,
    String stockCode,
    BigDecimal price,
    int quantity,
    String requestId,
    long tradeId
) {
}
