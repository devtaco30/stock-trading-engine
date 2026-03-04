package com.flab.stocktradingengine.trading.matching.event;

import com.flab.stocktradingengine.trading.matching.FillResult;

/**
 * 매칭 엔진이 체결을 완료했을 때 발행하는 도메인 이벤트.
 *
 * <p><b>패턴: Domain Event</b><br>
 * 매칭 결과를 settlement 계층에 전달하되,
 * trading 모듈이 settlement 모듈에 직접 의존하지 않도록 이벤트로 분리했다.
 * settlement 모듈의 {@code MatchingFillHandler} 가 이 이벤트를 구독해 DB 를 갱신한다.</p>
 *
 * <p>Spring 4.2+ 에서는 {@code ApplicationEvent} 를 상속하지 않아도
 * POJO 이벤트를 {@code ApplicationEventPublisher} 로 발행할 수 있다.
 * record 로 선언해 불변성을 보장하고 스레드 간 공유 시 안전하다.</p>
 */
public record OrderFilledEvent(
    String stockCode,
    FillResult fillResult
) {}
