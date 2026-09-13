package com.flab.stocktradingengine.account.disruptor.snapshot;

import java.util.Map;

import com.flab.stocktradingengine.account.disruptor.engine.AccountEngine;
import com.flab.stocktradingengine.account.disruptor.engine.AccountOrderIdGenerator;
import com.flab.stocktradingengine.account.disruptor.journal.AccountJournal;

/**
 * 계좌 엔진 전체 상태의 스냅샷(2d-2, ADR-019 "자체 스냅샷"의 계좌판). {@link AccountEngine#snapshot()}이
 * 찍고 {@link AccountEngine#restore(AccountSnapshot)}가 되살린다.
 *
 * <p>{@code generatorCounter}는 결정론적 orderId 발급기({@link AccountOrderIdGenerator})의
 * 카운터다 — matching엔 없는 계좌 고유 상태로, 복구 뒤 새 주문이 스냅샷 이전 orderId와 겹치지
 * 않고 이어 발급되게 한다. {@code journalPosition}은 이 스냅샷을 찍은 시점의 저널 위치
 * ({@link AccountJournal#position()})다 — 복구할 때 저널을 처음부터가 아니라 이 위치부터만
 * 리플레이하는 근거가 된다(2d-2b).</p>
 */
public record AccountSnapshot(
    Map<Long, AccountStateSnapshot> accountsById,
    long generatorCounter,
    long journalPosition
) {
}
