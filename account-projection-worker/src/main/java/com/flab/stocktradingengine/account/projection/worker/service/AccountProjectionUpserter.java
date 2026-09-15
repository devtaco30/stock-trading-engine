package com.flab.stocktradingengine.account.projection.worker.service;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.flab.stocktradingengine.account.projection.worker.entity.AccountProjection;
import com.flab.stocktradingengine.account.projection.worker.entity.AccountProjectionHolding;
import com.flab.stocktradingengine.account.projection.worker.repository.AccountProjectionHoldingRepository;
import com.flab.stocktradingengine.account.projection.worker.repository.AccountProjectionRepository;
import com.flab.stocktradingengine.kafka.event.AccountStateEvent;

import lombok.RequiredArgsConstructor;

/**
 * {@code AccountStateEvent}(full-state)를 read model에 stale-guard upsert하는 트랜잭션
 * 경계(계좌 상태 영속/프로젝션 트랙 U3).
 *
 * <p>계좌가 처음 보이면(findById 빈 값) seq=0인 새 행을 만들어 같은 stale-guard 경로를
 * 태운다 — 계좌별 seq는 1부터 시작하므로(account-disruptor {@code AccountState}, U1) 항상
 * {@code newSeq(>=1) > 0}이 성립해 첫 이벤트가 특별 취급 없이 그대로 반영된다.</p>
 *
 * <p>보유는 델타가 아니라 매번 전체를 담으므로, 반영이 확정되면(stale이 아니면) 그 계좌의
 * 기존 보유 행을 전부 지우고 이벤트의 map으로 다시 채운다(전체 교체) — 부분 갱신이면 매도로
 * 사라진 종목이 옛 값으로 남는다.</p>
 *
 * <h3>배치 coalesce (U3-ii)</h3>
 * <p>{@link #upsertBatch}는 한 poll 배치 안에 같은 계좌 이벤트가 여러 개 섞여 있으면 계좌당
 * 최신 seq 이벤트 하나로 합친 뒤 반영한다 — DB 쓰기가 "배치 속 서로 다른 계좌 수"에 비례하게
 * 잡는다(체결 수가 아니라). 배치 전체를 한 트랜잭션으로 묶어, 컨슈머가 이 메서드 반환(=커밋)
 * 뒤에만 배치 전체를 ack하게 한다.</p>
 */
@Component
@RequiredArgsConstructor
public class AccountProjectionUpserter {

    private final AccountProjectionRepository accountProjectionRepository;
    private final AccountProjectionHoldingRepository accountProjectionHoldingRepository;

    @Transactional
    public void upsert(AccountStateEvent event) {
        applyOne(event);
    }

    @Transactional
    public void upsertBatch(List<AccountStateEvent> events) {
        for (AccountStateEvent event : latestPerAccount(events).values()) {
            applyOne(event);
        }
    }

    /** 배치를 순서대로 훑어 계좌마다 seq가 가장 큰 이벤트만 남긴다. */
    private Map<Long, AccountStateEvent> latestPerAccount(List<AccountStateEvent> events) {
        Map<Long, AccountStateEvent> latestByAccountId = new LinkedHashMap<>();
        for (AccountStateEvent event : events) {
            latestByAccountId.merge(event.accountId(), event,
                (existing, incoming) -> incoming.seq() > existing.seq() ? incoming : existing);
        }
        return latestByAccountId;
    }

    private void applyOne(AccountStateEvent event) {
        AccountProjection projection = accountProjectionRepository.findById(event.accountId())
            .orElseGet(() -> new AccountProjection(event.accountId(), BigDecimal.ZERO, 0L, Instant.EPOCH));

        Instant updatedAt = Instant.ofEpochMilli(event.epochMillis());
        boolean applied = projection.applyIfNewer(event.balance(), event.seq(), updatedAt);
        if (!applied) {
            return; // stale — 순서 역전이거나 재도착(멱등), 무시
        }

        accountProjectionRepository.save(projection);
        replaceHoldings(event.accountId(), event.holdings());
    }

    private void replaceHoldings(long accountId, Map<String, Integer> holdings) {
        accountProjectionHoldingRepository.deleteByAccountId(accountId);
        List<AccountProjectionHolding> rows = holdings.entrySet().stream()
            .map(entry -> new AccountProjectionHolding(accountId, entry.getKey(), entry.getValue()))
            .toList();
        accountProjectionHoldingRepository.saveAll(rows);
    }
}
