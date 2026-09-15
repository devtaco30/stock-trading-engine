package com.flab.stocktradingengine.account.projection.worker.service;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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
 */
@Component
@RequiredArgsConstructor
public class AccountProjectionUpserter {

    private final AccountProjectionRepository accountProjectionRepository;
    private final AccountProjectionHoldingRepository accountProjectionHoldingRepository;

    @Transactional
    public void upsert(AccountStateEvent event) {
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
