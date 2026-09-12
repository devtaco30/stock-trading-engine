package com.flab.stocktradingengine.settlement.worker;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;

import jakarta.persistence.EntityManager;

/**
 * T+2 스캔 쿼리({@code findByStatusAndDueAtEpochMillisLessThanEqual})가 기대한
 * (status, due_at_epoch_millis) 복합 인덱스를 갖는지 확인한다.
 *
 * <p>실행계획(EXPLAIN) 자체는 자동테스트에 넣지 않았다 — Postgres 비용 기반 플래너는 이 테스트가
 * 다루는 몇 건짜리 테이블에서는 인덱스가 있어도 항상 Seq Scan을 고른다(작은 테이블에서는 그게
 * 실제로 더 싸다). 그래서 여기서는 "그 인덱스가 존재하는지"를 pg_indexes 메타데이터로 고정하고,
 * "실제로 쓰이는지"는 대량 데이터로 수동 검증했다:</p>
 *
 * <p>2026-09-08, Postgres 9702, pending_settlements 20만 행(PENDING/SETTLED 절반씩,
 * due_at_epoch_millis 1~200000 균등분포)을 채운 뒤
 * {@code EXPLAIN SELECT * FROM pending_settlements WHERE status='PENDING' AND due_at_epoch_millis <= 50000}
 * 실행 결과:</p>
 * <pre>
 * Bitmap Heap Scan on pending_settlements  (cost=783.33..2817.51 rows=24479 width=37)
 *   Recheck Cond: (((status)::text = 'PENDING'::text) AND (due_at_epoch_millis &lt;= 50000))
 *   -&gt;  Bitmap Index Scan on idx_pending_settlements_status_due_at  (cost=0.00..777.21 rows=24479 width=0)
 *         Index Cond: (((status)::text = 'PENDING'::text) AND (due_at_epoch_millis &lt;= 50000))
 * </pre>
 * <p>Seq Scan이 아니라 인덱스(Bitmap Index Scan)를 탔다. 검증 후 이 데이터는 삭제했다.</p>
 */
@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
class PendingSettlementRepositoryIndexTest {

    @Autowired
    private EntityManager entityManager;

    @Test
    void status_dueAtEpochMillis_복합인덱스가_존재한다() {
        Boolean indexExists = (Boolean) entityManager
            .createNativeQuery(
                "SELECT EXISTS (SELECT 1 FROM pg_indexes WHERE tablename = 'pending_settlements' "
                    + "AND indexname = 'idx_pending_settlements_status_due_at')")
            .getSingleResult();

        assertThat(indexExists).isTrue();
    }
}
