package com.flab.stocktradingengine.settlement.worker;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;

import jakarta.persistence.EntityManager;

/**
 * T+2 스캔 쿼리({@code findByStatusAndDueAtEpochMillisLessThanEqual})가 (status,
 * due_at_epoch_millis) 복합 인덱스를 실제로 타는지 EXPLAIN으로 확인한다. 이 쿼리는 스케줄러가
 * 주기적으로 전체 테이블을 status로 훑는 경로라 인덱스 없이는 풀스캔이 된다.
 */
@DataJpaTest
class PendingSettlementRepositoryIndexTest {

    @Autowired
    private PendingSettlementRepository pendingSettlementRepository;

    @Autowired
    private EntityManager entityManager;

    @Test
    void 스캔_쿼리가_status_dueAtEpochMillis_복합인덱스를_탄다() {
        pendingSettlementRepository.save(new PendingSettlement(1L, 1L, new BigDecimal("1000"), 100L));
        pendingSettlementRepository.save(new PendingSettlement(2L, 1L, new BigDecimal("2000"), 200L));
        entityManager.flush();

        String plan = (String) entityManager
            .createNativeQuery(
                "EXPLAIN SELECT * FROM pending_settlements WHERE status = 'PENDING' AND due_at_epoch_millis <= 150")
            .getSingleResult();

        assertThat(plan.toUpperCase()).contains("IDX_PENDING_SETTLEMENTS_STATUS_DUE_AT");
    }
}
