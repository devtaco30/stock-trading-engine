package com.flab.stocktradingengine.persistence;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.test.autoconfigure.orm.jpa.TestEntityManager;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.test.context.TestPropertySource;

import com.flab.stocktradingengine.api.projection.entity.AccountProjection;
import com.flab.stocktradingengine.api.projection.entity.AccountProjectionHolding;
import com.flab.stocktradingengine.api.projection.repository.AccountProjectionHoldingRepository;
import com.flab.stocktradingengine.api.projection.repository.AccountProjectionRepository;

/**
 * api 모듈이 account-projection-worker와 같은 테이블(account_projection,
 * account_projection_holding)을 독립적으로 매핑한 read-only 엔티티가 실제 컬럼과 정확히
 * 대응하는지 검증한다(계좌 상태 프로젝션 트랙 U4, CQRS Option 1의 실질 리스크 — 두 벌 매핑의
 * 컬럼 어긋남).
 */
@DataJpaTest
@TestPropertySource(properties = "spring.sql.init.mode=never")
@DisplayName("api AccountProjection 매핑 - 워커와 같은 테이블 컬럼 매핑")
class AccountProjectionMappingTest {

    /** Kafka/Redis 를 끌어오지 않는 JPA 슬라이스 전용 최소 설정. 엔티티·리포지토리가 전 모듈에 걸쳐 있어 명시 스캔. */
    @SpringBootConfiguration
    @EnableAutoConfiguration
    @EntityScan(basePackages = "com.flab.stocktradingengine")
    @EnableJpaRepositories(basePackages = "com.flab.stocktradingengine")
    static class TestConfig {
    }

    @Autowired
    TestEntityManager em;
    @Autowired
    AccountProjectionRepository accountProjectionRepository;
    @Autowired
    AccountProjectionHoldingRepository accountProjectionHoldingRepository;

    @Test
    @DisplayName("잔고 프로젝션을 저장하면 accountId·balance·seq가 그대로 재조회된다")
    void 잔고_프로젝션_저장후_재조회() {
        Instant updatedAt = Instant.now();
        em.persist(new AccountProjection(1001L, new BigDecimal("500000"), 3L, updatedAt));
        em.flush();
        em.clear();

        Optional<AccountProjection> found = accountProjectionRepository.findById(1001L);

        assertThat(found).isPresent();
        assertThat(found.get().getAccountId()).isEqualTo(1001L);
        assertThat(found.get().getBalance()).isEqualByComparingTo("500000");
        assertThat(found.get().getSeq()).isEqualTo(3L);
    }

    @Test
    @DisplayName("보유 프로젝션을 계좌 단위로 저장하면 findByAccountId가 전부 돌려준다")
    void 보유_프로젝션_계좌단위_조회() {
        em.persist(new AccountProjectionHolding(1001L, "005930", 10));
        em.persist(new AccountProjectionHolding(1001L, "000660", 5));
        em.flush();
        em.clear();

        List<AccountProjectionHolding> holdings = accountProjectionHoldingRepository.findByAccountId(1001L);

        assertThat(holdings).hasSize(2);
        assertThat(holdings)
            .extracting(AccountProjectionHolding::getStockCode, AccountProjectionHolding::getQuantity)
            .containsExactlyInAnyOrder(tuple("005930", 10), tuple("000660", 5));
    }

    @Test
    @DisplayName("보유 프로젝션이 없는 계좌는 빈 리스트를 돌려준다")
    void 보유_프로젝션_없음() {
        List<AccountProjectionHolding> holdings = accountProjectionHoldingRepository.findByAccountId(9999L);

        assertThat(holdings).isEmpty();
    }
}
