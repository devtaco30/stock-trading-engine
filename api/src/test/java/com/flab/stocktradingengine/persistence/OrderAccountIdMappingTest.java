package com.flab.stocktradingengine.persistence;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.time.Instant;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.test.autoconfigure.orm.jpa.TestEntityManager;
import org.springframework.test.context.TestPropertySource;

import com.flab.stocktradingengine.account.entity.Account;
import com.flab.stocktradingengine.account.entity.AccountStatus;
import com.flab.stocktradingengine.support.SnowflakeIdGenerator;
import com.flab.stocktradingengine.support.SnowflakeIdGeneratorHolder;
import com.flab.stocktradingengine.trading.entity.Order;
import com.flab.stocktradingengine.trading.entity.OrderSide;
import com.flab.stocktradingengine.trading.entity.OrderStatus;
import com.flab.stocktradingengine.trading.entity.OrderType;
import com.flab.stocktradingengine.user.entity.User;

/**
 * Order.accountId 가 대리 PK(id)가 아니라 Snowflake account_id 를 저장하는지 검증.
 *
 * <p>FK가 Account.account_id(Snowflake, UNIQUE)를 참조하도록 고치기 전에는
 * order.getAccountId() 가 accounts.id(대리키)를 반환해 정산의 계좌 조회가 어긋났다.</p>
 */
@DataJpaTest
@TestPropertySource(properties = "spring.sql.init.mode=never")
@DisplayName("Order.accountId 매핑 - Snowflake accountId 저장")
class OrderAccountIdMappingTest {

    /** 앱의 Kafka/Redis 설정을 끌어오지 않는 JPA 슬라이스 전용 최소 설정. 엔티티는 전 모듈에 걸쳐 있어 명시 스캔. */
    @SpringBootConfiguration
    @EnableAutoConfiguration
    @EntityScan(basePackages = "com.flab.stocktradingengine")
    static class TestConfig {
    }

    @BeforeAll
    static void initSnowflake() {
        SnowflakeIdGeneratorHolder.set(new SnowflakeIdGenerator(0L));
    }

    @Autowired
    TestEntityManager em;

    @Test
    @DisplayName("order.getAccountId() 는 대리 PK 가 아니라 Snowflake accountId 를 돌려준다")
    void orderAccountIdIsSnowflakeNotSurrogatePk() {
        User user = em.persist(new User());
        Account account = em.persist(
            new Account(user, new BigDecimal("100000000000"), new BigDecimal("1.00"), AccountStatus.ACTIVE));
        em.flush();

        Long snowflakeAccountId = account.getAccountId();
        Long surrogatePk = account.getId();
        // 전제: 두 식별자가 실제로 다른 값이어야 검증이 의미 있다
        assertThat(snowflakeAccountId).isNotEqualTo(surrogatePk);

        Order order = Order.builder()
            .account(account)
            .stockCode("005930")
            .side(OrderSide.BUY)
            .orderType(OrderType.LIMIT)
            .price(new BigDecimal("70000"))
            .quantity(10)
            .status(OrderStatus.PENDING)
            .orderAt(Instant.now())
            .requestId("req-mapping-1")
            .build();
        em.persist(order);
        em.flush();
        em.clear();

        Order reloaded = em.find(Order.class, order.getId());
        assertThat(reloaded.getAccountId())
            .as("order.getAccountId() 는 Snowflake accountId 와 같아야 한다")
            .isEqualTo(snowflakeAccountId);
        assertThat(reloaded.getAccountId())
            .as("대리 PK 가 아니어야 한다")
            .isNotEqualTo(surrogatePk);
        assertThat(reloaded.getAccount().getAccountId())
            .isEqualTo(reloaded.getAccountId());
    }
}
