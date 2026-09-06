package com.flab.stocktradingengine.persistence;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

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
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.test.context.TestPropertySource;

import com.flab.stocktradingengine.account.entity.Account;
import com.flab.stocktradingengine.account.entity.AccountStatus;
import com.flab.stocktradingengine.support.SnowflakeIdGenerator;
import com.flab.stocktradingengine.support.SnowflakeIdGeneratorHolder;
import com.flab.stocktradingengine.trading.entity.Order;
import com.flab.stocktradingengine.trading.entity.OrderSide;
import com.flab.stocktradingengine.trading.entity.OrderStatus;
import com.flab.stocktradingengine.trading.entity.OrderType;
import com.flab.stocktradingengine.trading.repository.OrderRepository;
import com.flab.stocktradingengine.trading.service.OrderCommandService;
import com.flab.stocktradingengine.trading.service.OrderIdempotencyReader;
import com.flab.stocktradingengine.trading.service.OrderWriter;
import com.flab.stocktradingengine.user.entity.User;

/**
 * 주문 취소가 DB 에 실제로 반영되는지 검증.
 *
 * <p>취소 경로는 비트랜잭션 조회(getOrder)로 detached 된 Order 를 받아 상태만 바꾸고 저장하지 않아,
 * dirty checking 이 flush 하지 못했다. 그 결과 주문은 PENDING 으로 남고 예약증거금이 영구 잠겼다.
 * mock 기반 단위 테스트로는 잡히지 않아 실제 영속 계층으로 검증한다.</p>
 */
@DataJpaTest
@TestPropertySource(properties = "spring.sql.init.mode=never")
@DisplayName("주문 취소 - DB 영속 반영")
class OrderCancelPersistenceTest {

    /** Kafka/Redis 를 끌어오지 않는 JPA 슬라이스 전용 최소 설정. 엔티티·리포지토리가 전 모듈에 걸쳐 있어 명시 스캔. */
    @SpringBootConfiguration
    @EnableAutoConfiguration
    @EntityScan(basePackages = "com.flab.stocktradingengine")
    @EnableJpaRepositories(basePackages = "com.flab.stocktradingengine")
    static class TestConfig {
    }

    @BeforeAll
    static void initSnowflake() {
        SnowflakeIdGeneratorHolder.set(new SnowflakeIdGenerator(0L));
    }

    @Autowired
    TestEntityManager em;

    @Autowired
    OrderRepository orderRepository;

    @Test
    @DisplayName("취소 요청이 처리되면 DB 의 주문 상태가 CANCELLED 로 반영된다")
    void 취소하면_DB_상태가_CANCELLED() {
        User user = em.persist(new User());
        Account account = em.persist(
            new Account(user, new BigDecimal("100000000000"), new BigDecimal("1.00"), AccountStatus.ACTIVE));
        Order order = em.persist(Order.builder()
            .account(account)
            .stockCode("005930")
            .side(OrderSide.BUY)
            .orderType(OrderType.LIMIT)
            .price(new BigDecimal("70000"))
            .quantity(10)
            .status(OrderStatus.PENDING)
            .orderAt(Instant.now())
            .reservedMargin(new BigDecimal("7000000"))
            .requestId("req-cancel-1")
            .build());
        em.flush();
        Long orderId = order.getOrderId();
        em.clear(); // 프로덕션 getOrder(비트랜잭션) 반환처럼 detached 상태 재현

        OrderCommandService service =
            new OrderCommandService(mock(OrderWriter.class), mock(OrderIdempotencyReader.class), orderRepository);
        service.cancelOrder(orderId);

        em.flush();
        em.clear();
        Order reloaded = orderRepository.findByOrderId(orderId).orElseThrow();
        assertThat(reloaded.getStatus())
            .as("취소 후 DB 주문 상태는 CANCELLED 여야 한다")
            .isEqualTo(OrderStatus.CANCELLED);
    }
}
