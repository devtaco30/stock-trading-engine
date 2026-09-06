package com.flab.stocktradingengine.persistence;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;

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
import com.flab.stocktradingengine.account.exception.InsufficientResourceException;
import com.flab.stocktradingengine.account.repository.AccountRepository;
import com.flab.stocktradingengine.account.repository.AccountStatusHistoryRepository;
import com.flab.stocktradingengine.account.repository.HoldingRepository;
import com.flab.stocktradingengine.account.service.AccountService;
import com.flab.stocktradingengine.support.SnowflakeIdGenerator;
import com.flab.stocktradingengine.support.SnowflakeIdGeneratorHolder;
import com.flab.stocktradingengine.user.entity.User;

/**
 * 출금이 가용잔고(잔고 - 예약증거금 - 미결제) 기준으로 검증되는지 확인.
 *
 * <p>기존 withdraw 는 잔고만 검사해, PENDING 매수에 예약된 증거금·미결제 미수금에 묶인 현금을
 * 출금할 수 있었다. 매수 접수(락 안에서 가용잔고 검증)와 대칭이 되도록 락 보유 중
 * committedSupplier(예약+미결제 합) 로 가용을 계산해 검증한다.</p>
 */
@DataJpaTest
@TestPropertySource(properties = "spring.sql.init.mode=never")
@DisplayName("출금 - 가용잔고(잔고 - 예약증거금 - 미결제) 기준 검증")
class AccountWithdrawAvailableBalanceTest {

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
    AccountRepository accountRepository;
    @Autowired
    HoldingRepository holdingRepository;
    @Autowired
    AccountStatusHistoryRepository accountStatusHistoryRepository;

    private AccountService accountService() {
        return new AccountService(accountRepository, holdingRepository, accountStatusHistoryRepository);
    }

    private Long persistAccount(String balance) {
        User user = em.persist(new User());
        Account account = em.persist(
            new Account(user, new BigDecimal(balance), new BigDecimal("1.00"), AccountStatus.ACTIVE));
        em.flush();
        return account.getAccountId();
    }

    @Test
    @DisplayName("가용잔고(잔고 - 예약·미결제) 초과 출금은 거부된다")
    void 가용초과_출금_거부() {
        Long accountId = persistAccount("1000000"); // 잔고 100만
        AccountService service = accountService();

        // committed(예약증거금+미결제) = 30만 → 가용 = 70만. 80만 출금은 거부돼야 한다.
        assertThatThrownBy(() ->
            service.withdraw(accountId, new BigDecimal("800000"), () -> new BigDecimal("300000")))
            .isInstanceOf(InsufficientResourceException.class);
    }

    @Test
    @DisplayName("가용잔고 이내 출금은 성공하고 잔고가 차감된다")
    void 가용이내_출금_성공() {
        Long accountId = persistAccount("1000000");
        AccountService service = accountService();

        // committed 30만 → 가용 70만. 60만 출금 성공, 잔고 40만.
        BigDecimal newBalance =
            service.withdraw(accountId, new BigDecimal("600000"), () -> new BigDecimal("300000"));

        assertThat(newBalance).isEqualByComparingTo("400000");
        em.flush();
        em.clear();
        Account reloaded = accountRepository.findByAccountId(accountId).orElseThrow();
        assertThat(reloaded.getBalance()).isEqualByComparingTo("400000");
    }
}
