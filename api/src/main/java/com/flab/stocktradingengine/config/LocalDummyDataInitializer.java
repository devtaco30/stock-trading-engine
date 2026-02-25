package com.flab.stocktradingengine.config;

import java.math.BigDecimal;
import java.util.List;

import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Profile;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.flab.stocktradingengine.account.entity.Account;
import com.flab.stocktradingengine.account.entity.AccountStatus;
import com.flab.stocktradingengine.account.entity.Holding;
import com.flab.stocktradingengine.account.repository.AccountRepository;
import com.flab.stocktradingengine.account.repository.HoldingRepository;
import com.flab.stocktradingengine.market.repository.QuoteRepository;
import com.flab.stocktradingengine.user.entity.User;
import com.flab.stocktradingengine.user.repository.UserRepository;

import lombok.RequiredArgsConstructor;

/**
 * 로컬(profile=local) 기동 시 더미 유저·계좌·우량주 보유·balance 초기화.
 * <p>이미 유저가 있으면 스킵. stocks·quotes 는 stocks.sql·quotes.sql 로 먼저 적재된 상태를 전제로 함.</p>
 * <p>우량주 10종목은 quotes 테이블에서 현재가를 조회해 보유 데이터 생성.</p>
 */
@Component
@Profile("local")
@Order(2)
@RequiredArgsConstructor
public class LocalDummyDataInitializer implements ApplicationRunner {

	private final UserRepository userRepository;
	private final AccountRepository accountRepository;
	private final HoldingRepository holdingRepository;
	private final QuoteRepository quoteRepository;

	/** 우량주 10종목 코드 (stocks.sql 형식). 시세는 quotes 테이블에서 조회. */
	private static final List<String> BLUE_CHIP_STOCK_CODES = List.of(
		"A005930",   // 삼성전자
		"A000660",   // SK하이닉스
		"A035720",   // 카카오
		"A005380",   // 현대차
		"A035420",   // NAVER
		"A373220",   // LG에너지솔루션
		"A207940",   // 삼성바이오로직스
		"A055550",   // 신한지주
		"A086790",   // 하나금융지주
		"A105560"    // KB금융
	);

	@Override
	@Transactional
	public void run(ApplicationArguments args) {
		if (userRepository.count() > 0) {
			return;
		}

		User user1 = userRepository.save(new User());
		User user2 = userRepository.save(new User());
		User user3 = userRepository.save(new User());

		BigDecimal marginRate = new BigDecimal("0.40");
		Account account1 = accountRepository.save(
			new Account(user1, new BigDecimal("50000000"), marginRate, AccountStatus.ACTIVE));
		Account account2 = accountRepository.save(
			new Account(user2, new BigDecimal("30000000"), marginRate, AccountStatus.ACTIVE));
		Account account3 = accountRepository.save(
			new Account(user3, new BigDecimal("20000000"), marginRate, AccountStatus.ACTIVE));

		// 유저1: 삼성전자, SK하이닉스, 현대차, NAVER (인덱스 0,1,3,4)
		saveHoldingsFromQuotes(account1, List.of(0, 1, 3, 4), List.of(100, 50, 30, 20));
		// 유저2: 카카오, LG에너지솔루션, 삼성바이오로직스 (인덱스 2,5,6)
		saveHoldingsFromQuotes(account2, List.of(2, 5, 6), List.of(80, 20, 10));
		// 유저3: 신한지주, 하나금융지주, KB금융 (인덱스 7,8,9)
		saveHoldingsFromQuotes(account3, List.of(7, 8, 9), List.of(100, 80, 50));
	}

	private void saveHoldingsFromQuotes(Account account, List<Integer> blueChipIndices, List<Integer> quantities) {
		for (int i = 0; i < blueChipIndices.size(); i++) {
			String stockCode = BLUE_CHIP_STOCK_CODES.get(blueChipIndices.get(i));
			int quantity = quantities.get(i);
			quoteRepository.findById(stockCode)
				.map(q -> q.getCurrentPrice())
				.filter(price -> price != null && price.compareTo(BigDecimal.ZERO) > 0)
				.ifPresent(price -> holdingRepository.save(new Holding(account, stockCode, quantity, price)));
		}
	}
}
