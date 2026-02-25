package com.flab.stocktradingengine.config;

import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Profile;
import org.springframework.core.annotation.Order;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flab.stocktradingengine.account.entity.Account;
import com.flab.stocktradingengine.account.entity.AccountStatus;
import com.flab.stocktradingengine.account.entity.Holding;
import com.flab.stocktradingengine.account.repository.AccountRepository;
import com.flab.stocktradingengine.account.repository.HoldingRepository;
import com.flab.stocktradingengine.market.repository.QuoteRepository;
import com.flab.stocktradingengine.user.entity.User;
import com.flab.stocktradingengine.user.repository.UserRepository;
import com.flab.stocktradingengine.user.token.TokenService;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * 로컬(profile=local) 기동 시 더미 유저·계좌·우량주 보유·balance 초기화.
 * <p>유저가 0명이면 7명+계좌+보유 생성. 1~6명이면 부족분만 유저·계좌 추가 후 토큰 파일 갱신. 7명 이상이면 토큰 파일만 갱신.</p>
 */
@Slf4j
@Component
@Profile("local")
@Order(2)
@RequiredArgsConstructor
public class LocalDummyDataInitializer implements ApplicationRunner {

	/** 로컬에서 목표로 하는 더미 유저 수 (user1~3: 기존, user4~7: 마켓메이킹용). */
	private static final int TARGET_DUMMY_USER_COUNT = 7;

	private final UserRepository userRepository;
	private final AccountRepository accountRepository;
	private final HoldingRepository holdingRepository;
	private final QuoteRepository quoteRepository;
	private final TokenService tokenService;
	private final ObjectMapper objectMapper;

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
		long userCount = userRepository.count();
		BigDecimal marginRate = new BigDecimal("0.40");

		if (userCount == 0) {
			// 최초: 7명 유저·계좌 생성, 1~7 모두 우량주 다양하게 보유 (마켓메이킹 시 매수·매도 호가 모두 가능)
			User user1 = userRepository.save(new User());
			User user2 = userRepository.save(new User());
			User user3 = userRepository.save(new User());
			User user4 = userRepository.save(new User());
			User user5 = userRepository.save(new User());
			User user6 = userRepository.save(new User());
			User user7 = userRepository.save(new User());

			Account account1 = accountRepository.save(
				new Account(user1, new BigDecimal("50000000"), marginRate, AccountStatus.ACTIVE));
			Account account2 = accountRepository.save(
				new Account(user2, new BigDecimal("30000000"), marginRate, AccountStatus.ACTIVE));
			Account account3 = accountRepository.save(
				new Account(user3, new BigDecimal("20000000"), marginRate, AccountStatus.ACTIVE));
			Account account4 = accountRepository.save(
				new Account(user4, new BigDecimal("100000000"), marginRate, AccountStatus.ACTIVE));
			Account account5 = accountRepository.save(
				new Account(user5, new BigDecimal("100000000"), marginRate, AccountStatus.ACTIVE));
			Account account6 = accountRepository.save(
				new Account(user6, new BigDecimal("100000000"), marginRate, AccountStatus.ACTIVE));
			Account account7 = accountRepository.save(
				new Account(user7, new BigDecimal("100000000"), marginRate, AccountStatus.ACTIVE));

			// user1~3: 기존처럼 종목별 수량
			saveHoldingsFromQuotes(account1, List.of(0, 1, 3, 4), List.of(100, 50, 30, 20));
			saveHoldingsFromQuotes(account2, List.of(2, 5, 6), List.of(80, 20, 10));
			saveHoldingsFromQuotes(account3, List.of(7, 8, 9), List.of(100, 80, 50));
			// user4~7: 우량주 10종목 전부 보유(종목당 30주) — 마켓메이킹 매도 호가용
			List<Integer> allIndices = List.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
			List<Integer> thirtyPerStock = List.of(30, 30, 30, 30, 30, 30, 30, 30, 30, 30);
			saveHoldingsFromQuotes(account4, allIndices, thirtyPerStock);
			saveHoldingsFromQuotes(account5, allIndices, thirtyPerStock);
			saveHoldingsFromQuotes(account6, allIndices, thirtyPerStock);
			saveHoldingsFromQuotes(account7, allIndices, thirtyPerStock);

			writeLocalTokensFile(List.of(user1, user2, user3, user4, user5, user6, user7),
				List.of(account1, account2, account3, account4, account5, account6, account7));
			return;
		}

		if (userCount < TARGET_DUMMY_USER_COUNT) {
			// 기존 유저만 있는 경우: 부족분만 유저·계좌 추가 (보유 없음)
			int toAdd = TARGET_DUMMY_USER_COUNT - (int) userCount;
			for (int i = 0; i < toAdd; i++) {
				User u = userRepository.save(new User());
				accountRepository.save(
					new Account(u, new BigDecimal("100000000"), marginRate, AccountStatus.ACTIVE));
			}
			log.info("로컬 더미 유저·계좌 {}명 추가 (총 {}명)", toAdd, TARGET_DUMMY_USER_COUNT);
		}

		// DB에 있는 전체 유저 기준으로 토큰 파일 갱신 (재기동 시 파일 복구용)
		writeLocalTokensFileFromDb();
	}

	/**
	 * DB에 저장된 전체 유저(id 순) 중 계좌가 있는 유저만 골라 토큰 파일 갱신.
	 */
	private void writeLocalTokensFileFromDb() {
		List<User> usersWithAccount = new ArrayList<>();
		List<Account> accountsForToken = new ArrayList<>();
		for (User u : userRepository.findAll(Sort.by("id"))) {
			accountRepository.findByUser_Id(u.getId()).stream().findFirst().ifPresent(acc -> {
				usersWithAccount.add(u);
				accountsForToken.add(acc);
			});
		}
		if (usersWithAccount.isEmpty()) {
			log.warn("계좌가 있는 유저가 없어 토큰 파일 미생성");
			return;
		}
		writeLocalTokensFile(usersWithAccount, accountsForToken);
	}

	/**
	 * 로컬 프로파일에서만: 각 유저·계좌에 대해 토큰 발급 후 JSON 파일로 저장.
	 * Python 등 외부 스크립트가 이 파일을 읽어 Authorization 헤더·accountId 로 API 호출에 사용할 수 있음.
	 */
	private void writeLocalTokensFile(List<User> users, List<Account> accounts) {
		if (users.size() != accounts.size()) {
			log.warn("유저·계좌 수 불일치로 토큰 파일 미생성");
			return;
		}
		List<Map<String, Object>> entries = new ArrayList<>();
		for (int i = 0; i < users.size(); i++) {
			User u = users.get(i);
			Account a = accounts.get(i);
			String token = tokenService.issueToken(u.getId());
			Map<String, Object> entry = new LinkedHashMap<>();
			entry.put("userId", u.getId());
			entry.put("accountId", a.getAccountId());
			entry.put("token", token);
			entries.add(entry);
			log.info("로컬 테스트용 토큰 (userId={}, accountId={}): {}", u.getId(), a.getAccountId(), token);
		}
		Path path = Paths.get(System.getProperty("user.dir")).resolve("local-tokens.json");
		try {
			Files.writeString(path, objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(entries));
			log.info("로컬 토큰 파일 저장: {}", path.toAbsolutePath());
		} catch (Exception e) {
			log.warn("로컬 토큰 파일 저장 실패: {}", e.getMessage());
		}
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
