package com.flab.stocktradingengine.facade;

import java.math.BigDecimal;

import org.springframework.stereotype.Component;

import com.flab.stocktradingengine.account.service.AccountService;

import lombok.RequiredArgsConstructor;

/**
 * 계좌 명령(쓰기) 전용 퍼사드.
 * <p>계좌 balance 변경, 증거금 예치/해제, 정산 차감 등 계좌에 대한 모든 쓰기 진입점을 담당한다.</p>
 * <p>입출금·정산 변제·매도 체결 시 balance 증가 등은 모두 이 퍼사드를 통해 수행한다.</p>
 */
@Component
@RequiredArgsConstructor
public class AccountCommandFacade {

	private final AccountService accountService;

	/** 입금. 반영 후 잔액 반환. (소유/ACTIVE 검증은 호출부 책임) */
	public BigDecimal deposit(Long accountId, BigDecimal amount) {
		return accountService.deposit(accountId, amount);
	}

	/** 출금. 반영 후 잔액 반환. 잔액 부족 시 예외. (소유/ACTIVE 검증은 호출부 책임) */
	public BigDecimal withdraw(Long accountId, BigDecimal amount) {
		return accountService.withdraw(accountId, amount);
	}

	// 예정: 증거금 예치/해제, 정산 차감, 매도 체결 시 balance 증가, 연체 처리 등
}
