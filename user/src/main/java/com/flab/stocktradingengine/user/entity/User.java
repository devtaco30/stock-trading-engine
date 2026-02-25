package com.flab.stocktradingengine.user.entity;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

/**
 * 사용자 (인증된 사용자)
 * <p>계좌 소유자. 1명의 사용자가 여러 계좌를 가질 수 있음.</p>
 */
@Entity
@Table(name = "users")
public class User {

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	private Long id;

	// OneToMany accounts는 account 도메인(Account 엔티티)에서 mappedBy로 참조.
	// user 모듈은 account에 의존하지 않으므로 여기서 선언하지 않음.
	// 필요 시 account 측에서 User만 참조.
	// public no-arg: 로컬 더미 데이터 초기화 등에서 생성용.
	public User() {
	}

	public Long getId() {
		return id;
	}
}
