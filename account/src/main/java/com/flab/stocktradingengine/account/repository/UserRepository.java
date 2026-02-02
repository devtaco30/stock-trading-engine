package com.flab.stocktradingengine.account.repository;

import org.springframework.data.jpa.repository.JpaRepository;

import com.flab.stocktradingengine.account.entity.User;

/**
 * 사용자 저장소
 */
public interface UserRepository extends JpaRepository<User, Long> {
}
