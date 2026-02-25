package com.flab.stocktradingengine.user.repository;

import org.springframework.data.jpa.repository.JpaRepository;

import com.flab.stocktradingengine.user.entity.User;

/**
 * 사용자 저장소
 */
public interface UserRepository extends JpaRepository<User, Long> {
}
