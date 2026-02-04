package com.flab.stocktradingengine.account.repository;

import java.util.List;
import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;

import com.flab.stocktradingengine.account.entity.Account;

/**
 * 계좌 저장소
 */
public interface AccountRepository extends JpaRepository<Account, Long> {

    Optional<Account> findByAccountId(String accountId);

    List<Account> findByUser_Id(Long userId);
}
