package com.flab.stocktradingengine.settlement.repository;

import java.util.List;
import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import com.flab.stocktradingengine.settlement.entity.Unpaid;
import com.flab.stocktradingengine.settlement.entity.UnpaidStatus;

/**
 * 미결제 저장소
 */
public interface UnpaidRepository extends JpaRepository<Unpaid, Long> {

    List<Unpaid> findByAccount_AccountId(Long accountId);

    List<Unpaid> findByAccount_AccountIdAndStatus(Long accountId, UnpaidStatus status);

    Optional<Unpaid> findByUnpaidId(String unpaidId);

    /**
     * status = PENDING 인 경우에만 SETTLED 로 원자적 업데이트.
     * 체크와 변경을 단일 쿼리로 처리해 dirty checking 의존을 제거하고 동시 요청 시 중복 정산을 방지한다.
     *
     * @return 실제로 변경된 행 수 (0 = 이미 정산됐거나 해당 계좌의 건이 아님)
     */
    @Modifying(clearAutomatically = true)
    @Query("UPDATE Unpaid u SET u.status = 'SETTLED' WHERE u.unpaidId = :unpaidId AND u.account.accountId = :accountId AND u.status = 'PENDING'")
    int markSettledIfPending(@Param("unpaidId") String unpaidId, @Param("accountId") Long accountId);
}
