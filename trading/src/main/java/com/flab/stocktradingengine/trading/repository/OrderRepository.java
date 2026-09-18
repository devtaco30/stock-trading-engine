package com.flab.stocktradingengine.trading.repository;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import com.flab.stocktradingengine.trading.entity.Order;
import com.flab.stocktradingengine.trading.entity.OrderStatus;

/**
 * 주문 저장소
 */
public interface OrderRepository extends JpaRepository<Order, Long> {

    /**
     * 주문 ID로 주문 조회
     * @param orderId 주문 ID
     * @return 주문
     */
    Optional<Order> findByOrderId(Long orderId);

    /** 멱등 판별용. request_id 전역 UNIQUE 제약과 쌍. */
    Optional<Order> findByRequestId(String requestId);

    List<Order> findByAccount_AccountIdAndStatus(Long accountId, OrderStatus status);

    /**
     * PENDING 매수 주문의 예약증거금 합계. 매수 가능 금액 계산 시 락 보유 시간 단축용.
     * reserved_margin 이 null 인 행은 SUM 에서 자동 제외된다.
     */
    @Query("SELECT COALESCE(SUM(o.reservedMargin), 0) FROM Order o " +
           "WHERE o.account.accountId = :accountId AND o.status = 'PENDING' AND o.side = 'BUY'")
    BigDecimal sumReservedMarginByAccountId(@Param("accountId") Long accountId);

    /**
     * 특정 계좌·종목의 PENDING 매도 주문 잔량(quantity - filledQuantity) 합계.
     * 매도 주문 시점의 가용 수량(보유 - 이 합계) 계산에 쓴다. 매수 sumReservedMarginByAccountId 와 대칭.
     * 부분 체결분은 이미 보유 수량에서 차감됐으므로 잔량만 미체결 매도 약정으로 집계한다.
     */
    @Query("SELECT COALESCE(SUM(o.quantity - o.filledQuantity), 0) FROM Order o " +
           "WHERE o.account.accountId = :accountId AND o.stockCode = :stockCode " +
           "AND o.status = 'PENDING' AND o.side = 'SELL'")
    long sumPendingSellQuantity(@Param("accountId") Long accountId, @Param("stockCode") String stockCode);

    /** 증거금 홀딩 중인 주문 (PENDING/FILLED 공통, T+2 정산 전까지 출금 불가 반영용) */
    List<Order> findByAccount_AccountIdAndReservedMarginIsNotNull(Long accountId);

    /**
     * 파티션 할당 시 특정 종목의 PENDING 주문을 시간 순으로 조회.
     * Account를 JOIN FETCH해 세션 종료 후에도 accountId 접근 가능하게 한다.
     */
    @Query("SELECT o FROM Order o JOIN FETCH o.account WHERE o.status = :status AND o.stockCode = :stockCode ORDER BY o.orderAt ASC")
    List<Order> findByStatusAndStockCodeOrderByOrderAtAsc(@Param("status") OrderStatus status, @Param("stockCode") String stockCode);

    /**
     * 특정 상태의 주문이 남아 있는 종목 코드를 중복 없이 조회.
     * orders 토픽이 하나라 파티션 할당만으로는 종목을 알 수 없어, 복원 후보를 여기서 얻는다.
     */
    @Query("SELECT DISTINCT o.stockCode FROM Order o WHERE o.status = :status")
    List<String> findDistinctStockCodeByStatus(@Param("status") OrderStatus status);

    /**
     * 계좌 ID와 주문 일시 범위로 주문 조회
     */
    Page<Order> findByAccount_AccountIdAndOrderAtBetweenOrderByOrderAtDesc(
        Long accountId,
        Instant startAt,
        Instant endAt,
        Pageable pageable
    );

    Page<Order> findByAccount_AccountIdAndStatusAndOrderAtBetweenOrderByOrderAtDesc(
        Long accountId,
        OrderStatus status,
        Instant startAt,
        Instant endAt,
        Pageable pageable
    );
}
