package com.flab.stocktradingengine.trading.service;

import java.math.BigDecimal;
import java.util.Optional;
import java.util.function.Supplier;

import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.flab.stocktradingengine.exception.InvalidRequestException;
import com.flab.stocktradingengine.trading.command.BuyOrderCommand;
import com.flab.stocktradingengine.trading.command.SellOrderCommand;
import com.flab.stocktradingengine.trading.entity.Order;
import com.flab.stocktradingengine.trading.entity.OrderStatus;
import com.flab.stocktradingengine.trading.repository.OrderRepository;
import com.flab.stocktradingengine.trading.view.CancelOrderResultView;
import com.flab.stocktradingengine.trading.view.PlaceOrderResultView;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * 주문 접수·취소 오케스트레이션 (trading 도메인).
 *
 * <h3>멱등 처리 (requestId)</h3>
 * <p>같은 requestId 의 재접수(Kafka 재전달·클라이언트 재전송)를 막는다.</p>
 * <ol>
 *   <li>정상 경로: 저장 전에 requestId 로 조회(check-then-act). 이미 있으면 기존 주문을 그대로 반환.</li>
 *   <li>경쟁 안전망: 그래도 동시 삽입이 겹치면 {@code request_id} UNIQUE 가 뒤늦은 쪽을 막는다.
 *       이때는 승자 주문을 새 트랜잭션({@link OrderIdempotencyReader})으로 조회해 반환한다.</li>
 * </ol>
 * <p>저장은 별도 트랜잭션 빈({@link OrderWriter})이 담당한다. 이 클래스에는 저장 트랜잭션이 없어,
 * 저장이 롤백되어도 여기서의 복구 조회가 오염된 트랜잭션을 재사용하지 않는다.</p>
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class OrderCommandService {

    private final OrderWriter orderWriter;
    private final OrderIdempotencyReader idempotencyReader;
    private final OrderRepository orderRepository;

    /**
     * 매수 주문 접수 (증거금 예약).
     * @param unpaidSumSupplier 계좌의 미결제 미수금 합계 공급자. 저장 트랜잭션의 락 획득 후 호출된다.
     */
    public PlaceOrderResultView placeBuyOrder(BuyOrderCommand command,
            Supplier<BigDecimal> unpaidSumSupplier) {
        Optional<Order> duplicate = idempotencyReader.findByRequestId(command.requestId());
        if (duplicate.isPresent()) {
            return toView(duplicate.get());
        }

        try {
            return orderWriter.writeBuyOrder(command, unpaidSumSupplier);
        } catch (DataIntegrityViolationException race) {
            return recoverDuplicate(command.requestId(), race);
        }
    }

    /**
     * 매도 주문 접수.
     */
    public PlaceOrderResultView placeSellOrder(SellOrderCommand command) {
        Optional<Order> duplicate = idempotencyReader.findByRequestId(command.requestId());
        if (duplicate.isPresent()) {
            return toView(duplicate.get());
        }

        try {
            return orderWriter.writeSellOrder(command);
        } catch (DataIntegrityViolationException race) {
            return recoverDuplicate(command.requestId(), race);
        }
    }

    /**
     * 주문 취소 (PENDING만 가능, 매수 시 예약 증거금 반환).
     * 쓰기 경로이므로 이 트랜잭션 안에서 orderId 로 직접 로드해 managed 상태로 만든다.
     * 그래야 order.cancel() 의 상태 변경이 dirty checking 으로 DB 에 반영된다.
     * (조회 결과를 넘겨받으면 detached 라 상태 변경이 flush 되지 않는다.)
     */
    @Transactional
    public CancelOrderResultView cancelOrder(Long orderId) {
        Order order = orderRepository.findByOrderId(orderId)
            .orElseThrow(() -> new IllegalArgumentException("Order not found: " + orderId));
        if (order.getStatus() != OrderStatus.PENDING) {
            throw new InvalidRequestException("취소 가능한 상태가 아님: " + order.getStatus());
        }
        BigDecimal returnedMargin = order.getReservedMargin() != null ? order.getReservedMargin() : BigDecimal.ZERO;
        order.cancel();
        return new CancelOrderResultView(order.getOrderId(), returnedMargin);
    }

    /** 동시 삽입 경쟁에서 진 경우 — 먼저 저장된 승자 주문을 새 트랜잭션으로 조회해 반환. */
    private PlaceOrderResultView recoverDuplicate(String requestId, DataIntegrityViolationException cause) {
        log.warn("[주문 접수] requestId 중복 저장 경쟁 — 기존 주문 반환: requestId={}", requestId);
        return idempotencyReader.findByRequestId(requestId)
            .map(this::toView)
            .orElseThrow(() -> new IllegalStateException("중복 주문 조회 실패: requestId=" + requestId, cause));
    }

    private PlaceOrderResultView toView(Order order) {
        return new PlaceOrderResultView(
            order.getOrderId(),
            order.getStatus().name(),
            order.getOrderAt().toEpochMilli(),
            order.getReservedMargin());
    }
}
