package com.flab.stocktradingengine.settlement.service;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.util.Optional;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import com.flab.stocktradingengine.account.service.AccountService;
import com.flab.stocktradingengine.settlement.entity.ProcessedFill;
import com.flab.stocktradingengine.settlement.repository.ProcessedFillRepository;
import com.flab.stocktradingengine.settlement.repository.UnpaidRepository;
import com.flab.stocktradingengine.support.SnowflakeIdGenerator;
import com.flab.stocktradingengine.trading.entity.Order;
import com.flab.stocktradingengine.trading.repository.OrderRepository;

/**
 * OrderSettlementService 체결 멱등성 테스트.
 *
 * <p>at-least-once 환경에서 같은 체결(tradeId)이 재전달돼도 부분 체결이 중복 반영되지 않아야 한다.
 * ProcessedFill(tradeId) 존재 여부로 이미 반영한 체결을 걸러낸다.</p>
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class OrderSettlementServiceTest {

    @Mock
    private AccountService accountService;
    @Mock
    private OrderRepository orderRepository;
    @Mock
    private UnpaidRepository unpaidRepository;
    @Mock
    private ProcessedFillRepository processedFillRepository;
    @Mock
    private SnowflakeIdGenerator snowflakeIdGenerator;

    @InjectMocks
    private OrderSettlementService orderSettlementService;

    private static final long TRADE_ID = 9001L;
    private static final long BUY_ORDER_ID = 8801L;
    private static final long SELL_ORDER_ID = 8802L;
    private static final int FILL_QTY = 30;
    private static final BigDecimal MATCH_PRICE = new BigDecimal("70000");

    @Test
    @DisplayName("이미 처리된 tradeId → 주문 조회·마커 저장 모두 생략 (중복 반영 방지)")
    void duplicateTradeId_skipsEverything() {
        when(processedFillRepository.existsById(TRADE_ID)).thenReturn(true);

        orderSettlementService.fillTradePartially(TRADE_ID, BUY_ORDER_ID, SELL_ORDER_ID, FILL_QTY, MATCH_PRICE);

        // 가드에서 early return → 체결 반영 로직(주문 조회)에 진입하지 않아야 한다
        verify(orderRepository, never()).findByOrderId(anyLong());
        verify(processedFillRepository, never()).save(any());
    }

    @Test
    @DisplayName("미처리 tradeId → 멱등 확인 후 처리 마커를 저장한다")
    void newTradeId_checksAndSavesMarker() {
        when(processedFillRepository.existsById(TRADE_ID)).thenReturn(false);
        // 주문 조회는 통과시키되, side가 null이라 fillBuy/Sell 내부에서 조기 반환(반영 로직 자체는 테스트 밖)
        when(orderRepository.findByOrderId(anyLong())).thenReturn(Optional.of(mock(Order.class)));

        orderSettlementService.fillTradePartially(TRADE_ID, BUY_ORDER_ID, SELL_ORDER_ID, FILL_QTY, MATCH_PRICE);

        verify(processedFillRepository).existsById(TRADE_ID);
        verify(processedFillRepository).save(any(ProcessedFill.class));
    }
}
