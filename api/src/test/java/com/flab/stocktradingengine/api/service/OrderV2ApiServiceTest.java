package com.flab.stocktradingengine.api.service;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.flab.stocktradingengine.account.entity.Account;
import com.flab.stocktradingengine.api.dto.order.BuyOrderRequest;
import com.flab.stocktradingengine.api.dto.order.SellOrderRequest;
import com.flab.stocktradingengine.api.exception.ForbiddenException;
import com.flab.stocktradingengine.api.messaging.AeronAccountOrderSender;
import com.flab.stocktradingengine.api.redis.LtpRedisRepository;
import com.flab.stocktradingengine.api.resolver.AccountAccessResolver;
import com.flab.stocktradingengine.exception.InvalidRequestException;
import com.flab.stocktradingengine.market.service.QuoteService;
import com.flab.stocktradingengine.trading.entity.OrderSide;

/**
 * fork5 U1a·U1b — v2 주문 게이트웨이. requestId·계좌소유·가격밴드 검증(U1a) 후 계좌 인테이크로
 * 동기 발신한다(U1b, ADR-032). requestId는 클라 필수 — v1의 서버 생성 fallback(resolveRequestId)을
 * 쓰지 않는다.
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("OrderV2ApiService - v2 게이트웨이")
class OrderV2ApiServiceTest {

    @Mock AccountAccessResolver accountAccessResolver;
    @Mock LtpRedisRepository ltpRedisRepository;
    @Mock QuoteService quoteService;
    @Mock AeronAccountOrderSender aeronAccountOrderSender;

    @InjectMocks
    OrderV2ApiService orderV2ApiService;

    private static final Long USER_ID = 1L;
    private static final Long ACCOUNT_ID = 123L;
    private static final String STOCK_CODE = "005930";
    private static final BigDecimal REFERENCE_PRICE = new BigDecimal("70000");

    private Account mockAccount;

    @BeforeEach
    void setUp() {
        mockAccount = mock(Account.class);
        lenient().when(mockAccount.getAccountId()).thenReturn(ACCOUNT_ID);
    }

    private BuyOrderRequest buyRequest(BigDecimal price, String requestId) {
        return BuyOrderRequest.builder()
            .accountId(ACCOUNT_ID)
            .stockCode(STOCK_CODE)
            .orderType("LIMIT")
            .price(price)
            .quantity(10)
            .requestId(requestId)
            .build();
    }

    private SellOrderRequest sellRequest(BigDecimal price, String requestId) {
        return SellOrderRequest.builder()
            .accountId(ACCOUNT_ID)
            .stockCode(STOCK_CODE)
            .orderType("LIMIT")
            .price(price)
            .quantity(10)
            .requestId(requestId)
            .build();
    }

    @Test
    @DisplayName("requestId가 없으면 400(InvalidRequestException)으로 거부한다")
    void requestId_없으면_거부() {
        BuyOrderRequest request = buyRequest(REFERENCE_PRICE, null);

        assertThatThrownBy(() -> orderV2ApiService.placeBuyOrder(USER_ID, request))
            .isInstanceOf(InvalidRequestException.class);
    }

    @Test
    @DisplayName("requestId가 빈 문자열이면 400(InvalidRequestException)으로 거부한다")
    void requestId_빈문자열이면_거부() {
        BuyOrderRequest request = buyRequest(REFERENCE_PRICE, "   ");

        assertThatThrownBy(() -> orderV2ApiService.placeBuyOrder(USER_ID, request))
            .isInstanceOf(InvalidRequestException.class);
    }

    @Test
    @DisplayName("본인 소유가 아닌 계좌면 거부한다")
    void 미소유_계좌면_거부() {
        BuyOrderRequest request = buyRequest(REFERENCE_PRICE, "req-1");
        when(accountAccessResolver.resolveAccountOwnedAndActive(USER_ID, ACCOUNT_ID))
            .thenThrow(ForbiddenException.notOwnerOfAccount());

        assertThatThrownBy(() -> orderV2ApiService.placeBuyOrder(USER_ID, request))
            .isInstanceOf(ForbiddenException.class);
    }

    @Test
    @DisplayName("가격 제한폭을 초과하면 거부한다 — 91001 > 70000*1.3")
    void 가격제한폭_초과하면_거부() {
        BuyOrderRequest request = buyRequest(new BigDecimal("91001"), "req-1");
        when(accountAccessResolver.resolveAccountOwnedAndActive(USER_ID, ACCOUNT_ID)).thenReturn(mockAccount);
        when(ltpRedisRepository.get(STOCK_CODE)).thenReturn(Optional.of(REFERENCE_PRICE));

        assertThatThrownBy(() -> orderV2ApiService.placeBuyOrder(USER_ID, request))
            .isInstanceOf(InvalidRequestException.class)
            .hasMessageContaining("가격 제한폭 초과");
    }

    @Test
    @DisplayName("매수 — requestId·계좌소유·가격밴드 전부 통과하면 계좌 인테이크로 발신한다")
    void 정상_매수는_발신된다() {
        BuyOrderRequest request = buyRequest(REFERENCE_PRICE, "req-1");
        when(accountAccessResolver.resolveAccountOwnedAndActive(USER_ID, ACCOUNT_ID)).thenReturn(mockAccount);
        when(ltpRedisRepository.get(STOCK_CODE)).thenReturn(Optional.of(REFERENCE_PRICE));

        assertDoesNotThrow(() -> orderV2ApiService.placeBuyOrder(USER_ID, request));

        verify(aeronAccountOrderSender).send(
            eq(OrderSide.BUY), eq(ACCOUNT_ID), eq(STOCK_CODE), eq(REFERENCE_PRICE), eq(10), eq("req-1"));
    }

    @Test
    @DisplayName("매도 — requestId·계좌소유·가격밴드 전부 통과하면 계좌 인테이크로 발신한다")
    void 정상_매도는_발신된다() {
        SellOrderRequest request = sellRequest(REFERENCE_PRICE, "req-2");
        when(accountAccessResolver.resolveAccountOwnedAndActive(USER_ID, ACCOUNT_ID)).thenReturn(mockAccount);
        when(ltpRedisRepository.get(STOCK_CODE)).thenReturn(Optional.of(REFERENCE_PRICE));

        assertDoesNotThrow(() -> orderV2ApiService.placeSellOrder(USER_ID, request));

        verify(aeronAccountOrderSender).send(
            eq(OrderSide.SELL), eq(ACCOUNT_ID), eq(STOCK_CODE), eq(REFERENCE_PRICE), eq(10), eq("req-2"));
    }
}
