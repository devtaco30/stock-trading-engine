package com.flab.stocktradingengine.service;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Optional;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.flab.stocktradingengine.account.entity.Account;
import com.flab.stocktradingengine.dto.account.AccountDetailData;
import com.flab.stocktradingengine.dto.order.BuyOrderRequest;
import com.flab.stocktradingengine.facade.AccountReadFacade;
import com.flab.stocktradingengine.market.service.QuoteService;
import com.flab.stocktradingengine.market.view.QuoteView;
import org.springframework.kafka.core.KafkaTemplate;

import com.flab.stocktradingengine.resolver.AccountAccessResolver;
import com.flab.stocktradingengine.trading.matching.OrderBookRegistry;
import com.flab.stocktradingengine.trading.repository.OrderRepository;
import com.flab.stocktradingengine.trading.service.OrderCommandService;
import com.flab.stocktradingengine.trading.service.OrderQueryService;
import com.flab.stocktradingengine.trading.view.PlaceOrderResultView;

@ExtendWith(MockitoExtension.class)
@DisplayName("OrderApiService - 가격 제한폭 검증 단위 테스트")
class OrderApiServiceTest {

    @Mock AccountAccessResolver accountAccessResolver;
    @Mock OrderCommandService orderCommandService;
    @Mock OrderQueryService orderQueryService;
    @Mock OrderBookRegistry orderBookRegistry;
    @Mock @SuppressWarnings("rawtypes") KafkaTemplate kafkaTemplate;
    @Mock OrderRepository orderRepository;
    @Mock AccountReadFacade accountReadFacade;
    @Mock QuoteService quoteService;

    @InjectMocks
    OrderApiService orderApiService;

    private static final Long USER_ID = 1L;
    private static final Long ACCOUNT_ID = 123L;
    private static final String STOCK_CODE = "005930";
    private static final BigDecimal REFERENCE_PRICE = new BigDecimal("70000");

    private Account mockAccount;

    @BeforeEach
    void setUp() {
        // registerAfterCommit()이 TransactionSynchronizationManager를 사용하므로 단위 테스트에서 활성화 필요
        TransactionSynchronizationManager.initSynchronization();
        mockAccount = mock(Account.class);
        // getAccountId()는 성공 경로에서만 호출됨 (OrderEntry 생성 시점)
        // 예외 케이스에서는 validatePriceLimit에서 조기 종료되므로 lenient 처리
        lenient().when(mockAccount.getAccountId()).thenReturn(ACCOUNT_ID);
        when(accountAccessResolver.resolveAccountOwnedAndActive(USER_ID, ACCOUNT_ID))
            .thenReturn(mockAccount);
    }

    @AfterEach
    void tearDown() {
        TransactionSynchronizationManager.clearSynchronization();
    }

    private BuyOrderRequest buyRequest(BigDecimal price) {
        return BuyOrderRequest.builder()
            .accountId(ACCOUNT_ID)
            .stockCode(STOCK_CODE)
            .orderType("LIMIT")
            .price(price)
            .quantity(10)
            .build();
    }

    // ── lastTradedPrice 기준 ──────────────────────────────────────────────────

    @Nested
    @DisplayName("lastTradedPrice 기준 검증")
    class LastTradedPriceBased {

        @BeforeEach
        void givenLastTradedPrice() {
            when(orderBookRegistry.getLastTradedPrice(STOCK_CODE))
                .thenReturn(Optional.of(REFERENCE_PRICE));
        }

        @Test
        @DisplayName("기준가 상한(+30%) 초과 시 예외 — 91001 > 91000")
        void 상한_초과_예외() {
            // 70000 * 1.3 = 91000 → 91001은 초과
            assertThatThrownBy(() -> orderApiService.placeBuyOrder(USER_ID, buyRequest(new BigDecimal("91001"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("가격 제한폭 초과");
        }

        @Test
        @DisplayName("기준가 하한(-30%) 미달 시 예외 — 48999 < 49000")
        void 하한_미달_예외() {
            // 70000 * 0.7 = 49000 → 48999는 미달
            assertThatThrownBy(() -> orderApiService.placeBuyOrder(USER_ID, buyRequest(new BigDecimal("48999"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("가격 제한폭 초과");
        }

        @Test
        @DisplayName("기준가 ±30% 경계 안 주문은 통과")
        void 유효한_가격_통과() {
            // 70000 기준 → 상한 91000, 하한 49000 → 70000은 유효
            when(orderCommandService.placeBuyOrder(any(), any(), any()))
                .thenReturn(new PlaceOrderResultView(1L, "PENDING", Instant.now().toEpochMilli(), BigDecimal.ZERO));

            assertDoesNotThrow(() -> orderApiService.placeBuyOrder(USER_ID, buyRequest(REFERENCE_PRICE)));
        }
    }

    // ── previousClose fallback ────────────────────────────────────────────────

    @Nested
    @DisplayName("previousClose fallback 검증")
    class PreviousCloseFallback {

        @BeforeEach
        void givenNoLastTradedPrice() {
            when(orderBookRegistry.getLastTradedPrice(STOCK_CODE)).thenReturn(Optional.empty());
        }

        @Test
        @DisplayName("lastTradedPrice 없으면 previousClose 기준으로 검증 — 68000 기준 88401 초과")
        void previousClose_기준_상한_초과_예외() {
            BigDecimal previousClose = new BigDecimal("68000");
            when(quoteService.getQuote(STOCK_CODE))
                .thenReturn(Optional.of(new QuoteView(STOCK_CODE, "삼성전자",
                    BigDecimal.ZERO, previousClose,
                    BigDecimal.ZERO, BigDecimal.ZERO, BigDecimal.ZERO, BigDecimal.ZERO, 0L)));

            // 68000 * 1.3 = 88400 → 88401은 초과
            assertThatThrownBy(() -> orderApiService.placeBuyOrder(USER_ID, buyRequest(new BigDecimal("88401"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("가격 제한폭 초과");
        }

        @Test
        @DisplayName("lastTradedPrice, previousClose 모두 없으면 종목 미존재 예외")
        void 기준가_없으면_예외() {
            when(quoteService.getQuote(STOCK_CODE)).thenReturn(Optional.empty());

            assertThatThrownBy(() -> orderApiService.placeBuyOrder(USER_ID, buyRequest(new BigDecimal("70000"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("기준가를 조회할 수 없는 종목");
        }
    }
}
