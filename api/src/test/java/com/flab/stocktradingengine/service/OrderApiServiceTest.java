package com.flab.stocktradingengine.service;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import com.flab.stocktradingengine.kafka.event.OrderRequestEvent;
import org.springframework.kafka.core.KafkaTemplate;

import com.flab.stocktradingengine.account.entity.Account;
import com.flab.stocktradingengine.api.dto.order.BuyOrderRequest;
import com.flab.stocktradingengine.api.redis.LtpRedisRepository;
import com.flab.stocktradingengine.api.resolver.AccountAccessResolver;
import com.flab.stocktradingengine.api.service.OrderApiService;
import com.flab.stocktradingengine.exception.InvalidRequestException;
import com.flab.stocktradingengine.exception.ResourceNotFoundException;
import com.flab.stocktradingengine.market.service.QuoteService;
import com.flab.stocktradingengine.market.view.QuoteView;
import com.flab.stocktradingengine.trading.service.OrderQueryService;

@ExtendWith(MockitoExtension.class)
@DisplayName("OrderApiService - 가격 제한폭 검증 단위 테스트")
class OrderApiServiceTest {

    @Mock AccountAccessResolver accountAccessResolver;
    @Mock OrderQueryService orderQueryService;
    @Mock LtpRedisRepository ltpRedisRepository;
    @Mock QuoteService quoteService;
    @SuppressWarnings("rawtypes")
    @Mock KafkaTemplate kafkaTemplate;

    @InjectMocks
    OrderApiService orderApiService;

    private static final Long USER_ID = 1L;
    private static final Long ACCOUNT_ID = 123L;
    private static final String STOCK_CODE = "005930";
    private static final BigDecimal REFERENCE_PRICE = new BigDecimal("70000");

    private Account mockAccount;

    @BeforeEach
    void setUp() {
        mockAccount = mock(Account.class);
        lenient().when(mockAccount.getAccountId()).thenReturn(ACCOUNT_ID);
        when(accountAccessResolver.resolveAccountOwnedAndActive(USER_ID, ACCOUNT_ID))
            .thenReturn(mockAccount);
    }

    private BuyOrderRequest buyRequest(BigDecimal price) {
        return buyRequest(price, null);
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

    // ── 멱등키(requestId) 생성 ────────────────────────────────────────────────

    @Nested
    @DisplayName("멱등키(requestId) 생성")
    class RequestIdGeneration {

        @BeforeEach
        void givenValidPrice() {
            when(ltpRedisRepository.get(STOCK_CODE)).thenReturn(Optional.of(REFERENCE_PRICE));
        }

        private OrderRequestEvent capturePublishedEvent() {
            ArgumentCaptor<OrderRequestEvent> captor = ArgumentCaptor.forClass(OrderRequestEvent.class);
            verify(kafkaTemplate).send(any(), any(), captor.capture());
            return captor.getValue();
        }

        @Test
        @DisplayName("클라이언트가 requestId 를 보내면 그대로 이벤트에 실린다")
        void usesClientRequestId() {
            orderApiService.placeBuyOrder(USER_ID, buyRequest(REFERENCE_PRICE, "client-req-42"));

            OrderRequestEvent event = capturePublishedEvent();
            assertThat(event.requestId()).isEqualTo("client-req-42");
        }

        @Test
        @DisplayName("클라이언트가 requestId 를 안 보내면 서버가 UUID 를 생성한다")
        void generatesRequestIdWhenAbsent() {
            orderApiService.placeBuyOrder(USER_ID, buyRequest(REFERENCE_PRICE, null));

            OrderRequestEvent event = capturePublishedEvent();
            assertThat(event.requestId()).isNotBlank();
        }
    }

    // ── lastTradedPrice 기준 ──────────────────────────────────────────────────

    @Nested
    @DisplayName("lastTradedPrice 기준 검증")
    class LastTradedPriceBased {

        @BeforeEach
        void givenLastTradedPrice() {
            when(ltpRedisRepository.get(STOCK_CODE)).thenReturn(Optional.of(REFERENCE_PRICE));
        }

        @Test
        @DisplayName("기준가 상한(+30%) 초과 시 예외 — 91001 > 91000")
        void 상한_초과_예외() {
            // 70000 * 1.3 = 91000 → 91001은 초과
            assertThatThrownBy(() -> orderApiService.placeBuyOrder(USER_ID, buyRequest(new BigDecimal("91001"))))
                .isInstanceOf(InvalidRequestException.class)
                .hasMessageContaining("가격 제한폭 초과");
        }

        @Test
        @DisplayName("기준가 하한(-30%) 미달 시 예외 — 48999 < 49000")
        void 하한_미달_예외() {
            // 70000 * 0.7 = 49000 → 48999는 미달
            assertThatThrownBy(() -> orderApiService.placeBuyOrder(USER_ID, buyRequest(new BigDecimal("48999"))))
                .isInstanceOf(InvalidRequestException.class)
                .hasMessageContaining("가격 제한폭 초과");
        }

        @Test
        @DisplayName("기준가 ±30% 경계 안 주문은 통과")
        void 유효한_가격_통과() {
            // 70000 기준 → 상한 91000, 하한 49000 → 70000은 유효
            assertDoesNotThrow(() -> orderApiService.placeBuyOrder(USER_ID, buyRequest(REFERENCE_PRICE)));
        }
    }

    // ── previousClose fallback ────────────────────────────────────────────────

    @Nested
    @DisplayName("previousClose fallback 검증")
    class PreviousCloseFallback {

        @BeforeEach
        void givenNoLastTradedPrice() {
            when(ltpRedisRepository.get(STOCK_CODE)).thenReturn(Optional.empty());
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
                .isInstanceOf(InvalidRequestException.class)
                .hasMessageContaining("가격 제한폭 초과");
        }

        @Test
        @DisplayName("lastTradedPrice, previousClose 모두 없으면 종목 미존재 예외")
        void 기준가_없으면_예외() {
            when(quoteService.getQuote(STOCK_CODE)).thenReturn(Optional.empty());

            assertThatThrownBy(() -> orderApiService.placeBuyOrder(USER_ID, buyRequest(new BigDecimal("70000"))))
                .isInstanceOf(ResourceNotFoundException.class)
                .hasMessageContaining("기준가를 조회할 수 없는 종목");
        }
    }
}
