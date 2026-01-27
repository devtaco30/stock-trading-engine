package com.flab.stocktradingengine;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.math.BigDecimal;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flab.stocktradingengine.dto.order.BuyOrderRequest;

/**
 * 시나리오 통합 테스트
 * 
 * <p>docs/2_4단계_시나리오.md에 정의된 시나리오를 테스트합니다.
 */
@SpringBootTest(properties = {
    "spring.autoconfigure.exclude=org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration,"
        + "org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration"
})
@AutoConfigureMockMvc
@DisplayName("시나리오 통합 테스트")
class ScenarioIntegrationTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    private static final String VALID_TOKEN = "dummy-token-001";
    private static final String AUTHORIZATION_HEADER = "Authorization";
    private static final String BEARER_TOKEN = "Bearer " + VALID_TOKEN;

    @Test
    @DisplayName("1단계: 기본 주문 및 체결 - 계좌 조회")
    void scenario1_step1_accountDetail() throws Exception {
        // given
        String accountId = "account-001";

        // when & then
        mockMvc.perform(get("/api/v1/accounts/{accountId}", accountId)
                .header(AUTHORIZATION_HEADER, BEARER_TOKEN)
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.data.accountId").value(accountId))
                .andExpect(jsonPath("$.data.balance").value(3000000))
                .andExpect(jsonPath("$.data.withdrawableBalance").value(180000))
                .andExpect(jsonPath("$.data.unpaidAmount").value(4230000))
                .andExpect(jsonPath("$.data.status").value("ACTIVE"));
    }

    @Test
    @DisplayName("1단계: 기본 주문 및 체결 - 현재가 조회")
    void scenario1_step2_quote() throws Exception {
        // given
        String stockCode = "005930"; // 삼성전자

        // when & then
        mockMvc.perform(get("/api/v1/stocks/{stockCode}/quote", stockCode)
                .header(AUTHORIZATION_HEADER, BEARER_TOKEN)
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.data.stockCode").value(stockCode))
                .andExpect(jsonPath("$.data.stockName").value("삼성전자"))
                .andExpect(jsonPath("$.data.currentPrice").value(70000));
    }

    @Test
    @DisplayName("1단계: 기본 주문 및 체결 - 보유 주식 조회")
    void scenario1_step3_holdings() throws Exception {
        // given
        String accountId = "account-001";

        // when & then
        mockMvc.perform(get("/api/v1/accounts/{accountId}/holdings", accountId)
                .header(AUTHORIZATION_HEADER, BEARER_TOKEN)
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.data.data[0].stockCode").value("005930"))
                .andExpect(jsonPath("$.data.data[0].stockName").value("삼성전자"))
                .andExpect(jsonPath("$.data.data[0].quantity").value(100))
                .andExpect(jsonPath("$.data.data[0].averagePrice").value(70500));
    }

    @Test
    @DisplayName("1단계: 기본 주문 및 체결 - 미결제 내역 조회")
    void scenario1_step4_unpaidSettlements() throws Exception {
        // given
        String accountId = "account-001";

        // when & then
        mockMvc.perform(get("/api/v1/accounts/{accountId}/unpaid", accountId)
                .header(AUTHORIZATION_HEADER, BEARER_TOKEN)
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.data.data[0].stockCode").value("005930"))
                .andExpect(jsonPath("$.data.data[0].amount").value(4230000))
                .andExpect(jsonPath("$.data.data[0].status").value("PENDING"));
    }

    @Test
    @DisplayName("2단계: 회전매매 - 계좌 조회 (매도 예정금액 포함)")
    void scenario2_step1_accountDetailWithPendingSell() throws Exception {
        // given
        String accountId = "account-002";

        // when & then
        mockMvc.perform(get("/api/v1/accounts/{accountId}", accountId)
                .header(AUTHORIZATION_HEADER, BEARER_TOKEN)
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.data.accountId").value(accountId))
                .andExpect(jsonPath("$.data.pendingSellAmount").value(3184000))
                .andExpect(jsonPath("$.data.unpaidAmount").value(10254000))
                .andExpect(jsonPath("$.data.buyLimit").value(18450000));
    }

    @Test
    @DisplayName("2단계: 회전매매 - 보유 주식 조회 (카카오)")
    void scenario2_step2_holdings() throws Exception {
        // given
        String accountId = "account-002";

        // when & then
        mockMvc.perform(get("/api/v1/accounts/{accountId}/holdings", accountId)
                .header(AUTHORIZATION_HEADER, BEARER_TOKEN)
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.data.data[0].stockCode").value("035720"))
                .andExpect(jsonPath("$.data.data[0].stockName").value("카카오"))
                .andExpect(jsonPath("$.data.data[0].quantity").value(200))
                .andExpect(jsonPath("$.data.data[0].averagePrice").value(50200));
    }

    @Test
    @DisplayName("2단계: 회전매매 - 주문 내역 조회")
    void scenario2_step3_orders() throws Exception {
        // given
        String accountId = "account-002";

        // when & then
        mockMvc.perform(get("/api/v1/accounts/{accountId}/orders", accountId)
                .header(AUTHORIZATION_HEADER, BEARER_TOKEN)
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.data.data.length()").value(3))
                .andExpect(jsonPath("$.data.data[0].side").value("BUY"))
                .andExpect(jsonPath("$.data.data[0].stockCode").value("005930"))
                .andExpect(jsonPath("$.data.data[1].side").value("SELL"))
                .andExpect(jsonPath("$.data.data[1].stockCode").value("005930"))
                .andExpect(jsonPath("$.data.data[2].side").value("BUY"))
                .andExpect(jsonPath("$.data.data[2].stockCode").value("035720"));
    }

    @Test
    @DisplayName("3단계: 미수금 발생 - 미수금 조회")
    void scenario3_arrears() throws Exception {
        // given
        String accountId = "account-003";

        // when & then
        mockMvc.perform(get("/api/v1/accounts/{accountId}/arrears", accountId)
                .header(AUTHORIZATION_HEADER, BEARER_TOKEN)
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.data.amount").value(54000))
                .andExpect(jsonPath("$.data.overdueDays").value(1));
    }

    @Test
    @DisplayName("3단계: 미수금 발생 - 계좌 상태 확인 (IN_ARREARS)")
    void scenario3_accountStatus() throws Exception {
        // given
        String accountId = "account-003";

        // when & then
        mockMvc.perform(get("/api/v1/accounts/{accountId}", accountId)
                .header(AUTHORIZATION_HEADER, BEARER_TOKEN)
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.data.status").value("IN_ARREARS"));
    }

    @Test
    @DisplayName("4단계: 강제 청산 대상 - 계좌 상태 확인")
    void scenario4_accountStatus() throws Exception {
        // given
        String accountId = "account-004";

        // when & then
        mockMvc.perform(get("/api/v1/accounts/{accountId}", accountId)
                .header(AUTHORIZATION_HEADER, BEARER_TOKEN)
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.data.status").value("IN_ARREARS"));
    }

    @Test
    @DisplayName("인증 실패 - 헤더 없음")
    void authenticationFailure_noHeader() throws Exception {
        // when & then
        mockMvc.perform(get("/api/v1/accounts/account-001")
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isUnauthorized())
                .andExpect(jsonPath("$.success").value(false))
                .andExpect(jsonPath("$.code").value("AUTH_REQUIRED"))
                .andExpect(jsonPath("$.message").value("인증이 필요합니다"));
    }

    @Test
    @DisplayName("인증 실패 - 유효하지 않은 토큰")
    void authenticationFailure_invalidToken() throws Exception {
        // when & then
        mockMvc.perform(get("/api/v1/accounts/account-001")
                .header(AUTHORIZATION_HEADER, "Bearer invalid-token")
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isUnauthorized())
                .andExpect(jsonPath("$.success").value(false))
                .andExpect(jsonPath("$.code").value("INVALID_TOKEN_FORMAT"))
                .andExpect(jsonPath("$.message").value("유효하지 않은 토큰입니다"));
    }

    @Test
    @DisplayName("매수 주문 - 더미 응답 확인")
    void buyOrder_dummyResponse() throws Exception {
        // given
        BuyOrderRequest request = BuyOrderRequest.builder()
                .accountId("account-001")
                .stockCode("005930")
                .orderType("LIMIT")
                .price(new BigDecimal("70500"))
                .quantity(100)
                .build();

        // when & then
        mockMvc.perform(post("/api/v1/orders/buy")
                .header(AUTHORIZATION_HEADER, BEARER_TOKEN)
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.data.status").value("PENDING"))
                .andExpect(jsonPath("$.data.heldMargin").exists());
    }

    @Test
    @DisplayName("종목 검색")
    void stockSearch() throws Exception {
        // given
        String keyword = "삼성";

        // when & then
        mockMvc.perform(get("/api/v1/stocks/search")
                .header(AUTHORIZATION_HEADER, BEARER_TOKEN)
                .param("keyword", keyword)
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.data.data[0].stockCode").value("005930"))
                .andExpect(jsonPath("$.data.data[0].stockName").value("삼성전자"));
    }
}
