package com.flab.stocktradingengine;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestClient;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.JsonPath;
import com.flab.stocktradingengine.api.dto.order.BuyOrderRequest;

/**
 * 시나리오 통합 테스트
 *
 * <p>docs/2_4단계_시나리오.md에 정의된 시나리오를 테스트합니다.
 * <p>실제 내장 서버(RANDOM_PORT)를 띄우고 RestClient로 HTTP 요청을 보냅니다.
 */
@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    properties = {
        "spring.autoconfigure.exclude=org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration,"
            + "org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration"
    }
)
@DisplayName("시나리오 통합 테스트")
class ScenarioIntegrationTest {

    @LocalServerPort
    private int port;

    private RestClient restClient;

    private final ObjectMapper objectMapper = new ObjectMapper();

    private static final String VALID_TOKEN = "dummy-token-001";
    private static final String AUTHORIZATION_HEADER = "Authorization";
    private static final String BEARER_TOKEN = "Bearer " + VALID_TOKEN;

    @BeforeEach
    void setUp() {
        restClient = RestClient.builder()
            .baseUrl("http://localhost:" + port)
            .build();
    }

    @Test
    @DisplayName("1단계: 기본 주문 및 체결 - 계좌 조회")
    void scenario1_step1_accountDetail() {
        String accountId = "account-001";

        ResponseEntity<String> response = restClient.get()
            .uri("/api/v1/accounts/{accountId}", accountId)
            .header(AUTHORIZATION_HEADER, BEARER_TOKEN)
            .retrieve()
            .toEntity(String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        String body = response.getBody();
        assertThat(JsonPath.<Boolean>read(body, "$.success")).isTrue();
        assertThat(JsonPath.<String>read(body, "$.data.accountId")).isEqualTo(accountId);
        assertThat(JsonPath.<Integer>read(body, "$.data.balance")).isEqualTo(3000000);
        assertThat(JsonPath.<Integer>read(body, "$.data.withdrawableBalance")).isEqualTo(180000);
        assertThat(JsonPath.<Integer>read(body, "$.data.unpaidAmount")).isEqualTo(4230000);
        assertThat(JsonPath.<String>read(body, "$.data.status")).isEqualTo("ACTIVE");
    }

    @Test
    @DisplayName("1단계: 기본 주문 및 체결 - 현재가 조회")
    void scenario1_step2_quote() {
        String stockCode = "005930";

        ResponseEntity<String> response = restClient.get()
            .uri("/api/v1/stocks/{stockCode}/quote", stockCode)
            .header(AUTHORIZATION_HEADER, BEARER_TOKEN)
            .retrieve()
            .toEntity(String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        String body = response.getBody();
        assertThat(JsonPath.<Boolean>read(body, "$.success")).isTrue();
        assertThat(JsonPath.<String>read(body, "$.data.stockCode")).isEqualTo(stockCode);
        assertThat(JsonPath.<String>read(body, "$.data.stockName")).isEqualTo("삼성전자");
        assertThat(JsonPath.<Integer>read(body, "$.data.currentPrice")).isEqualTo(70000);
    }

    @Test
    @DisplayName("1단계: 기본 주문 및 체결 - 보유 주식 조회")
    void scenario1_step3_holdings() {
        String accountId = "account-001";

        ResponseEntity<String> response = restClient.get()
            .uri("/api/v1/accounts/{accountId}/holdings", accountId)
            .header(AUTHORIZATION_HEADER, BEARER_TOKEN)
            .retrieve()
            .toEntity(String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        String body = response.getBody();
        assertThat(JsonPath.<Boolean>read(body, "$.success")).isTrue();
        assertThat(JsonPath.<String>read(body, "$.data.data[0].stockCode")).isEqualTo("005930");
        assertThat(JsonPath.<String>read(body, "$.data.data[0].stockName")).isEqualTo("삼성전자");
        assertThat(JsonPath.<Integer>read(body, "$.data.data[0].quantity")).isEqualTo(100);
        assertThat(JsonPath.<Integer>read(body, "$.data.data[0].averagePrice")).isEqualTo(70500);
    }

    @Test
    @DisplayName("1단계: 기본 주문 및 체결 - 미결제 내역 조회")
    void scenario1_step4_unpaidSettlements() {
        String accountId = "account-001";

        ResponseEntity<String> response = restClient.get()
            .uri("/api/v1/accounts/{accountId}/unpaid", accountId)
            .header(AUTHORIZATION_HEADER, BEARER_TOKEN)
            .retrieve()
            .toEntity(String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        String body = response.getBody();
        assertThat(JsonPath.<Boolean>read(body, "$.success")).isTrue();
        assertThat(JsonPath.<String>read(body, "$.data.data[0].stockCode")).isEqualTo("005930");
        assertThat(JsonPath.<Integer>read(body, "$.data.data[0].amount")).isEqualTo(4230000);
        assertThat(JsonPath.<String>read(body, "$.data.data[0].status")).isEqualTo("PENDING");
    }

    @Test
    @DisplayName("2단계: 회전매매 - 계좌 조회 (매도 예정금액 포함)")
    void scenario2_step1_accountDetailWithPendingSell() {
        String accountId = "account-002";

        ResponseEntity<String> response = restClient.get()
            .uri("/api/v1/accounts/{accountId}", accountId)
            .header(AUTHORIZATION_HEADER, BEARER_TOKEN)
            .retrieve()
            .toEntity(String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        String body = response.getBody();
        assertThat(JsonPath.<Boolean>read(body, "$.success")).isTrue();
        assertThat(JsonPath.<String>read(body, "$.data.accountId")).isEqualTo(accountId);
        assertThat(JsonPath.<Integer>read(body, "$.data.pendingSellAmount")).isEqualTo(3184000);
        assertThat(JsonPath.<Integer>read(body, "$.data.unpaidAmount")).isEqualTo(10254000);
        assertThat(JsonPath.<Integer>read(body, "$.data.buyLimit")).isEqualTo(18450000);
    }

    @Test
    @DisplayName("2단계: 회전매매 - 보유 주식 조회 (카카오)")
    void scenario2_step2_holdings() {
        String accountId = "account-002";

        ResponseEntity<String> response = restClient.get()
            .uri("/api/v1/accounts/{accountId}/holdings", accountId)
            .header(AUTHORIZATION_HEADER, BEARER_TOKEN)
            .retrieve()
            .toEntity(String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        String body = response.getBody();
        assertThat(JsonPath.<Boolean>read(body, "$.success")).isTrue();
        assertThat(JsonPath.<String>read(body, "$.data.data[0].stockCode")).isEqualTo("035720");
        assertThat(JsonPath.<String>read(body, "$.data.data[0].stockName")).isEqualTo("카카오");
        assertThat(JsonPath.<Integer>read(body, "$.data.data[0].quantity")).isEqualTo(200);
        assertThat(JsonPath.<Integer>read(body, "$.data.data[0].averagePrice")).isEqualTo(50200);
    }

    @Test
    @DisplayName("2단계: 회전매매 - 주문 내역 조회")
    void scenario2_step3_orders() {
        String accountId = "account-002";

        ResponseEntity<String> response = restClient.get()
            .uri("/api/v1/accounts/{accountId}/orders", accountId)
            .header(AUTHORIZATION_HEADER, BEARER_TOKEN)
            .retrieve()
            .toEntity(String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        String body = response.getBody();
        assertThat(JsonPath.<Boolean>read(body, "$.success")).isTrue();
        assertThat(JsonPath.<Integer>read(body, "$.data.data.length()")).isEqualTo(3);
        assertThat(JsonPath.<String>read(body, "$.data.data[0].side")).isEqualTo("BUY");
        assertThat(JsonPath.<String>read(body, "$.data.data[0].stockCode")).isEqualTo("005930");
        assertThat(JsonPath.<String>read(body, "$.data.data[1].side")).isEqualTo("SELL");
        assertThat(JsonPath.<String>read(body, "$.data.data[1].stockCode")).isEqualTo("005930");
        assertThat(JsonPath.<String>read(body, "$.data.data[2].side")).isEqualTo("BUY");
        assertThat(JsonPath.<String>read(body, "$.data.data[2].stockCode")).isEqualTo("035720");
    }

    @Test
    @DisplayName("3단계: 미수금 발생 - 미수금 조회")
    void scenario3_arrears() {
        String accountId = "account-003";

        ResponseEntity<String> response = restClient.get()
            .uri("/api/v1/accounts/{accountId}/arrears", accountId)
            .header(AUTHORIZATION_HEADER, BEARER_TOKEN)
            .retrieve()
            .toEntity(String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        String body = response.getBody();
        assertThat(JsonPath.<Boolean>read(body, "$.success")).isTrue();
        assertThat(JsonPath.<Integer>read(body, "$.data.amount")).isEqualTo(54000);
        assertThat(JsonPath.<Integer>read(body, "$.data.overdueDays")).isEqualTo(1);
    }

    @Test
    @DisplayName("3단계: 미수금 발생 - 계좌 상태 확인 (IN_ARREARS)")
    void scenario3_accountStatus() {
        String accountId = "account-003";

        ResponseEntity<String> response = restClient.get()
            .uri("/api/v1/accounts/{accountId}", accountId)
            .header(AUTHORIZATION_HEADER, BEARER_TOKEN)
            .retrieve()
            .toEntity(String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        String body = response.getBody();
        assertThat(JsonPath.<Boolean>read(body, "$.success")).isTrue();
        assertThat(JsonPath.<String>read(body, "$.data.status")).isEqualTo("IN_ARREARS");
    }

    @Test
    @DisplayName("4단계: 강제 청산 대상 - 계좌 상태 확인")
    void scenario4_accountStatus() {
        String accountId = "account-004";

        ResponseEntity<String> response = restClient.get()
            .uri("/api/v1/accounts/{accountId}", accountId)
            .header(AUTHORIZATION_HEADER, BEARER_TOKEN)
            .retrieve()
            .toEntity(String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        String body = response.getBody();
        assertThat(JsonPath.<Boolean>read(body, "$.success")).isTrue();
        assertThat(JsonPath.<String>read(body, "$.data.status")).isEqualTo("IN_ARREARS");
    }

    @Test
    @DisplayName("인증 실패 - 헤더 없음")
    void authenticationFailure_noHeader() {
        ResponseEntity<String> response = restClient.get()
            .uri("/api/v1/accounts/account-001")
            .exchange((request, resp) -> {
                String respBody = resp.getBody() != null ? new String(resp.getBody().readAllBytes()) : "";
                return ResponseEntity.status(resp.getStatusCode())
                    .headers(resp.getHeaders())
                    .body(respBody);
            });

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
        String body = response.getBody();
        assertThat(JsonPath.<Boolean>read(body, "$.success")).isFalse();
        assertThat(JsonPath.<String>read(body, "$.code")).isEqualTo("AUTH_REQUIRED");
        assertThat(JsonPath.<String>read(body, "$.message")).isEqualTo("인증이 필요합니다");
    }

    @Test
    @DisplayName("인증 실패 - 유효하지 않은 토큰")
    void authenticationFailure_invalidToken() {
        ResponseEntity<String> response = restClient.get()
            .uri("/api/v1/accounts/account-001")
            .header(AUTHORIZATION_HEADER, "Bearer invalid-token")
            .exchange((request, resp) -> {
                String respBody = resp.getBody() != null ? new String(resp.getBody().readAllBytes()) : "";
                return ResponseEntity.status(resp.getStatusCode())
                    .headers(resp.getHeaders())
                    .body(respBody);
            });

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
        String body = response.getBody();
        assertThat(JsonPath.<Boolean>read(body, "$.success")).isFalse();
        assertThat(JsonPath.<String>read(body, "$.code")).isEqualTo("INVALID_TOKEN_FORMAT");
        assertThat(JsonPath.<String>read(body, "$.message")).isEqualTo("유효하지 않은 토큰입니다");
    }

    @Test
    @DisplayName("매수 주문 - 더미 응답 확인")
    void buyOrder_dummyResponse() throws Exception {
        BuyOrderRequest request = BuyOrderRequest.builder()
            .accountId(1L)
            .stockCode("005930")
            .orderType("LIMIT")
            .price(new BigDecimal("70500"))
            .quantity(100)
            .build();

        ResponseEntity<String> response = restClient.post()
            .uri("/api/v1/orders/buy")
            .header(AUTHORIZATION_HEADER, BEARER_TOKEN)
            .contentType(org.springframework.http.MediaType.APPLICATION_JSON)
            .body(objectMapper.writeValueAsString(request))
            .retrieve()
            .toEntity(String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        String body = response.getBody();
        assertThat(JsonPath.<Boolean>read(body, "$.success")).isTrue();
        assertThat(JsonPath.<String>read(body, "$.data.status")).isEqualTo("PENDING");
        Object heldMargin = JsonPath.read(body, "$.data.heldMargin");
        assertThat(heldMargin).isNotNull();
    }

    @Test
    @DisplayName("종목 검색")
    void stockSearch() {
        String keyword = "삼성";

        ResponseEntity<String> response = restClient.get()
            .uri(uriBuilder -> uriBuilder.path("/api/v1/stocks/search").queryParam("keyword", keyword).build())
            .header(AUTHORIZATION_HEADER, BEARER_TOKEN)
            .retrieve()
            .toEntity(String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        String body = response.getBody();
        assertThat(JsonPath.<Boolean>read(body, "$.success")).isTrue();
        assertThat(JsonPath.<String>read(body, "$.data.data[0].stockCode")).isEqualTo("005930");
        assertThat(JsonPath.<String>read(body, "$.data.data[0].stockName")).isEqualTo("삼성전자");
    }
}
