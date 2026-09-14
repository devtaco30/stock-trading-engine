package com.flab.stocktradingengine.api.service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Optional;

import org.springframework.stereotype.Service;

import com.flab.stocktradingengine.account.entity.Account;
import com.flab.stocktradingengine.api.dto.order.BuyOrderRequest;
import com.flab.stocktradingengine.api.dto.order.SellOrderRequest;
import com.flab.stocktradingengine.api.redis.LtpRedisRepository;
import com.flab.stocktradingengine.api.resolver.AccountAccessResolver;
import com.flab.stocktradingengine.exception.InvalidRequestException;
import com.flab.stocktradingengine.exception.ResourceNotFoundException;
import com.flab.stocktradingengine.market.service.QuoteService;
import com.flab.stocktradingengine.market.view.QuoteView;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * fork5, U1a — v2 주문 게이트웨이 뼈대(<code>/api/v2/orders</code>). v1
 * {@link OrderApiService}의 검증(계좌 소유·활성, 가격 제한폭)을 그대로 재사용하되, requestId는
 * 클라이언트 필수로 바꾼다 — 게이트웨이가 여럿(stateless)일 수 있어 서버가 대신 생성하면
 * 재전송을 같은 요청으로 못 알아봐 중복 예약이 생긴다({@code fork5 결정 ③}).
 *
 * <p>이 유닛은 검증 통과 지점까지만 처리한다 — 계좌 엔진으로의 Aeron 발신은 다음 유닛(U1b)에서
 * 붙인다.</p>
 *
 * <h3>가격 제한폭 검증을 v1과 별도로 갖는 이유</h3>
 * <p>v1 {@code OrderApiService.validatePriceBandLimit}은 private이다. 지금 단계에서 공용 빈으로
 * 추출하는 건 리팩토링이라 이 기능 추가 커밋과 분리해야 한다(리팩토링·기능추가 분리 원칙) —
 * 그래서 이번엔 v1을 건드리지 않고 같은 규칙을 이 클래스에 다시 둔다. 추출은 필요해지면 별도
 * 커밋으로 진행한다.</p>
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class OrderV2ApiService {

    private final AccountAccessResolver accountAccessResolver;
    private final LtpRedisRepository ltpRedisRepository;
    private final QuoteService quoteService;

    /** 매수 주문 접수(뼈대). 검증만 수행하고 반환한다 — 발신은 U1b. */
    public void placeBuyOrder(Long userId, BuyOrderRequest request) {
        String requestId = requireRequestId(request.requestId());
        Account account = accountAccessResolver.resolveAccountOwnedAndActive(userId, request.accountId());
        validatePriceBandLimit(request.stockCode(), request.price());

        // TODO(U1b): accountId 샤드 라우팅 후 Aeron으로 매수 주문 발신
        log.info("[v2 매수 접수(검증만)] 종목={} 계좌={} requestId={}", request.stockCode(), account.getAccountId(), requestId);
    }

    /** 매도 주문 접수(뼈대). 검증만 수행하고 반환한다 — 발신은 U1b. */
    public void placeSellOrder(Long userId, SellOrderRequest request) {
        String requestId = requireRequestId(request.requestId());
        Account account = accountAccessResolver.resolveAccountOwnedAndActive(userId, request.accountId());
        validatePriceBandLimit(request.stockCode(), request.price());

        // TODO(U1b): accountId 샤드 라우팅 후 Aeron으로 매도 주문 발신
        log.info("[v2 매도 접수(검증만)] 종목={} 계좌={} requestId={}", request.stockCode(), account.getAccountId(), requestId);
    }

    /**
     * requestId는 클라이언트 필수다 — v1의 서버 생성 fallback을 쓰지 않는다.
     *
     * @throws InvalidRequestException requestId가 null 이거나 빈 문자열이면
     */
    private String requireRequestId(String requestId) {
        if (requestId == null || requestId.isBlank()) {
            throw new InvalidRequestException("requestId는 필수입니다");
        }
        return requestId;
    }

    /** v1 {@code OrderApiService.validatePriceBandLimit}과 같은 규칙(LTP 또는 전일종가 ±30%). */
    private void validatePriceBandLimit(String stockCode, BigDecimal price) {
        BigDecimal reference = getLtp(stockCode)
            .or(() -> quoteService.getQuote(stockCode).map(QuoteView::previousClose))
            .orElseThrow(() -> new ResourceNotFoundException("기준가를 조회할 수 없는 종목: " + stockCode));

        BigDecimal upper = reference.multiply(new BigDecimal("1.3")).setScale(0, RoundingMode.DOWN);
        BigDecimal lower = reference.multiply(new BigDecimal("0.7")).setScale(0, RoundingMode.UP);

        if (price.compareTo(upper) > 0 || price.compareTo(lower) < 0) {
            throw new InvalidRequestException(
                "가격 제한폭 초과: " + price + " (기준가: " + reference + ", 범위: " + lower + "~" + upper + ")");
        }
    }

    private Optional<BigDecimal> getLtp(String stockCode) {
        return ltpRedisRepository.get(stockCode);
    }
}
