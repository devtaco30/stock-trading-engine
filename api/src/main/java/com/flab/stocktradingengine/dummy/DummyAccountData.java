package com.flab.stocktradingengine.dummy;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import com.flab.stocktradingengine.dto.account.AccountDetailDto;
import com.flab.stocktradingengine.dto.account.AccountDto;
import com.flab.stocktradingengine.dto.account.HoldingDto;
import com.flab.stocktradingengine.dto.account.TransactionResponse;
import com.flab.stocktradingengine.dto.order.OrderDto;
import com.flab.stocktradingengine.dto.transaction.TransactionDto;

/**
 * 계좌 관련 더미 데이터
 * 
 * <p>시나리오별 accountId:
 * <ul>
 *   <li>account-001: 1단계 (삼성전자 매수 후) - 현금 3,000,000원, 증거금 홀딩 중, 삼성전자 100주 보유</li>
 *   <li>account-002: 2단계 (회전매매 후) - 삼성전자 매도 후 카카오 매수, 매도 예정금액 존재</li>
 *   <li>account-003: 3단계 (미수금 발생) - 결제일 배치 후 미수금 54,000원 발생, 계좌 상태 IN_ARREARS</li>
 *   <li>account-004: 4단계 (강제 청산 대상) - 미수금 미납으로 강제 청산 대상 계좌</li>
 * </ul>
 * 
 * @see docs/2_4단계_시나리오.md
 */
public class DummyAccountData {

    /**
     * 계좌 목록 조회 더미 데이터
     * 
     * @return 시나리오별 계좌 목록 (4개 계좌)
     */
    public static List<AccountDto> getAccounts() {
        return List.of(
            AccountDto.builder()
                .accountId("account-001")
                .balance(new BigDecimal("3000000"))
                .withdrawableBalance(new BigDecimal("180000"))
                .marginRate(40)
                .status("ACTIVE")
                .build(),
            AccountDto.builder()
                .accountId("account-002")
                .balance(new BigDecimal("3000000"))
                .withdrawableBalance(new BigDecimal("0"))
                .marginRate(40)
                .status("ACTIVE")
                .build(),
            AccountDto.builder()
                .accountId("account-003")
                .balance(new BigDecimal("3000000"))
                .withdrawableBalance(new BigDecimal("0"))
                .marginRate(40)
                .status("IN_ARREARS")
                .build(),
            AccountDto.builder()
                .accountId("account-004")
                .balance(new BigDecimal("3000000"))
                .withdrawableBalance(new BigDecimal("0"))
                .marginRate(40)
                .status("IN_ARREARS")
                .build()
        );
    }

    /**
     * 계좌 상세 조회 더미 데이터
     * 
     * <p>시나리오별 상세 정보:
     * <ul>
     *   <li>account-001: 삼성전자 100주 매수 후 상태
     *       <ul>
     *         <li>현금: 3,000,000원</li>
     *         <li>출금가능: 180,000원 (증거금 2,820,000원 홀딩 중)</li>
     *         <li>미결제: 4,230,000원 (T+2 결제일 대기)</li>
     *         <li>매수가능: 7,500,000원</li>
     *       </ul>
     *   </li>
     *   <li>account-002: 회전매매 후 상태
     *       <ul>
     *         <li>현금: 3,000,000원</li>
     *         <li>출금가능: 0원 (전액 증거금 홀딩)</li>
     *         <li>매도 예정금액: 3,184,000원 (삼성전자 매도 대금)</li>
     *         <li>미결제: 10,254,000원 (삼성전자 4,230,000 + 카카오 6,024,000)</li>
     *         <li>매수가능: 18,450,000원 (매도 예정금액 포함)</li>
     *       </ul>
     *   </li>
     *   <li>account-003, account-004: 미수금 발생 상태 (IN_ARREARS)</li>
     * </ul>
     * 
     * @param accountId 계좌 ID
     * @return 계좌 상세 정보, 존재하지 않는 accountId인 경우 null
     */
    public static AccountDetailDto getAccountDetail(String accountId) {
        return switch (accountId) {
            case "account-001" -> AccountDetailDto.builder()
                .accountId("account-001")
                .balance(new BigDecimal("3000000"))      // balance: 총 현금
                .withdrawableBalance(new BigDecimal("180000"))       // withdrawableBalance: 출금 가능 현금 (증거금 2,820,000원 홀딩 중)
                .totalAssets(new BigDecimal("7050000"))      // totalAssets: 총 자산 (현금 3,000,000 + 보유주식 평가액 7,050,000)
                .unpaidAmount(new BigDecimal("4230000"))      // unpaidAmount: 미결제 금액 (7,050,000 × 0.6 = 4,230,000)
                .pendingSellAmount(new BigDecimal("0"))            // pendingSellAmount: 매도 예정금액
                .buyLimit(new BigDecimal("7500000"))      // buyLimit: 매수 가능 금액 (증거금 40% 기준)
                .marginRate(40)                             // marginRate: 증거금률 40%
                .status("ACTIVE")                         // status: 정상 계좌
                .build();
            case "account-002" -> AccountDetailDto.builder()
                .accountId("account-002")
                .balance(new BigDecimal("3000000"))      // balance: 총 현금
                .withdrawableBalance(new BigDecimal("0"))            // withdrawableBalance: 출금 가능 현금 (전액 증거금 홀딩)
                .totalAssets(new BigDecimal("10040000"))     // totalAssets: 총 자산 (현금 3,000,000 + 카카오 평가액 10,040,000)
                .unpaidAmount(new BigDecimal("10254000"))     // unpaidAmount: 미결제 금액 (삼성전자 4,230,000 + 카카오 6,024,000)
                .pendingSellAmount(new BigDecimal("3184000"))      // pendingSellAmount: 매도 예정금액 (삼성전자 매도 대금 7,200,000 - 증거금 4,016,000)
                .buyLimit(new BigDecimal("18450000"))     // buyLimit: 매수 가능 금액 (매도 예정금액 포함 계산)
                .marginRate(40)                             // marginRate: 증거금률 40%
                .status("ACTIVE")                         // status: 정상 계좌
                .build();
            case "account-003" -> AccountDetailDto.builder()
                .accountId("account-003")
                .balance(new BigDecimal("3000000"))      // balance: 총 현금
                .withdrawableBalance(new BigDecimal("0"))            // withdrawableBalance: 출금 가능 현금
                .totalAssets(new BigDecimal("10040000"))     // totalAssets: 총 자산
                .unpaidAmount(new BigDecimal("10254000"))     // unpaidAmount: 미결제 금액
                .pendingSellAmount(new BigDecimal("0"))            // pendingSellAmount: 매도 예정금액 (이미 정산됨)
                .buyLimit(new BigDecimal("0"))            // buyLimit: 매수 가능 금액 (미수금 발생으로 거래 제한)
                .marginRate(40)                             // marginRate: 증거금률 40%
                .status("IN_ARREARS")                     // status: 미수금 발생 (결제일 배치 후 미수금 54,000원 발생)
                .build();
            case "account-004" -> AccountDetailDto.builder()
                .accountId("account-004")
                .balance(new BigDecimal("3000000"))      // balance: 총 현금
                .withdrawableBalance(new BigDecimal("0"))            // withdrawableBalance: 출금 가능 현금
                .totalAssets(new BigDecimal("10040000"))     // totalAssets: 총 자산
                .unpaidAmount(new BigDecimal("10254000"))     // unpaidAmount: 미결제 금액
                .pendingSellAmount(new BigDecimal("0"))            // pendingSellAmount: 매도 예정금액
                .buyLimit(new BigDecimal("0"))            // buyLimit: 매수 가능 금액 (미수금 발생으로 거래 제한)
                .marginRate(40)                             // marginRate: 증거금률 40%
                .status("IN_ARREARS")                     // status: 강제 청산 대상 (미수금 미납으로 보유 주식 강제 매도 대상)
                .build();
            default -> null;
        };
    }

    /**
     * 보유 주식 조회 더미 데이터
     * 
     * <p>시나리오별 보유 주식:
     * <ul>
     *   <li>account-001: 삼성전자 100주 (매수가 70,500원)</li>
     *   <li>account-002, account-003, account-004: 카카오 200주 (매수가 50,200원)</li>
     * </ul>
     * 
     * @param accountId 계좌 ID
     * @return 보유 주식 목록, 존재하지 않는 accountId인 경우 빈 리스트
     */
    public static List<HoldingDto> getHoldings(String accountId) {
        return switch (accountId) {
            case "account-001" -> List.of(
                HoldingDto.builder()
                    .stockCode("005930")                           // stockCode: 삼성전자 종목 코드
                    .stockName("삼성전자")
                    .quantity(100)                                // quantity: 보유 수량
                    .averagePrice(new BigDecimal("70500"))            // averagePrice: 평균 매수가
                    .currentPrice(new BigDecimal("70500"))            // currentPrice: 현재가
                    .evaluationAmount(new BigDecimal("7050000"))          // evaluationAmount: 평가액 (수량 × 현재가)
                    .profit(new BigDecimal("0"))                // profit: 평가 손익
                    .profitRate(new BigDecimal("0"))                 // profitRate: 수익률 (%)
                    .build()
            );
            case "account-002", "account-003", "account-004" -> List.of(
                HoldingDto.builder()
                    .stockCode("035720")                           // stockCode: 카카오 종목 코드
                    .stockName("카카오")
                    .quantity(200)                                // quantity: 보유 수량
                    .averagePrice(new BigDecimal("50200"))            // averagePrice: 평균 매수가
                    .currentPrice(new BigDecimal("50200"))            // currentPrice: 현재가
                    .evaluationAmount(new BigDecimal("10040000"))        // evaluationAmount: 평가액 (200주 × 50,200원)
                    .profit(new BigDecimal("0"))                // profit: 평가 손익
                    .profitRate(new BigDecimal("0"))                 // profitRate: 수익률 (%)
                    .build()
            );
            default -> new ArrayList<>();
        };
    }

    /**
     * 입금 처리 더미 응답
     * 
     * <p>계좌 잔액에 입금 금액을 더한 새로운 잔액을 반환합니다.
     * 
     * @param accountId 계좌 ID
     * @param amount 입금 금액
     * @return 입금 처리 응답 (거래 ID, 새로운 잔액, 거래 시각), 존재하지 않는 accountId인 경우 null
     */
    public static TransactionResponse getDepositResponse(String accountId, BigDecimal amount) {
        AccountDetailDto account = getAccountDetail(accountId);
        if (account == null) {
            return null;
        }
        BigDecimal newBalance = account.balance().add(amount);
        return TransactionResponse.builder()
            .transactionId("txn-" + System.currentTimeMillis())
            .balance(newBalance)
            .transactionAt(System.currentTimeMillis())
            .build();
    }

    /**
     * 출금 처리 더미 응답
     * 
     * <p>계좌 잔액에서 출금 금액을 뺀 새로운 잔액을 반환합니다.
     * 
     * @param accountId 계좌 ID
     * @param amount 출금 금액
     * @return 출금 처리 응답 (거래 ID, 새로운 잔액, 거래 시각), 존재하지 않는 accountId인 경우 null
     */
    public static TransactionResponse getWithdrawResponse(String accountId, BigDecimal amount) {
        AccountDetailDto account = getAccountDetail(accountId);
        if (account == null) {
            return null;
        }
        BigDecimal newBalance = account.balance().subtract(amount);
        return TransactionResponse.builder()
            .transactionId("txn-" + System.currentTimeMillis())
            .balance(newBalance)
            .transactionAt(System.currentTimeMillis())
            .build();
    }

    /**
     * 주문 내역 조회 더미 데이터
     * 
     * <p>시나리오별 주문 내역:
     * <ul>
     *   <li>account-001: 삼성전자 매수 주문 1건 (체결 완료)</li>
     *   <li>account-002, account-003, account-004: 삼성전자 매수 → 매도 → 카카오 매수 (3건)</li>
     * </ul>
     * 
     * <p>참고: 파라미터(status, startAt, endAt)는 현재 필터링하지 않고 전체 데이터를 반환합니다.
     * 
     * @param accountId 계좌 ID
     * @param status 주문 상태 필터 (현재 미사용)
     * @param startAt 시작 시각 필터 (현재 미사용)
     * @param endAt 종료 시각 필터 (현재 미사용)
     * @return 주문 내역 목록, 존재하지 않는 accountId인 경우 빈 리스트
     */
    public static List<OrderDto> getOrders(String accountId, String status, Long startAt, Long endAt) {
        return switch (accountId) {
            case "account-001" -> List.of(
                OrderDto.builder()
                    .orderId("order-001")
                    .stockCode("005930")                           // stockCode: 삼성전자
                    .stockName("삼성전자")
                    .side("BUY")                              // side: 매수
                    .orderType("LIMIT")                            // orderType: 지정가
                    .price(new BigDecimal("70500"))             // price: 주문가
                    .quantity(100)                                // quantity: 수량
                    .status("FILLED")                           // status: 체결 완료
                    .orderAt(System.currentTimeMillis() - 900000)  // orderAt: 주문 시각 (15분 전)
                    .filledAt(System.currentTimeMillis() - 750000)   // filledAt: 체결 시각 (12.5분 전)
                    .build()
            );
            case "account-002", "account-003", "account-004" -> List.of(
                OrderDto.builder()
                    .orderId("order-001")
                    .stockCode("005930")                           // stockCode: 삼성전자
                    .stockName("삼성전자")
                    .side("BUY")                              // side: 매수
                    .orderType("LIMIT")                            // orderType: 지정가
                    .price(new BigDecimal("70500"))             // price: 주문가 70,500원
                    .quantity(100)                                // quantity: 수량 100주
                    .status("FILLED")                           // status: 체결 완료
                    .orderAt(System.currentTimeMillis() - 86400000) // orderAt: 주문 시각 (1일 전)
                    .filledAt(System.currentTimeMillis() - 86300000)  // filledAt: 체결 시각
                    .build(),
                OrderDto.builder()
                    .orderId("order-002")
                    .stockCode("005930")                           // stockCode: 삼성전자
                    .stockName("삼성전자")
                    .side("SELL")                             // side: 매도
                    .orderType("MARKET")                           // orderType: 시장가
                    .price(new BigDecimal("72000"))             // price: 체결가 72,000원
                    .quantity(100)                                // quantity: 수량 100주
                    .status("FILLED")                           // status: 체결 완료
                    .orderAt(System.currentTimeMillis() - 3600000)  // orderAt: 주문 시각 (1시간 전)
                    .filledAt(System.currentTimeMillis() - 3590000)   // filledAt: 체결 시각
                    .build(),
                OrderDto.builder()
                    .orderId("order-003")
                    .stockCode("035720")                           // stockCode: 카카오
                    .stockName("카카오")
                    .side("BUY")                              // side: 매수
                    .orderType("LIMIT")                            // orderType: 지정가
                    .price(new BigDecimal("50200"))             // price: 주문가 50,200원
                    .quantity(200)                                // quantity: 수량 200주
                    .status("FILLED")                           // status: 체결 완료
                    .orderAt(System.currentTimeMillis() - 3300000) // orderAt: 주문 시각 (55분 전)
                    .filledAt(System.currentTimeMillis() - 3290000)  // filledAt: 체결 시각
                    .build()
            );
            default -> new ArrayList<>();
        };
    }

    /**
     * 거래 내역 조회 더미 데이터
     * 
     * <p>시나리오별 거래 내역:
     * <ul>
     *   <li>account-001: 삼성전자 매수 거래 1건</li>
     *   <li>account-002, account-003, account-004: 삼성전자 매수 → 매도 → 카카오 매수 (3건)</li>
     * </ul>
     * 
     * <p>참고: 파라미터(startAt, endAt)는 현재 필터링하지 않고 전체 데이터를 반환합니다.
     * 
     * @param accountId 계좌 ID
     * @param startAt 시작 시각 필터 (현재 미사용)
     * @param endAt 종료 시각 필터 (현재 미사용)
     * @return 거래 내역 목록, 존재하지 않는 accountId인 경우 빈 리스트
     */
    public static List<TransactionDto> getTransactions(String accountId, Long startAt, Long endAt) {
        return switch (accountId) {
            case "account-001" -> List.of(
                TransactionDto.builder()
                    .transactionAt(System.currentTimeMillis() - 750000)  // transactionAt: 거래 시각 (12.5분 전)
                    .stockCode("005930")                             // stockCode: 삼성전자
                    .stockName("삼성전자")
                    .side("BUY")                                // side: 매수
                    .quantity(100)                                   // quantity: 수량
                    .executionPrice(new BigDecimal("70500"))              // executionPrice: 체결가
                    .transactionAmount(new BigDecimal("7050000"))             // transactionAmount: 거래 금액 (100주 × 70,500원)
                    .build()
            );
            case "account-002", "account-003", "account-004" -> List.of(
                TransactionDto.builder()
                    .transactionAt(System.currentTimeMillis() - 86300000) // transactionAt: 거래 시각 (1일 전)
                    .stockCode("005930")                              // stockCode: 삼성전자
                    .stockName("삼성전자")
                    .side("BUY")                                 // side: 매수
                    .quantity(100)                                    // quantity: 수량
                    .executionPrice(new BigDecimal("70500"))               // executionPrice: 체결가 70,500원
                    .transactionAmount(new BigDecimal("7050000"))              // transactionAmount: 거래 금액 7,050,000원
                    .build(),
                TransactionDto.builder()
                    .transactionAt(System.currentTimeMillis() - 3590000)  // transactionAt: 거래 시각 (1시간 전)
                    .stockCode("005930")                              // stockCode: 삼성전자
                    .stockName("삼성전자")
                    .side("SELL")                                // side: 매도
                    .quantity(100)                                    // quantity: 수량
                    .executionPrice(new BigDecimal("72000"))               // executionPrice: 체결가 72,000원
                    .transactionAmount(new BigDecimal("7200000"))             // transactionAmount: 거래 금액 7,200,000원
                    .build(),
                TransactionDto.builder()
                    .transactionAt(System.currentTimeMillis() - 3290000)  // transactionAt: 거래 시각 (55분 전)
                    .stockCode("035720")                              // stockCode: 카카오
                    .stockName("카카오")
                    .side("BUY")                                 // side: 매수
                    .quantity(200)                                    // quantity: 수량
                    .executionPrice(new BigDecimal("50200"))               // executionPrice: 체결가 50,200원
                    .transactionAmount(new BigDecimal("10040000"))             // transactionAmount: 거래 금액 10,040,000원
                    .build()
            );
            default -> new ArrayList<>();
        };
    }
}
