package com.flab.stocktradingengine.dummy;

import com.flab.stocktradingengine.dto.settlement.ArrearsDto;
import com.flab.stocktradingengine.dto.settlement.RepaymentRequest;
import com.flab.stocktradingengine.dto.settlement.RepaymentResponse;
import com.flab.stocktradingengine.dto.settlement.UnpaidSettlementDto;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

/**
 * 결제/정산 관련 더미 데이터
 * 
 * <p>미결제 내역, 미수금, 변제에 대한 더미 데이터를 제공합니다.
 */
public class DummySettlementData {

    /**
     * 미결제 내역 조회 더미 데이터
     * 
     * <p>시나리오별 미결제 내역:
     * <ul>
     *   <li>account-001: 삼성전자 매수분 미결제 1건 (T+2 결제일 대기)</li>
     *   <li>account-002, account-003, account-004: 삼성전자 매수분 + 카카오 매수분 미결제 2건</li>
     * </ul>
     * 
     * @param accountId 계좌 ID
     * @return 미결제 내역 목록, 존재하지 않는 accountId인 경우 빈 리스트
     */
    public static List<UnpaidSettlementDto> getUnpaidSettlements(String accountId) {
        return switch (accountId) {
            case "account-001" -> List.of(
                UnpaidSettlementDto.builder()
                    .settlementId("settlement-001")
                    .stockCode("005930")                              // stockCode: 삼성전자
                    .settlementDate(LocalDate.now().plusDays(2))           // settlementDate: T+2 결제일
                    .amount(new BigDecimal("4230000"))             // amount: 미결제 금액 (7,050,000 × 0.6)
                    .status("PENDING")                              // status: 결제 대기
                    .build()
            );
            case "account-002", "account-003", "account-004" -> List.of(
                UnpaidSettlementDto.builder()
                    .settlementId("settlement-001")
                    .stockCode("005930")                              // stockCode: 삼성전자
                    .settlementDate(LocalDate.now().plusDays(1))           // settlementDate: 수요일 결제일
                    .amount(new BigDecimal("4230000"))             // amount: 미결제 금액 (7,050,000 × 0.6)
                    .status("PENDING")                              // status: 결제 대기
                    .build(),
                UnpaidSettlementDto.builder()
                    .settlementId("settlement-002")
                    .stockCode("035720")                              // stockCode: 카카오
                    .settlementDate(LocalDate.now().plusDays(2))           // settlementDate: 목요일 결제일
                    .amount(new BigDecimal("6024000"))             // amount: 미결제 금액 (10,040,000 × 0.6)
                    .status("PENDING")                              // status: 결제 대기
                    .build()
            );
            default -> new ArrayList<>();
        };
    }

    /**
     * 미수금 조회 더미 데이터
     * 
     * <p>시나리오별 미수금:
     * <ul>
     *   <li>account-003, account-004: 미수금 54,000원 발생 (결제일 배치 후 부족 금액)</li>
     *   <li>account-001, account-002: 미수금 없음 (null 반환)</li>
     * </ul>
     * 
     * <p>미수금 발생 시나리오:
     * <ul>
     *   <li>필요한 결제 금액: 10,254,000원</li>
     *   <li>가용 자금: 10,200,000원 (현금 3,000,000 + 매도 대금 7,200,000)</li>
     *   <li>부족 금액: 54,000원 → 미수금 발생</li>
     * </ul>
     * 
     * @param accountId 계좌 ID
     * @return 미수금 정보 (미수금 ID, 금액, 발생일, 연체 일수, 누적 이자), 미수금이 없거나 존재하지 않는 accountId인 경우 null
     */
    public static ArrearsDto getArrears(String accountId) {
        return switch (accountId) {
            case "account-003", "account-004" -> ArrearsDto.builder()
                .arrearsId("arrears-001")
                .amount(new BigDecimal("54000"))         // amount: 미수금 54,000원
                .occurredDate(LocalDate.now().minusDays(1))    // occurredDate: 발생일 (1일 전)
                .overdueDays(1)                               // overdueDays: 연체 일수
                .accumulatedInterest(new BigDecimal("0"))             // accumulatedInterest: 누적 이자 (아직 발생 안 함)
                .build();
            default -> null;
        };
    }

    /**
     * 미수금 변제 더미 응답
     * 
     * <p>변제 요청 금액을 미수금에서 차감한 잔액을 반환합니다.
     * 변제 금액이 미수금보다 큰 경우 잔액은 0으로 설정됩니다.
     * 
     * @param accountId 계좌 ID
     * @param arrearsId 미수금 ID (현재 미사용)
     * @param request 변제 요청 (변제 금액)
     * @return 변제 응답 (잔여 미수금, 변제 시각), 미수금이 없거나 존재하지 않는 accountId인 경우 null
     */
    public static RepaymentResponse getRepaymentResponse(String accountId, String arrearsId, RepaymentRequest request) {
        ArrearsDto arrears = getArrears(accountId);
        if (arrears == null) {
            return null;
        }
        
        BigDecimal remainingAmount = arrears.amount().subtract(request.amount());
        if (remainingAmount.compareTo(BigDecimal.ZERO) < 0) {
            remainingAmount = BigDecimal.ZERO;
        }
        
        return RepaymentResponse.builder()
            .remainingAmount(remainingAmount)
            .repaymentAt(System.currentTimeMillis())
            .build();
    }
}
