package com.flab.stocktradingengine.api.dto.account;

/**
 * 계좌 상태 read model 조회 응답의 보유 한 종목(계좌 상태 프로젝션 트랙 U4). v1 {@link HoldingDto}와
 * 달리 파생 필드(평가금액·손익 등)가 없다 — 프로젝션 스키마엔 잔고·수량만 있다.
 */
public record HoldingView(String stockCode, int quantity) {
}
