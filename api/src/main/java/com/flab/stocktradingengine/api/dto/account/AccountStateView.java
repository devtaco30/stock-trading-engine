package com.flab.stocktradingengine.api.dto.account;

import java.math.BigDecimal;
import java.util.List;

/**
 * v2 계좌 상태 조회 응답(계좌 상태 프로젝션 트랙 U4). {@code account_projection} read model을
 * 그대로 반영한다. seq는 read model 신선도 관찰용이다.
 */
public record AccountStateView(Long accountId, BigDecimal balance, long seq, List<HoldingView> holdings) {
}
