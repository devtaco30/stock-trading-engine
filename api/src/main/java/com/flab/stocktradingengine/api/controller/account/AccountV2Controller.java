package com.flab.stocktradingengine.api.controller.account;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.flab.stocktradingengine.api.dto.account.AccountStateView;
import com.flab.stocktradingengine.api.resolver.CurrentUserId;
import com.flab.stocktradingengine.api.service.AccountProjectionQueryService;
import com.flab.stocktradingengine.dto.common.ApiResponse;

import lombok.RequiredArgsConstructor;

/**
 * 계좌 상태 프로젝션 트랙 U4 — v2 계좌 상태 read model 조회 게이트웨이.
 * account-projection-worker가 upsert한 잔고·보유를 그대로 반환한다.
 */
@RestController
@RequestMapping("/api/v2/accounts")
@RequiredArgsConstructor
public class AccountV2Controller {

    private final AccountProjectionQueryService accountProjectionQueryService;

    @GetMapping("/{accountId}")
    public ResponseEntity<ApiResponse<AccountStateView>> getAccountState(
            @CurrentUserId Long userId,
            @PathVariable Long accountId) {
        AccountStateView view = accountProjectionQueryService.getAccountState(userId, accountId);
        return ResponseEntity.ok(ApiResponse.of(view));
    }
}
