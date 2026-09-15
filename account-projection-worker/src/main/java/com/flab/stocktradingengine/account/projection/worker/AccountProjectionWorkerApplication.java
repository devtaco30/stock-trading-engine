package com.flab.stocktradingengine.account.projection.worker;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * v2 계좌 상태 프로젝션 워커 앱. {@code account-state} 토픽을 소비해 account-worker의 인메모리
 * 잔고·보유를 v2 read model 테이블(account_projection)에 반영한다(계좌 상태 영속/프로젝션
 * 트랙 U3) — U4의 조회 API가 이 테이블을 읽는다.
 */
@SpringBootApplication(scanBasePackages = "com.flab.stocktradingengine.account.projection.worker")
public class AccountProjectionWorkerApplication {

    public static void main(String[] args) {
        SpringApplication.run(AccountProjectionWorkerApplication.class, args);
    }
}
