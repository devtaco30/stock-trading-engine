# ADR-010 Order.accountId 를 Snowflake accountId 로 일치

## 문제

`Order.accountId`(비정규화 필드)에 Snowflake accountId 대신 **accounts 테이블의 대리 PK(`id`)** 가 담겨 있었다.

원인은 매핑이다. `Order.account` 의 `@JoinColumn(name="account_id")` 이 `referencedColumnName` 을 주지 않아 기본값인 Account 의 PK(`id`, auto-increment)를 참조했다. 그래서 `orders.account_id` FK 컬럼에 Snowflake(1002) 대신 대리키(26)가 저장됐고, 같은 컬럼을 읽는 `order.getAccountId()` 도 대리키를 돌려줬다.

접수 경로는 `command.accountId()`(Snowflake)로 조회·락을 걸어 정상이었지만, 이 컬럼을 되읽는 정산은 대리키를 Snowflake accountId 로 오인해 계좌·보유 조회에 실패했다. 실제 체결을 처음 태우자 정산에서 `Account not found: 26` 으로 fill 이 폐기되고, 주문은 PENDING 에 머물고 보유가 반영되지 않았다.

ADR-005 의 비정규화 결정(Order 가 accountId 를 직접 보유) 자체는 유효하다. 다만 그 실행에서 재사용한 컬럼이 Snowflake 대신 대리키였다. 이 ADR 은 그 결정을 그대로 두고 매핑 실수만 바로잡는다.

## 대안

1. **FK 를 Snowflake `account_id` 에 맞춘다.** `referencedColumnName="account_id"` + `accounts.account_id` UNIQUE. `orders.account_id` 가 Snowflake 를 담고, `order.getAccountId()` 도 Snowflake.
2. **Snowflake 를 별도 컬럼으로 실제 저장.** FK 조인 컬럼은 대리키로 두고, 주문 생성 시 `lockedAccount.getAccountId()` 를 별도 컬럼에 명시 저장.
3. **정산 조회를 대리키 기준으로 통일.** 접수(Snowflake)와 영영 혼재. 파티션 키(Snowflake)와도 어긋나 비권장.

## 트레이드오프

| 대안 | 장점 | 단점 |
|---|---|---|
| A. FK 를 account_id 참조 | 한 컬럼이 일관되게 Snowflake, 소비처 무수정, `Quote` 선례 있음 | FK 가 PK 아닌 UNIQUE 컬럼 참조(약간 비관례), accounts.account_id UNIQUE 필요 |
| B. 별도 컬럼 | FK 매핑 안 건드림 | 컬럼 추가 + 생성 시 세팅, 이름 중복 정리 필요 |
| C. 정산 대리키 통일 | 정산만 수정 | 접수와 두 체계 혼재 지속, 파티션 키 불일치 |

## 결정

**대안 A** 채택. 코드베이스에 이미 있는 `Quote`(`referencedColumnName="stock_code"`) 패턴과 동일하다.

- `Account.accountId`: `@Column(unique=true)` 추가.
- `Order.account`: `@JoinColumn(name="account_id", referencedColumnName="account_id")`.
- 소비처(`MatchingConsumer.toEntry(Order)`, `OrderBook` 의 FillResult, `OrderSettlementService`)는 이미 `order.getAccountId()` 를 Snowflake 로 기대하고 있었으므로 수정하지 않는다.

## 결과

- 단위·슬라이스 테스트 127개 통과. `OrderAccountIdMappingTest`(`@DataJpaTest`) 신규 — 저장·재조회 후 `order.getAccountId()` 가 Snowflake accountId 와 같음을 검증.
- e2e 교차 체결 실증: 매도 50 + 매수 50 → 둘 다 FILLED, 판매자 보유 100→50, 매수자 0→50, `Account not found` 0. 정산 end-to-end 복구.
- `orders.account_id` 는 Snowflake, `holdings.account_id`·`unpaids.account_id` 는 대리키(그들은 네비게이션·JPQL 조인으로만 소비해 안전). 전체 통일은 별도 스코프로 남긴다.
- 운영 DB 반영: `ddl-auto` 는 기존 FK 를 자동 교체하지 않으므로 매뉴얼 DDL 이 필요하다.
  ```sql
  ALTER TABLE accounts ADD CONSTRAINT uq_accounts_account_id UNIQUE (account_id);
  ALTER TABLE orders DROP CONSTRAINT <기존_account_id_fk>;
  -- orders.account_id 값을 accounts.id → accounts.account_id 로 재매핑한 뒤
  ALTER TABLE orders ADD CONSTRAINT fk_orders_account
      FOREIGN KEY (account_id) REFERENCES accounts(account_id);
  ```
