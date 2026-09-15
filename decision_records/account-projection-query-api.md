---
feature: account-projection-query-api
date: 2026-09-15
branch: feat/account-state-projection
commits: [8bad381]
feeds: [adr, blog]
---

# v2 계좌 조회 API — 프로젝션 read model 읽기 (계좌 상태 프로젝션 트랙 U4)

계좌 상태 프로젝션 트랙 U1~U3(account-worker가 full-state를 Kafka로 발행 → account-projection-worker가 DB read model에 upsert)의 read-side 마무리. `GET /api/v2/accounts/{accountId}`로 유저가 그 read model을 조회한다. 이걸로 CQRS 한 바퀴(쓰기=인메모리 엔진 authority / 읽기=DB 프로젝션)가 닫히고, fork1 3b가 "로그로만 관찰"로 우회했던 조각(체결이 조회로 안 보임)이 풀린다.

## ADR 네타

### ADR: api가 프로젝션 테이블을 읽는 방식 — 자체 read-only 엔티티(Option 1)
- **context(무슨 상황)**: 프로젝션 read model 테이블(`account_projection`, `account_projection_holding`)은 `account-projection-worker` 모듈이 소유하고 그 안에 JPA 엔티티가 있다. api 모듈은 이 테이블을 읽어 조회 API를 제공해야 하는데, api는 `account-projection-worker`에 의존하지 않는다(모듈 의존: `core → user,market,account,trading,settlement → api`, 워커들은 leaf 앱 모듈이라 아무도 의존 안 함).
- **왜(문제·동기)**: api가 지금 그대로는 프로젝션 엔티티/리포지토리에 접근할 수 없다. 읽으려면 접근 경로를 하나 정해야 한다.
- **어떻게(대안·결정·트레이드오프)**:
  - 대안 ①(채택) — **api에 read-only 엔티티+리포지토리를 신설**해 같은 테이블을 독립 매핑. 쓰기(worker)·읽기(api)를 코드가 아니라 **DB 스키마(계약)로만** 잇는다 = CQRS 정석. 두 벌 매핑은 중복이 아니라 의도된 분리(각 쪽이 자기가 쓸 필드만 모델링). 대가 = 같은 테이블을 두 곳에서 매핑하니 컬럼이 어긋나면 런타임에야 드러남 → @DataJpaTest로 컬럼 일치를 못박아 방어.
  - 대안 ②(기각) — 프로젝션 엔티티를 공유 모듈(core 등)로 추출해 api·worker가 공유. 중복은 없지만 결합도↑(워커의 read model이 공유 모듈로 새어나옴)·모듈 하나 추가. CQRS의 읽기/쓰기 분리를 코드 레벨에서 도로 붙이는 셈이라 학습상 덜 정직.
  - 대안 ③(기각) — api가 worker 모듈을 직접 의존. worker는 Spring Boot 앱 모듈이라 라이브러리로 부적합.
  - **DB 공유 전제**: 두 앱이 같은 물리 DB를 봐야 성립. 확인 결과 docker-compose엔 postgres가 `stock_trading`@5432 하나뿐이고, 워커 `application.yml` 주석도 "운영/로컬 실행은 api와 같은 stock_trading DB"라 명시. api local 프로파일이 이미 그 DB를 봐서 **api 설정 추가 없음**. 둘 다 ddl-auto:update라 테이블 생성은 additive(먼저 뜨는 쪽이 생성). ★기본(default) 프로파일은 api·worker가 각자 별도 H2(in-mem)라 공유 안 됨 → 워커 write→api read 실 e2e는 local(postgres)에서만.
- **무엇을(실제 변경·파일·커밋)**: 커밋 `8bad381`, 전부 api 모듈 신규 11파일.
  - read-only 엔티티: `api/.../api/projection/entity/AccountProjection`(setter/apply 없음, 워커 엔티티와 같은 이름·다른 패키지=독립 매핑 의도 표시)·`AccountProjectionHolding`(@IdClass)·`AccountProjectionHoldingId`. 컬럼(account_id·stock_code(len10)·balance(19,0)·seq·updated_at·quantity) 워커와 정확 일치.
  - 리포지토리 2개(`findById`, `findByAccountId`).
  - `AccountProjectionQueryService`(@Transactional(readOnly=true)): resolveAccountOwnedAndActive로 소유·활성 검증(리턴 Account는 검증용, 잔고는 프로젝션이 authority) → findById orElseThrow ResourceNotFoundException(404) → holdings 조회 → View 빌드.
  - `AccountV2Controller`(/api/v2/accounts, @GetMapping, @CurrentUserId, 200 OK).
  - DTO `AccountStateView(accountId, balance, seq, holdings)`+`HoldingView(stockCode, quantity)` — v1 `HoldingDto`(평가금액·손익 등 파생필드)와 다른 lean 타입. 프로젝션엔 잔고·수량만.
- **결과·수치**: 신규 테스트 GREEN(설계세션이 직접 재실행 확인) — @DataJpaTest 매핑 3케이스 + Mockito 서비스 4케이스(정상/보유0/프로젝션없음→404/미소유→403). `contextLoads()` GREEN(새 빈 배선 확인). 성능/부하 미측정.

### ADR 곁가지: 조회 없는 계좌 → 404 (eventual consistency 노출)
- **왜**: 프로젝션은 상태 변경(체결·정산·예약·입출금) 이벤트로만 채워진다. 활동 이력 0인 계좌는 소유 검증(v1 계좌 테이블 기준)은 통과해도 프로젝션 행이 없다.
- **어떻게**: v1 잔고를 대신 읽어 메우지 않고(=읽기/쓰기 모델 혼합) `ResourceNotFoundException`(404)으로 그대로 노출. read model이 아직 못 따라온 상태를 숨기지 않는 게 CQRS eventual consistency의 정직한 표현.

## 블로그 네타

### "CQRS 읽기 모델을 어디에 두나 — 같은 테이블, 두 벌 매핑"
- **훅·핵심 주장**: 조회 API가 인메모리 엔진(쓰기 authority)이 아니라 별도 DB read model을 읽는다. 그 read model에 접근하는 가장 정직한 방법은 "공유 코드"가 아니라 "공유 스키마" — 쓰기 모듈과 읽기 모듈이 같은 테이블을 각자 독립 매핑한다.
- **context**: single-writer 인메모리 계좌 엔진(속도) + Kafka off-path 발행 + DB 프로젝션(조회). 엔진은 잔고의 authority지만 조회 부하를 지면 안 됨 → 읽기를 물리적으로 분리.
- **어떻게(서사·근거)**: 세 대안(api 자체 엔티티 / 공유 모듈 추출 / 앱 모듈 직접 의존)을 모듈 의존 방향·결합도로 비교. "중복 매핑"이 냄새처럼 보이지만 CQRS에선 읽기/쓰기가 자기 필드만 모델링하는 의도된 분리라는 점. 대가(컬럼 drift)를 @DataJpaTest로 못박은 것. 조회 없는 계좌를 v1 잔고로 안 메우고 404로 둔 판단(모델 혼합 회피 = eventual consistency 노출).
- **재료(커밋·도식·수치)**: 커밋 `8bad381`, LLD `docs/_account_projection_lld.md`(U4 상세), 모듈 의존도(architecture.md), @DataJpaTest 컬럼 일치 검증 코드. 트랙 전체 도식 `docs/_v2_account_state_persistence.html`.
