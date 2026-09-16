---
feature: e2e-measurement-driver-integration
date: 2026-09-16
branch: feat/c7-load-harness
commits: [b496715, f390fcf, 03a2cf0, 6d6f77f]
feeds: [adr, blog]
---

# v1/v2 전 과정 측정 — 리플레이 드라이버(④) + 통합 중 잡은 실기동 버그 4건

39 설계(decision_records/v1-v2-e2e-measurement.md)의 실행 축(④ 리플레이 드라이버 + 통합·실행·리포트)을
dc가 맡았다. a4(② 히스토그램 훅)·ec(③ 결정론 입력·시드)가 병렬로 만든 걸 이 브랜치로 합치면서,
코드 리뷰로는 안 보이고 **실제로 docker Postgres·Kafka·Redis에 e2e 프로파일로 기동해봐야만
드러나는 버그 4건**을 잡았다. 개별 버그는 각 커밋 메시지에 있고, 이 기록은 왜 재기동 없는
연속 패스 설계로 갔는지와 통합 과정 전체를 남긴다.

## 설계 변경 — 패스마다 재기동 → 재기동 없이 연속 4패스

### 문제
39 설계는 "워밍업 1패스(버림) + 측정 3패스"만 정했지 재기동 여부는 안 정했다. 처음엔 "패스마다
재기동해서 계좌 상태를 초기화"하려 했다(2b 초기 지시).

### 왜 틀렸는지(코드로 확인)
재기동해도 초기화가 안 된다 — v2 계좌 워커는 스냅샷+저널로 마지막 상태를 그대로 복구하고
(`account-worker/.../recovery/AccountSnapshotStore.java`), v1은 Postgres가 재기동과 무관하게
그대로 남는다. 재기동은 "상태 리셋"을 전혀 안 해주면서 JIT 워밍업만 날리는, 대가만 있고
얻는 게 없는 선택이었다.

### 결정
버전당 프로세스를 한 번만 띄우고, 워밍업+측정 3패스를 재기동 없이 연속 재생한다(`measure-e2e.sh`).
- **requestId 충돌**: 같은 입력 파일을 여러 패스에 재생하면 멱등 캐시가 재전송으로 오인해
  두 번째 패스부터 duplicate로 스킵된다 — `replay-e2e.js`가 `PASS` 환경변수로 requestId에
  접미사(`{원본}-{PASS}`)를 붙여 해결(계좌·종목·가격·수량·순서는 그대로).
- **패스 경계 분리**: a4의 히스토그램 훅은 "종료 시 1회 덤프"만으론 4개의 분리된 분포를 못 준다 —
  파일 트리거 방식을 새로 스펙으로 얹었다(`measure.latency.snapshot-trigger-path`에 파일이
  생기면 그 내용(출력 경로 한 줄)에 스냅샷을 쓰고 리셋, 트리거 파일 삭제 = 처리 완료 신호).
  a4가 39 리뷰까지 받아 원자적 rename(임시파일→rename)·재처리 방지(같은 목적지 경로 재도착 시
  히스토그램 재기록 안 함)까지 마감했다.
- **시드 4배**: 재기동이 없으므로 잔고·보유가 패스를 거칠수록 줄어든다 — ec가 실제 생성된
  주문에서 역산한 필요량 × 4(워밍업 1 + 측정 3)로 시드.

## 통합 중 잡은 실기동 버그 4건

### 1. holdings FK가 accounts의 업무키가 아니라 대리키를 참조
- **context**: ec의 v1 시드 SQL이 `INSERT INTO holdings (account_id, ...) VALUES (2051, ...)`
  형태로 업무키(account_id)를 그대로 넣음.
- **왜**: `\d holdings`로 직접 확인 — FK `fk1lvcybrc320h9lxbgaqs613bg`가
  `accounts(id)`(대리키, identity 자동증가)를 참조한다. `orders` 테이블은 반대로
  `accounts(account_id)`(업무키)를 참조한다 — 같은 컬럼명 "account_id"가 테이블마다 다른
  대상을 가리키는 기존 스키마 설계(이번에 새로 생긴 문제 아님, holdings에 SQL로 직접 seed하는
  걸 이번이 처음 해봐서 드러남).
- **어떻게**: `generate-e2e-input.py`의 holdings INSERT를
  `SELECT a.id, ... FROM accounts a WHERE a.account_id = {업무키}`로 대리키를 서브쿼리로
  찾게 수정.
- **결과·수치**: 실측 — 수정 전 FK violation으로 api 부팅 자체가 실패, 수정 후 holdings
  1500행 정상 적재 확인(`SELECT count(*) FROM holdings h JOIN accounts a ON h.account_id=a.id
  WHERE a.account_id BETWEEN 2001 AND 2100` → 1500).

### 2. e2e 계좌(user_id=2)와 기존 토큰(userId=1) 불일치로 403
- **context**: ec의 시드가 계좌를 `user_id=2`로 만드는데(기존 loadtest-seed.sql의
  `user_id=1`과 안 겹치게), 기존 `run-v1.sh`/`run-v2.sh`가 심는 `loadtest-token-1`은
  Redis에 `userId=1`로 매핑돼 있다.
- **왜**: `AccountAccessResolver.resolveAccountOwnedAndActive`가 소유 검증에서 403
  (`ForbiddenException.notOwnerOfAccount`).
- **어떻게**: `run-e2e-v1.sh`/`run-e2e-v2.sh`에 e2e 전용 토큰(`e2e-token-1` → Redis에
  `userId=2`)을 추가.
- **결과·수치**: 실측 — 토큰 교체 후 v1·v2 양쪽 buy/sell 202(SUCCESS) 확인.

### 3. 히스토그램 트리거 파일 경로가 상대경로라 안 먹힘
- **context**: a4의 `snapshot-trigger-path`/`output-path`를 리포 루트 기준 상대경로로 줬다
  (`loadtest/results/latency-trigger-v2`).
- **왜**: `gradlew bootRun`이 띄우는 JVM의 작업 디렉터리(`user.dir`)는 리포 루트가 아니라
  **모듈 서브디렉터리**(예: `account-worker/`)다 — 상대경로를 쓰면 폴링 스레드가
  `account-worker/loadtest/results/...`를 보는데, 트리거 파일은 리포 루트에 놨으니 영영 안
  보인다.
- **어떻게**: `run-e2e-v1.sh`/`run-e2e-v2.sh`가 `$(pwd)`(스크립트 자신은 리포 루트에서 실행)로
  절대경로를 만들어 `--measure.latency.snapshot-trigger-path`·`--measure.latency.output-path`에
  넘긴다. 또한 트리거 파일 **내용**(목적지 경로)도 절대경로로 써야 한다 — 목적지도 앱의
  `user.dir` 기준으로 풀리기 때문(`measure-e2e.sh`의 `request_latency_snapshot`이 이미
  `$(pwd)` 기반 절대경로 사용).
- **결과·수치**: 실측 — 상대경로일 때 트리거 파일이 영원히 안 지워짐(폴링 스레드가 못 찾음),
  절대경로로 바꾼 뒤 v1·v2 둘 다 트리거 즉시 처리(트리거 파일 삭제 + JSON 출력) 확인.

### 4. v1의 order-engine/matching-engine/settlement-engine이 group-id를 loadtest 프로파일과 공유해, 오늘 쌓인 Kafka 백로그를 이어받아 소비
- **context**: `application-e2e.yml`의 Kafka consumer `group-id`가 `application.yml`/
  `application-loadtest.yml`과 동일(`order-engine`/`matching-engine`/`settlement-engine`).
- **왜**: 오늘 하루 v1 HTTP 처리량 측정(4·6·8절)을 여러 차례 돌리면서 `order-requests` 토픽에
  메시지가 계속 쌓였다. `kafka-consumer-groups.sh --describe --group order-engine`으로 직접
  확인 — 파티션 0의 LAG=4620(committed 32653 / 최신 37273). `auto-offset-reset: earliest`
  조합이라, 이 인스턴스는 그 백로그를 처음부터 이어받아 소비하며 몇 시간 전 메시지의
  `requestedAt`으로 지연(`now - requestedAt`)을 계산 — 히스토그램이 오염됐다
  (`count=1731`, `p50Nanos`가 수천 초 단위로 튐, 실측 확인).
- **어떻게**: v1 세 앱의 `application-e2e.yml` 모두 `group-id`에 `-e2e` 접미사(예:
  `order-engine-e2e`) + `auto-offset-reset: latest`로 바꿔, 다른 프로파일의 그룹과 완전히
  분리하고 자신이 뜬 시점 이후 메시지만 보게 함(전체 재생이 아니라 latest — 과거 대량 백로그를
  받는 게 아니라 무시).
- **결과·수치**: 실측 — 수정 후 주문 1건 발행+트리거 즉시 확인 시 `count=1`,
  `p50Nanos=247988223`(≈248ms, 콜드 스타트 단일 요청 기준 타당한 값). matching-engine·
  settlement-engine은 이 시점 lag=0이라 오염이 실측되진 않았지만 같은 구조적 위험이라 예방적으로
  같이 고침.

## 부수적으로 확인된 것 — account-worker Aeron 테스트 병렬 제약
통합 중 `./gradlew :account-worker:test`를 이 워크트리에서 돌리다 ec의 독립 재확인 실행과
겹쳐 7개 실패가 났다. 별도 결정 기록([[aeron-test-parallel-isolation]])에 이미 문서화된
제약(Aeron media-driver 디렉터리 공유 추정)의 실제 재현 사례 — 겹치지 않게 다시 돌리니 26/26
GREEN.

## 결과
- v1·v2 개별 파이프라인(계좌·holdings·토큰·가격밴드·트리거·컨슈머 그룹 격리)을 소규모
  실기동으로 전부 확인 완료.
- 아직 안 한 것: `measure-e2e.sh`로 실제 10만 건 규모 워밍업+측정 3패스 전체 실행. 다음 작업.
- 관련: [[v1-v2-e2e-measurement]](설계)·[[measurement-instrumentation-permanent]](계측 영구
  반영 결정)·[[aeron-test-parallel-isolation]](병렬 테스트 제약).
