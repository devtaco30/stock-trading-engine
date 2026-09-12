---
feature: id-idempotency-determinism
date: 2026-09-09
branch: feat/disruptor-matching-core
commits: [362f05d, e1fdd73]
feeds: [adr, blog]
---

# id 발급·재전송 멱등·결정론 — 계좌 워커가 orderId를 찍는다

## ADR 네타
### ADR-022 requestId 재전송 멱등 = 인메모리 single-writer
- **context(무슨 상황)**: 네트워크 재시도로 같은 주문이 두 번 처리되는 걸 막아야 한다. v1은 orders 테이블의 requestId UNIQUE 제약으로 막았다(ADR-008).
- **왜(문제·동기)**: v2가 DB 락을 없애고 검증을 인메모리 single-writer로 옮기면서 requestId UNIQUE(DB)라는 안전장치도 같이 사라졌다. 그럼 멱등을 어디서 보장하느냐. 처음에 Redis를 추천했는데 Jack이 근거가 약하다고 지적해서 재고했다. 검증은 핫패스이고 핫패스에서는 외부 I/O를 안 한다는 원칙이 있다.
- **어떻게(대안·결정·트레이드오프)**: account-worker 인메모리에 `processedRequestIds`를 둔다. 체결 tradeId 멱등을 두는 그 자리다. 핸들러가 계좌 확인 후·검증 전에 `tryMarkRequest`를 호출하고, 재전송이면 재예약을 안 하고 `onDuplicateRequest`로 처리한다. 외부 저장소는 쓰지 않는다. 대가는 이 세트가 무한히 커진다는 것인데, C6 스냅샷에서 절단한다.
- **무엇을(실제 변경·파일·커밋)**: C5-1a 커밋 `362f05d`. accept/reject/invalid 모두 첫 등장에 마킹하고, 재전송은 같은 결과를 돌려준다.
- **결과·수치**: (없음, 정성)

### ADR-023 orderId 발급 위치 + requestId→orderId 멱등 맵 = 계좌 워커
- **context(무슨 상황)**: v2 핫패스엔 DB가 없다. orderId(서버 발급)를 누가 찍고, 재전송(같은 requestId 두 번) 때 같은 orderId를 어떻게 돌려주는지 정해야 한다. v1은 DB 저장 + requestId UNIQUE로 해결했는데 그게 사라졌다. ADR-022의 `processedRequestIds`(Set)를 map으로 확장하는 결정이다.
- **왜(문제·동기)**: 재전송에 같은 orderId를 돌려주려면 requestId와 orderId의 짝을 어딘가 보관해야 한다. 그 위치가 게이트웨이냐, 계좌 워커냐, Redis냐.
- **어떻게(대안·결정·트레이드오프)**: 대안은 (가) 게이트웨이가 orderId를 찍고 requestId→orderId 맵을 보관해 재전송을 조기 차단, (나) 계좌 워커가 orderId를 찍고 맵을 보관(이미 processedRequestIds가 있는 그 자리), (다) Redis에 맵을 두고 게이트웨이에서 조기 차단. 결정은 (나) 계좌 워커, Jack이 2026-09-08 confirm. 근거는 single-writer 원칙과 일치(한 계좌의 모든 상태가 한 곳에 있고 복구 스냅샷에 같이 실림)하고, 이미 재전송 방어가 계좌 워커에 있다는 것. (가)·(다) 기각 근거: (나)에서는 orderId가 계좌 워커에서 태어나므로 첫 요청 순간 게이트웨이/Redis엔 아직 짝이 없어 빠른 재전송을 조기 차단할 수 없다(맵이 왕복 뒤에야 채워진다). Redis는 네트워크 홉 ms이라 전송 반전(ADR-020, Aeron µs)으로 뺀 I/O를 핫패스에 다시 얹는 꼴이고 ADR-022의 "핫패스 외부 I/O 금지"를 위반한다. 게이트웨이가 막아도 크래시·인스턴스 경합으로 새어들어 계좌 워커 가드가 어차피 남으므로 dedup을 두 곳에 두면 책임이 분산된다(Jack이 스스로 짚음). Redis는 권위가 못 되고 나중에 best-effort 앞단 필터로만 선택적으로 둘 수 있다.
- **무엇을(실제 변경·파일·커밋)**: 구현 C5-2a. 인바운드 메시지에서 orderId를 제거(sender는 requestId만 보낸다). 계좌 워커가 첫 등장 requestId에 orderId를 발급하고 `requestIdToOrderId` map에 저장한 뒤 검증·예약한다. 재전송은 map에서 원 orderId를 꺼내 `onDuplicateRequest`에 실어 반환한다. ADR-022의 Set을 이 map으로 대체한다. 대가는 orderId가 계좌까지 가야 생겨 입구에서 즉시 반환할 수 없다는 것 — 클라이언트는 requestId 폴링으로 나중에 받는다(v2의 202 + 폴링 비동기 모델과 일치).
- **결과·수치**: 미착수 상태로 LLD를 워커에 전달. 코어 배선은 core의 프레임워크 없는 id 생성기(matching-worker의 `SnowflakeConfig` 패턴 재사용). 프로젝션·조회모델의 orderId도 이 값을 쓴다.

### ADR-023 갱신 orderId 발급 = Snowflake → 결정론적 (nodeId, 카운터)
- **context(무슨 상황)**: 위 (나)로 계좌 워커가 orderId를 찍기로 정했다(C5-2a, 처음엔 Snowflake). 2단계 durability/리플레이 설계를 하다 보니, 리플레이가 같은 입력에 같은 orderId를 재현해야 하는데 Snowflake는 벽시계 기반이라 재시작마다 값이 달라진다. 그러면 복구된 계좌가 매칭에 남아 있는 옛 orderId와 어긋난다(유령·불일치).
- **왜(문제·동기)**: 리플레이 결정론은 엔진(business logic processor)이 입력의 순수 함수여야 성립한다. LMAX의 근본 원칙 — 엔진 안에 벽시계·랜덤을 두지 않는다. Snowflake는 엔진 안의 비결정 요소다.
- **어떻게(대안·결정·트레이드오프)**: 대안 A는 게이트웨이가 부여하고 기록(C5-2a를 되돌림), B는 엔진이 찍고 곁저널로 보관, C는 엔진이 결정론 카운터로 찍음. B는 곁저널로 때우는 방식이라 탈락, A는 정석이지만 되돌림 + 게이트웨이 선행이 필요. 결정은 C — C5-2a의 (나)를 그대로 두고 orderId 소스만 Snowflake에서 `(nodeId << 53) | counter`로 바꾼다. nodeId는 전역 유일, counter는 엔진 상태라 재현된다. 시간 순서는 orderAt이 담당한다. 되돌림·게이트웨이·곁저널이 없다. 같은 원칙을 매칭 tradeId에도 적용(2c).
- **무엇을(실제 변경·파일·커밋)**: 2b-0 커밋 `e1fdd73`. `AccountOrderIdGenerator`, 카운터는 엔진 상태(2d 스냅샷 대비), 시임으로 nodeId 주입, `SnowflakeConfig` 삭제. 결정론 테스트(fresh 엔진 둘에 같은 입력 → 같은 id) PASS.
- **결과·수치**: (없음, 정성)

### (선택) 시간값 epochMillis 통일 — ADR감이나 룰로도 충분
- **context(무슨 상황)**: a2 정산의 `dueDate`를 LocalDate로 박았다가 Jack 격노. 이건 ADR로 세우기엔 무게가 약하고 이미 룰([[시간값은 epochMillis, LocalDate 금지]])로 존재한다.
- **왜(문제·동기)**: LocalDate는 '며칠'을 zone에 따라 정해서 비교할 때 드리프트가 난다. 이 프로젝트는 zone 논의 자체가 불필요하다.
- **어떻게(대안·결정·트레이드오프)**: 모든 시간 필드를 long epochMillis로 통일(Instant도). 신규는 처음부터 그렇게, 기존 Instant 통일은 v2 완료 후 리팩터(TODO-2). 덧붙여 처음에 "jsr310이 없어서 LocalDate가 터진다"고 오진했는데, build 파일 grep으로 클래스패스를 단정한 실수였다(실제로는 전이 의존이 있었다 — `gradlew dependencies`로 확인). epochMillis 근거는 zone뿐이고 직렬화가 아니다.
- **무엇을(실제 변경·파일·커밋)**: `SettlementRequestEvent.dueAtEpochMillis`.
- **결과·수치**: (없음, 정성)

## 블로그 네타
### "requestId 멱등 v1(DB UNIQUE)→v2(인메모리 single-writer)"
- **훅·핵심 주장**: 같은 주문을 두 번 처리하지 않는 멱등을, v1은 DB UNIQUE 제약으로 보장했다. v2는 DB 락을 없애면서 그 안전장치도 잃었고, 멱등을 account-worker 인메모리 single-writer로 옮겼다.
- **context**: 네트워크 재전송에 같은 주문이 중복 처리되는 걸 막는 방법의 v1→v2 변화.
- **어떻게(서사·근거)**: v1 orders 테이블 requestId UNIQUE(ADR-008). v2에서 DB 락 제거로 그 제약이 사라짐. Redis 추천 → 근거 약함 지적 → 핫패스 외부 I/O 금지 원칙으로 인메모리 `processedRequestIds` 선택. 세트 무한 증가는 스냅샷 절단으로 해결.
- **재료(커밋·도식·수치)**: C5-1a `362f05d`.

### "리플레이가 되려면 id가 결정론적이어야 한다 — Snowflake는 왜 안 되나"
- **훅·핵심 주장**: 크래시 복구를 저널 리플레이로 하려면 엔진이 입력의 순수 함수여야 한다. Snowflake는 벽시계 기반이라 재시작마다 다른 값을 내서, 복구된 상태가 매칭에 남은 옛 id와 어긋난다.
- **context**: 계좌 워커가 orderId를 발급하기로 정한 뒤(ADR-023), 2단계 durability 설계에서 리플레이 결정론이 걸린 지점.
- **어떻게(서사·근거)**: LMAX 근본 원칙 — 엔진 안에 벽시계·랜덤 금지. 대안 A(게이트웨이 부여)·B(엔진 발급 + 곁저널)·C(엔진 결정론 카운터) 비교. C 선택 — `(nodeId << 53) | counter`, counter는 엔진 상태라 재현. 시간 순서는 별도 필드(orderAt). 같은 원칙을 매칭 tradeId에도.
- **재료(커밋·도식·수치)**: 2b-0 `e1fdd73`, `AccountOrderIdGenerator`. 결정론 테스트 PASS.
