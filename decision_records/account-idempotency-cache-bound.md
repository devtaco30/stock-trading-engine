---
feature: account-idempotency-cache-bound
date: 2026-09-15
branch: fix/account-idempotency-cache-bound
commits: [8cae51e]
feeds: [adr, blog]
---

# 계좌 멱등 장부 3개의 무한증가(OOM) 막기 — 소스가 상한 방식을 정한다

v2 계좌 엔진 `AccountState`는 각 계좌의 잔고·보유를 프로세스 메모리에 들고 있고(락 없는 single-writer = 속도 축), 여기에 "이미 처리한 일"을 기억하는 멱등 장부 3개가 붙어 있다 — `processedTradeIds`·`processedSettlementRefs`(Set)·`requestIdToOrderId`(Map, `AccountState.java:40-42`). 셋 다 지우는 규칙이 없어 계좌 객체가 메모리에 있는 동안 무한 증가하고, 스냅샷에도 통째로 저장돼 스냅샷 파일·직렬화 비용까지 부풀린다. 워커 세션이 찾은 v2 최고심각 릭.

**이 flush의 핵심**: 처음엔 소스 성격별로 "tradeId→워터마크 / settlement→LRU / requestId→TTL" 3분할로 설계했는데, 착수하며 코드·실측으로 검증하니 **세 결론이 다 바뀌었다.** tradeId는 워터마크가 불가능(스냅샷 정책과 얽혀 이번 범위서 뺌), requestId는 TTL이 아니라 orderId 자체가 불필요(Set으로 축소), settlement만 LRU가 맞되 N의 근거가 바뀜. "설계는 검증 전엔 가설"이라는 실물 사례.

## ADR 네타

### tradeId 워터마크는 왜 안 되나 — "순서 보장 ≠ 값 단조", 그리고 발급 소스를 봐야 한다
- **context**: 멱등 장부 상한을 두려면 "오래된 건 지운다"가 필요한데, 지운 id가 다시 와서 멱등이 뚫리면 체결이 이중 반영된다(돈). tradeId는 순서가 있어 보여서, 집합 대신 워터마크(본 것 중 최대 tradeId `long` 하나, `id ≤ max`면 무시)로 접으려 했다.
- **왜(오판)**: 근거로 "계좌가 먼저 저널 기록 → 그다음 반영(journal-before-apply)이라 저널이 tradeId의 빠짐없는 앞부분(prefix)=단조"를 들었다. **이 문장이 틀렸다.** journal-before-apply가 보장하는 건 "계좌가 받은 **도착 순서** = 저널 순서 = replay 순서"지, tradeId **값**이 커지는 순서로 온다는 뜻이 아니다. 도착 순서 보존과 값 단조를 등치시킨 게 오류.
- **어떻게(정정)**: tradeId 값이 단조인지는 순전히 **발급 소스**가 정한다. 확인하니 tradeId = Snowflake(`AccountFillPublisher.java:63` `snowflakeIdGenerator.nextId()`, 레이아웃 = 벽시계 타임스탬프 + nodeId 10비트 + sequence). 단일 매칭노드면 값 단조지만, **목표 아키텍처는 종목별 LMAX 매칭(종목축 다중 프로세스)**이라 한 계좌가 여러 매칭노드의 tradeId를 섞어 받는다. 같은 밀리초에 노드가 다르면 nodeId 비트로 대소가 갈려 **도착 순서 ≠ 값 순서** → 늦게 온 작은 tradeId를 `≤ watermark`로 무시 = 체결 유실(돈). settlement에서 정확히 경계한 함정("순서 보장 ≠ 단조 도착")을 tradeId에선 놓친 것.
- **무엇을**: 워터마크 폐기. tradeId는 LRU 후보로 내려갔다가, 아래 복구 겹침 문제로 이번 범위에서 아예 빠졌다.
- **결과·수치**: 설계 폐기(구현 안 함). Snowflake 다중노드 시나리오는 코드로 확인, 실측 아님.

### tradeId 상한은 복구 겹침이 정하고, 복구 겹침은 스냅샷 정책이 정한다 — 고정 N으로 못 덮는다
- **context**: tradeId를 LRU로 가려면 N을 정해야 하는데, N은 "복구 시 겹침 |J|"를 덮어야 한다. |J| = 재기동 복구 때 저널 replay와 체결(fill) 스트림 replay가 **같은 tradeId를 겹쳐 싣는 수**.
- **왜(구조)**: 복구는 스냅샷 증분이다(`AccountEngineConfig.java:85-87`, `restore(snapshot)` → 저널 replay → fill replay, 각각 스냅샷 위치 이후만). 체결 스트림을 재생하는 목적은 "크래시 직전 계좌가 못 받은 체결(저널에 없는 꼬리)" 복구인데(U4b), 재생 시작점이 스냅샷의 `fillConsumedPosition`(= 마지막 graceful stop 소비 위치, 오래됨)이라 이미 받아 저널에도 있는 **몸통**까지 딸려온다. 그 몸통이 겹침이고, 멱등이 걸러야 한다. 저널(스트림 4005)과 fill(6001)은 **position 공간이 달라** 겹침을 위치로 자를 수 없어 tradeId로만 잇는다.
- **어떻게(실측)**: |J| = "마지막 스냅샷 이후 처리한 체결 수". 그런데 **주기 스냅샷이 없다** — 스냅샷은 graceful stop 때만 찍히고(`AccountSnapshotLifecycle`, `@Scheduled` 0건), 크래시엔 마지막 graceful stop 스냅샷을 쓴다. 그래서 |J| = "마지막 graceful stop ~ 크래시" 전체 = run 길이에 상한 없음. 계수를 재려고 매칭 코어 JMH(`MatchingThroughputBenchmark`)를 돌렸다.
- **무엇을**: LRU N을 유한값으로 못 정한다는 결론 → tradeId는 "복구는 완전 dedup(임시 집합, 복구 후 버림) / 라이브는 작은 LRU"로 경로 분리가 필요하고, 이는 스냅샷 정책(주기 스냅샷 도입 여부)과 한 덩어리라 **이번 릭 수정 범위에서 뺐다**(별도 트랙).
- **결과·수치**: JMH 실측(Apple M1 Pro, 10코어, OpenJDK 21.0.3) — 매칭 코어 최대 처리량 blocking 967,114 / yielding 1,008,512 / busyspin 1,002,427 orders/s, 워크로드가 교차주문 1개=체결 1건이라 **체결 ≈ 초당 50만**. 핫계좌 최악 |J|: 1초 50만 / 1분 3천만(≈1.9GB, `HashSet<Long>` 64B/엔트리 가정) / 1시간 18억. run 길이에 상한 없음 → 어떤 고정 N도 복구 겹침을 못 덮음.

### settlement는 LRU가 맞다 — 근거는 "빈도 낮음"이 아니라 durable-before-ack
- **context**: `processedSettlementRefs`도 무한 증가. tradeId처럼 복구 겹침이 있나 확인해야 했다.
- **왜**: settlement도 저널에 기록되고(전 이벤트 기록) 복구 시 재적용된다. 하지만 재전송 소스가 tradeId(fill 스트림, 스냅샷 이후 전부·오래된 것부터)와 다르다.
- **어떻게(결정·근거)**: `AccountSettlementConsumer`가 `ack-mode: manual_immediate` + `enable-auto-commit: false`(`application.yml`) — 각 정산을 저널에 기록(`blockUntilJournaled`)한 뒤 **즉시 offset 커밋**한다. 그래서 재소비되는 건 "커밋 직전 처리 중이던 최근 1건"뿐. LRU가 밀어내는 건 오래된 것인데 재소비분은 최근이라 안 밀린다 → LRU로 안전. (처음엔 "정산 빈도가 낮아서"라고 근거를 댔는데 그건 정성적 추정이었고, 진짜 근거는 durable-before-ack다.)
- **무엇을**: Set → bounded LRU(OrderBook.filledOrderTimestamps 패턴 = `Collections.newSetFromMap(LinkedHashMap + removeEldestEntry)`). `AccountState.java:41`, 판정 `applySettlement`. 스냅샷/코덱 무변경(개수 기반). TDD로 evict(밀려난 것 재반영)·잔존 멱등 검증.
- **결과·수치**: N = 500. 근거 = 재소비 상한 = `max.poll.records`(미설정=Kafka 기본 500) = 한 poll 배치가 재소비할 수 있는 최대. 실제 겹침은 manual_immediate라 ≤ 1건, 500은 보수적 상한. 커밋 `8cae51e`. (처음 N=1000을 근거 없이 박았다가 Jack 지적으로 500+근거로 amend — "수치엔 근거"의 실물 실패 사례.)

### requestId는 TTL이 아니라 orderId 자체가 불필요했다 — "봤나"만 남기면 settlement와 같은 집합
- **context**: `requestIdToOrderId`(Map)도 무한 증가. 처음엔 "재전송 시 orderId를 되돌려주는 조회맵이라 워터마크(값 하나)로 못 접는다 → TTL(24h)"로 결론냈다.
- **왜(재검토)**: Jack이 "같은 orderId가 생기는 것 자체가 이상하지 않냐 / 이미 처리된 requestId가 다시 오는 게 말이 되냐"고 물어 재검토. 정리하니 — 같은 requestId 재도착은 오직 "응답을 못 받은 짧은 미응답 재시도" 창에만 생긴다(응답 받으면 클라는 재전송 안 함, 연속 주문은 새 requestId). 그리고 그 재전송에 **orderId를 돌려줄 필요가 없다**: 재전송 응답은 202+requestId 비동기라 orderId가 클라에 안 가고, `onDuplicateRequest`의 orderId 소비처는 로그 한 줄뿐(리스너 2개 no-op, `LoggingAccountResultListener`만 로그). "TTL 지나면 새 주문 취급"이라던 첫 설명도 틀렸다 — 만료될 만큼 오래된 재전송은 애초에 안 온다.
- **어떻게(결정)**: orderId 저장/반환을 제거. `Map<String,Long> requestIdToOrderId` → `Set<String> processedRequestIds`(settlement와 동일한 bounded LRU "봤나" 집합). `orderIdFor`/`rememberRequest` 폐기, `onDuplicateRequest(accountId, orderId, requestId)` → `onDuplicateRequest(accountId, requestId)`(구현체 15+ 시그니처 수정). 스냅샷·코덱의 requestId 블록에서 orderId 필드 제거. 셋 중 tradeId·settlement·requestId가 결국 다 같은 "봤나" 멱등 집합이고, tradeId만 복구 겹침이 커서 빠진 것.
- **무엇을**: 위 변경 진행 중(U2). 통합테스트 6개가 `orderIdFor`를 관측 통로로 쓰던 걸 `seq()` 완료 대기 + 결정론적 orderId 예측(`(nodeId<<53)|counter`)으로 전환(테스트 편의로 프로덕션 조회 API를 남기지 않음).
- **결과·수치**: LRU 상한 = 근거 미확정 임시값 5000. requestId가 덮어야 하는 "재전송 창"은 클라 재시도 정책·결과 폴링 조회 API(fork5 ⑤ 후속)에 묶이는데 둘 다 미구현이라 지금 근거를 못 댄다 → 임시임을 상수 주석·커밋에 명시, 폴링 API 설계 시 재산정. (settlement의 `max.poll.records` 같은 코드 상한이 requestId엔 없다.)

## 블로그 네타

### "무한히 자라는 멱등 장부, 상한을 '몇'으로? — 그리고 실측이 설계를 뒤집을 때"
- **훅·핵심 주장**: dedup 상태를 아무 숫자로 자르면 돈이 틀어진다(정합성 창). 소스마다 답이 다르다는 것까지는 설계로 나오지만, **그 설계가 검증 전엔 전부 가설**이다 — 이 작업에선 세 장부의 첫 결론(워터마크/LRU/TTL)이 코드·실측 앞에서 다 바뀌었다.
- **context**: 계좌를 accountId로 샤딩해 상태를 인메모리로 들면(속도 축) 멱등 장부가 무한 증가한다. 세 장부가 각각 다른 소스(Aeron 체결 / Kafka 정산 / 클라 요청)라 답이 갈린다.
- **어떻게(서사·근거)**: ①tradeId 워터마크가 "저널=단조 prefix"라는 그럴듯한 근거로 섰다가, 발급 소스가 Snowflake(다중 매칭노드)임을 보고 무너진다 — "순서 보장 ≠ 값 단조". ②LRU로 내려갔다가, N을 정하려 복구 겹침을 파보니 그게 스냅샷 정책과 한 덩어리고, JMH로 코어 처리량(초당 50만 체결)을 재보니 주기 스냅샷 없이는 어떤 고정 N도 못 덮는다는 게 나온다 → tradeId는 아예 뺀다. ③requestId는 "orderId를 돌려줘야 하니 TTL"이라던 게, 그 orderId가 실은 로그 한 줄 외엔 안 쓰인다는 확인으로 "봤나 집합"으로 축소된다. ④settlement만 LRU로 남되, 근거가 "빈도 낮음"에서 durable-before-ack(manual_immediate)로 바뀐다. 곁가지로 "이미 처리된 requestId가 왜 다시 오나"라는 질문이 멱등의 실제 수명(미응답 재시도 창)을 드러낸다.
- **재료(커밋·도식·수치)**: `AccountState.java:40-42` / `AccountFillPublisher.java:63`(tradeId=Snowflake) / `AccountEngineConfig.java:85-87`(복구 3단계) / `AccountSnapshotLifecycle`(graceful-only 스냅샷) / `AccountSettlementConsumer`(manual_immediate) / 대조군 `OrderBook.java:63-77`(LRU 패턴) / JMH `MatchingThroughputBenchmark`(50만 체결/s, M1 Pro) / 도식 `docs/_recovery_overlap_flow.html`(복구 겹침 몸통·꼬리) / settlement 커밋 `8cae51e`. tradeId 별도 트랙 = 스냅샷 정책(주기 스냅샷 or 복구 전용 dedup).
