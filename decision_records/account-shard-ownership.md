---
feature: account-shard-ownership
date: 2026-09-18
branch: feat/account-shard-ownership (main 미병합, main ca43490 기준)
commits: [11a50a8, a831edc, a65cfe2, 2c28a4f, 25e9e50, 099c24d]
feeds: [adr, blog]
---

# 계좌 샤딩 U1~U6 — 정적 설정에서 Kafka 컨슈머 그룹 배정으로

## ADR 네타

### ADR 후보: 배정을 사람이 적지 않는다 — Kafka 컨슈머 그룹으로 슬롯을 나눠 갖는다

- **context(무슨 상황)**: 계좌 상태(잔고·예약)는 각 계좌 워커의 메모리에만 있다(ADR-031, 락 없는 single-writer). 워커를 여러 대 띄워 계좌를 나눠 맡기려면, accountId를 슬롯 번호로 해시한 뒤(`ShardRoutingTable`, 이미 있음) "그 슬롯을 지금 어느 워커가 맡고 있는가"를 누군가 정해야 한다. 기존 설계(`decision_records/c5-multiprocess-coordination.md`)는 이걸 정적 파일(`shard-routing.shards`)에 사람이 슬롯 범위→endpoint로 적어 두는 방식이었다.
- **왜(문제·동기)**: 사람이 적은 정적 표는 워커가 죽었을 때 재배치하려면 사람이 다시 파일을 고쳐 재배포해야 한다. k8s ConfigMap은 kubelet이 ~2분에 걸쳐 staggered 전파하므로, 배포 중 sender마다 맵 뷰가 다를 수 있어 split-brain(잔고 오염) 위험이 있다.
- **어떻게(대안·결정·트레이드오프)**: 조정용 Kafka 토픽 `account-shard-assignment`(파티션 수 = slot-count = 256)를 만들고, 이 토픽에는 메시지를 한 번도 보내지 않는다 — **파티션 번호를 그대로 슬롯 번호로 쓴다.** 워커들이 컨슈머 그룹 `account-shard-owners`에 조인하면, Kafka의 기존 파티션 배정 메커니즘이 곧 "슬롯을 누가 맡는가"가 된다. 사람이 적는 것은 슬롯 개수(`shard-routing.slot-count`)와 워커가 몇 대 뜨는지뿐 — "누가 몇 번 슬롯"은 안 적는다. 대안(정적 표를 자동화 스크립트로 재생성)은 여전히 "누가 쓰는 배포 파이프라인이 그 스크립트를 실제로 실행했는가"라는 새 신뢰 지점을 만들어 기각했다.
- **무엇을(실제 변경·파일·커밋)**: `account-worker/.../coordination/KafkaShardAssignment.java`(U4, 커밋 `2c28a4f`) — `group.instance.id`(워커 고정 신원, `account-worker.instance-id` 없으면 자기 인테이크 endpoint로 폴백)로 static membership을 켠 컨슈머가 조인, `ConsumerRebalanceListener.onPartitionsAssigned`가 받은 파티션 번호 집합이 그대로 담당 슬롯.
- **결과·수치**: 로컬 단일 브로커(docker-compose KRaft)에서 256파티션 토픽 생성 248ms, 컨슈머 1개 최초 배정(256개 전부) 3315ms, 2번째 컨슈머 합류 시 128/128 리밸런스 3224ms. 슬롯 수는 이 실측을 근거로 256 그대로 유지하기로 했다(줄이거나 파티션 하나가 슬롯 여러 개를 맡는 방식은 기각 — ADR-031의 "워커를 늘려도 계좌를 재배치하지 않는다"는 전제를 지키려면 슬롯 수 자체를 프로세스 수에 맞춰 줄일 이유가 없고, 파티션:슬롯을 1:1이 아닌 다른 비율로 두면 매핑 계층이 하나 더 생기는데 지금 그걸로 얻는 게 없다).

### ADR 후보: L1(재배정 없음)을 골랐다 — 그 대가는 "죽은 워커의 계좌만 몇 초 503"

- **context**: 워커가 죽으면(크래시든 정상 종료든) 그 워커가 맡던 슬롯을 어떻게 할지 정책이 필요하다. 후보는 셋 — L1(재배정 안 함, 같은 신원이 돌아올 때까지 대기) / L2(대기 워커가 즉시 이어받음, warm standby) / L3(L2 + 옛 주인 차단 fencing).
- **왜**: L2·L3는 대기 프로세스·저널 복제·fencing 토큰까지 얽혀 이 트랙 하나로 감당하기엔 크다(LLD가 이미 L3를 "이 트랙 범위 아님"으로 명시). L1은 Kafka의 static membership(`group.instance.id`) 기능 하나로 "재시작해도 같은 슬롯"을 거의 공짜로 얻는다.
- **어떻게(대안·결정·트레이드오프)**: static membership 컨슈머는 정상 종료(`close()`)해도 브로커에 LeaveGroup을 보내지 않는다 — 그래서 크래시든 정상 종료든 구분 없이, 같은 신원이 `session.timeout.ms` 안에 돌아오면 브로커가 그 슬롯을 그대로 돌려준다. api·매칭 쪽은 그 슬롯의 새 주문/체결을 목적지 없음(503, 매칭은 조용한 이벤트 폐기 — 아래 항목 참고)으로 처리하고, 클라이언트가 재전송하면 계좌 엔진의 requestId 멱등이 중복을 흡수한다. **대가는 "그 계좌만 몇 초 동안 503"이지 유실이 아니다** — 주문 경로는 재전송으로 안전하지만, 매칭이 만들어낸 체결 경로는 재전송 주체가 없어 이 대가가 그대로 안 적용된다(아래 "미해결" 항목).
- **무엇을**: `KafkaShardAssignment`가 `group.instance.id`를 명시적으로 설정(`ConsumerConfig.GROUP_INSTANCE_ID_CONFIG`). `session.timeout.ms`는 기본 45000보다 길게(현재 120000=2분, 잠정값 — 아래 참고).
- **결과·수치**: L1 테스트로 "session.timeout(테스트 10초) 이전 3초 시점엔 재배정이 없다"를 실제 브로커로 확인. `session.timeout.ms=120000`은 워커 재시작 시간(프로세스 시작~저널 재생~주문 수신 가능)을 실측해 재산정해야 하는데(LLD의 U7), **이번 트랙에서 U7은 실행하지 않았다** — 사용자 토큰 예산 문제로 조정 세션이 보류. 그래서 120000은 "기본 45000보다 넉넉히 길게"라는 안전 쪽 추정치일 뿐 실측 근거가 없다.

### ADR 후보: static membership만으로는 L1이 안 지켜진다 — U6로 애플리케이션 코드에 한 번 더 못 박았다

- **context**: static membership은 재배정을 **없애는 게 아니라 미루는 것**이다. `session.timeout.ms`가 실제로 지나면, Kafka는 죽은 멤버의 파티션을 살아 있는 멤버에게 재배정한다(eager rebalance). 즉 "살아 있는 다른 워커가 안 가져간다"는 L1의 문장은 Kafka 프로토콜 수준에서는 참이 아니다 — 애플리케이션이 스스로 지켜야 하는 규칙이다.
- **왜**: 재배정으로 새 슬롯을 받은 워커는 그 슬롯 계좌들의 상태(잔고·예약)를 메모리에 가진 적이 없다. 받아들이면 "상태 없이 주문을 처리"하게 된다 — L1이 막으려던 정확히 그 상황이다.
- **어떻게(대안·결정·트레이드오프)**: `KafkaShardAssignment`가 **워커가 살아 있는 동안 처음 받은 배정만** 진짜로 받아들인다(`initialSlots`, 한 번 기록된 뒤 불변). 그 뒤 `onPartitionsAssigned`가 처음 배정에 없던 새 슬롯을 들고 오면(경로 무관 — 설정이 뚫렸든, 다른 워커가 죽어 재배정됐든) 거부하고 경고 로그만 남기며, `account-shard-map`에도 그 슬롯에 대해 아무것도 발행하지 않는다(내가 담당인 척하지 않음 — 발행하면 api·매칭이 계속 여기로 보내고 매번 NOT_OWNED로 어긋난다). 슬롯이 **줄어드는 것**(회수)은 그대로 받아들인다(내가 죽는 중일 수 있어 막을 이유가 없다).
- **무엇을**: `KafkaShardAssignment.onPartitionsAssigned`(U6, 커밋 `099c24d`) — `initialAssignmentReceived` 플래그 + `initialSlots` 집합으로 이후 배정을 필터링.
- **결과·수치**: **이 트랙에서 가장 공들인 검증.** 워커 둘을 128/128로 띄우고 하나를 `close()`(static membership이라 LeaveGroup 안 보냄)한 뒤, `session.timeout`(테스트 10초)이 **실제로 지날 때까지 + 여유 10초**를 기다려 Kafka가 **진짜로 재배정하는 것까지 만들어 놓고**, 그래도 생존 워커의 담당 슬롯 수가 재배정 전과 그대로임을 확인했다(`KafkaShardAssignmentIntegrationTest#세션_타임아웃이_지나_재배정돼도_최초_배정에_없던_슬롯은_거부한다`). "타임아웃 전이라 아직 안 뺏겼다"가 아니라 "타임아웃이 지나 실제로 뺏길 수 있는 상황을 만들어도 코드가 거부한다"를 증명했다는 점이 L1을 설정이 아니라 코드로 못 박았다는 근거다.

### ADR 후보: 매칭의 "fail-fast" 자기 오판 — 실제로는 조용한 단건 폐기였다

- **context**: U5에서 목적지를 못 찾으면(`AccountDestinationResolver.endpointFor`가 empty) `AccountFillPublisher`가 `IllegalStateException`을 던지도록 짜고, 커밋 메시지·클래스 주석에 "매칭 소비자 스레드를 fail-fast로 멈춘다"고 적었다 — **코드를 실제로 추적하지 않고 짐작으로 쓴 문장**이었다.
- **왜**: 조정 세션이 "그 예외를 실제로 누가 받고 그다음 무슨 일이 일어나는지 코드로 확인해라"고 물어 다시 추적했다. `MatchingEventHandler.onEvent`는 `IllegalArgumentException | IllegalStateException`을 "도메인 불변식 위반이니 이 이벤트만 폐기"하는 catch로 이미 잡고 있었다 — 매칭 소비자는 멈추지 않고 계속 돈다. U5 이전엔 이 catch가 실질적으로 도달 불가능했다(`ShardRoutingTable.endpointFor`는 항상 값이 있어 null·예외가 안 남) — U5가 동적 목적지 조회를 들이면서 처음으로 실제로 밟히는 경로가 됐다.
- **비대칭 발견**: `requireEndpoint`(resolver가 목적지를 아는가)와 `enqueue`(그 목적지가 `shard-routing.endpoints` Publication 풀에 있는가)는 서로 다른 데이터를 확인한다. 매수·매도 둘 다 `requireEndpoint`를 통과해도(둘 다 목적지 문자열은 안다), `enqueue`는 매수→매도 순서로 불려서 매수 쪽 endpoint만 풀에 있으면 매수는 알림이 가고 매도 쪽에서 예외가 나 매도만 유실되는 비대칭이 실제로 가능함을 코드로 증명했다.
- **어떻게(대안·결정·트레이드오프)**: 목적지를 못 찾는 경우를 성격이 다른 셋으로 나눴다(U6, 조정 세션이 구분).
  1. **기동 직후 맵을 아직 못 읽음** — 매칭 엔진 스레드에서 바운드 있게 재시도하는 방안을 처음 제안했으나, "그 스레드가 10초 재시도하면 그 종목 매칭 전체가 10초 멈춘다"는 지적으로 반려. 대신 **기동 순서**로 막는다 — `AssignmentDestinationResolver.awaitInitialCatchUp`(초기 읽기 완료까지 블로킹, `consumer.position()`은 poll 스레드에서만 호출)을 추가하고, `AssignmentDestinationResolverLifecycle`(SmartLifecycle phase 0)이 이를 기다린 뒤에야 `MatchingOrderReceiverLifecycle`(phase 1)이 주문 인테이크 구독을 연다. 이 레포가 이미 쓰던 SmartLifecycle phase 패턴을 그대로 썼다.
  2. **운영 중 그 슬롯의 주인이 없음(계좌 워커가 죽어 tombstone됨)** — **미해결로 명시적으로 남겼다.** `requireEndpoint`의 `IllegalStateException`은 여전히 `onEvent`의 catch에 걸려 그 체결이 조용히 폐기된다(재전송·대기열 없음). 큐+상한+주인 복귀 시 재발행이 얽힌 별도 유닛 크기라 이 트랙 범위 밖으로 남겼다 — "쌓아뒀다 보내는 것은 미구현"임을 `AccountFillPublisher` 클래스 주석과 커밋 메시지에 명시했다.
  3. **목적지는 알지만 풀에 없는 endpoint(설정 어긋남)** — 신설 `AccountDestinationMisconfiguredException`(`IllegalArgumentException`·`IllegalStateException` 둘 다 아님)을 던져 `onEvent`의 catch를 피하고 `MatchingExceptionHandler`(진짜 fail-fast)까지 올라가게 했다. 재시도해도 안 나아지는 설정 오류라 fail-fast가 맞다는 판단.
- **무엇을**: `AccountDestinationMisconfiguredException.java`(신규) · `AccountFillPublisher.enqueue`가 이 타입을 던지도록 변경 · `AssignmentDestinationResolver.awaitInitialCatchUp` · `AssignmentDestinationResolverLifecycle`(matching-worker)가 이를 기다림. 커밋 `099c24d`.
- **결과·수치**: 새 통합테스트로 "await 리턴 직후 이미 반영돼 있다"(순서 보장)를 실제 브로커로 확인. 단위테스트로 `AccountDestinationMisconfiguredException` 타입 확인. 반대로 2번(운영 중 주인 없음)은 의도적으로 안 고쳤으므로 회귀 테스트가 없다 — 그 체결이 사라진다는 것 자체가 현재 동작.

### ADR 후보: 핫패스 오토박싱 — Set\<Integer\>에서 volatile boolean[]/String[]로

- **context**: `KafkaShardAssignment.test(int slot)`(주문마다 호출)과 `AssignmentDestinationResolver.endpointFor`(주문·체결마다 호출)가 처음엔 `Set<Integer>`/`Map<Integer,String>`으로 슬롯을 들고 있었다.
- **왜**: `Integer.valueOf`는 -128~127만 캐시한다. slot-count가 256이면 슬롯 128 이상에 떨어지는 주문마다 매번 `Integer` 객체를 새로 만든다 — 이 프로젝트가 v2 전체에서 락·할당을 걷어내는 것을 핵심 주장으로 삼아 온 것(p50 12ms→1.9ms)과 어긋난다는 게 조정 세션의 코드리뷰 지적이었다.
- **어떻게**: 내부 저장을 `volatile boolean[]`(KafkaShardAssignment, 슬롯 소유 여부)·`volatile String[]`(AssignmentDestinationResolver, 슬롯→endpoint)로 바꿨다. 읽는 쪽은 volatile 참조 한 번 읽고 배열 인덱스 조회 하나로 끝난다(박싱 없음). 쓰는 쪽(리밸런스 콜백)은 배열을 `clone()`해 채운 뒤 참조를 통째로 갈아끼운다 — 같은 volatile happens-before가 그대로 성립하고, 콜백은 항상 poll 스레드 하나에서만 불려 쓰기 쪽끼리 경합도 없다.
- **무엇을**: `KafkaShardAssignment.ownedSlotFlags`, `AssignmentDestinationResolver.slotToEndpoint`. 커밋 `2c28a4f`(U4)·`25e9e50`(U5).
- **결과·수치**: 미측정(실제 GC/할당 프로파일링은 안 함 — 코드 리뷰로 지적된 패턴을 제거한 것이 목적. 기존 통합테스트가 그대로 통과함을 재실행으로 확인).

## 블로그 네타

### "다른 세션이 준 설계 문서가 66커밋 뒤처져 있었다"

- **훅·핵심 주장**: 조정 세션이 준 LLD(작업 지시서)를 그대로 믿지 않고 코드를 먼저 grep해서 전제를 재검증했더니, LLD 자체가 이미 끝난 작업을 "아직 안 된 것"으로 잘못 서술하고 있었다 — 지시서보다 코드가 항상 더 최신이다.
- **context**: "api가 목적지 하나로만 보낸다"는 전제로 U1(목적지 조회 인터페이스화)을 시작했는데, 실제 코드(`AccountOrderPublishConfig`)를 열어보니 이미 다른 트랙(I8)이 `ShardRoutingTable.distinctEndpoints()`로 목적지별 Publication을 열고 있었다.
- **어떻게(서사·근거)**: ①U1을 구현하고 커밋한 뒤 U2 착수 전 관련 코드를 훑다가 이 불일치를 발견. ②조정 세션에 "당신 LLD의 전제가 틀렸다"를 코드 근거(파일:줄번호)와 함께 보고. ③조정 세션이 원인을 인정 — 자신이 LLD를 쓸 때 참조한 워크트리가 main보다 66커밋 뒤처져 있었다(I8이 병합되기 전 코드를 보고 썼다). ④"새로 만들기"였던 U1~U3의 성격이 "이미 있는 정적 구현을 인터페이스 뒤로 옮기기"로 바뀌었고, 실제로 작업량이 줄었다(코드량이 늘면 뭔가 잘못됐다는 게 조정 세션이 미리 건 기준이었는데, 정확히 반대로 줄어든 것이 방향이 맞다는 신호가 됐다).
- **재료(커밋·도식·수치)**: 커밋 `11a50a8`(U1, 순수 신규) vs `a831edc`(U2, 6개 파일 60줄 추가/24줄 삭제 — 새로 만들기가 아니라 갈아끼우기라 diff가 작음). `AccountOrderPublishConfig.java`·`ShardRoutingConfig.java`(api) 파일이 직접 증거.

### "정적 slot→endpoint 표와 동적 Kafka 배정 사이, Publication은 왜 여전히 정적인가"

- **훅·핵심 주장**: "배정을 동적으로 만든다"고 해서 시스템의 모든 부분이 동적이어야 하는 건 아니다 — "존재하는 워커가 무엇인가(멤버 목록)"와 "그 멤버가 지금 어느 슬롯을 맡는가(배정)"는 서로 다른 질문이고, 이 트랙은 후자만 동적으로 만들었다.
- **context**: Kafka 배정 모드에서 api·매칭이 Aeron Publication을 언제 열지가 애매했다 — "지금 존재하는 endpoint 목록"을 기동 시점에 알 방법이 없어 보였다(account-shard-map은 워커가 실제로 배정받아야 채워지는데 그건 기동 후).
- **어떻게(서사·근거)**: ①"풀 자체도 동적으로 만들자(account-shard-map에서 처음 보는 endpoint가 나오면 그 자리에서 Publication을 새로 연다)"는 안과 ②"가능한 워커 주소 풀은 여전히 정적으로 안다(k8s StatefulSet이면 pod 0..N-1 주소가 이미 고정)"는 안을 조정 세션에 함께 제시. 조정 세션이 ②를 택했다 — 근거는 "사용자가 싫다고 한 것은 '슬롯 3부터 7은 워커 A' 같은 할당표를 사람이 미리 적는 것이었지, 워커가 몇 대 있고 주소가 무엇인지(멤버 목록)를 아는 것이 아니었다"는 처음 정리에서 이미 그어져 있던 선이었다. `shard-routing.endpoints`(슬롯 범위 없는 순수 주소 목록)를 신설해 이 선을 지켰다.
- **재료(커밋·도식·수치)**: `AccountOrderPublishConfig.dynamicAccountOrderPublications`·`MatchingFillPublishConfig.dynamicMatchingFillPublications`(둘 다 U5, 커밋 `25e9e50`) — `shard-routing.endpoints` 풀 전체로 Publication을 미리 열고, 아직 안 뜬 워커의 Publication은 NOT_CONNECTED 상태로 남아도 에러로 다루지 않는다(Aeron `addPublication`은 연결 여부를 검사하지 않고 즉시 성공한다는 것도 이때 코드로 확인).
