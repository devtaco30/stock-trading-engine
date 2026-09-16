---
feature: account-shard-routing-step1
date: 2026-09-16
branch: feat/account-shard-routing
commits: [18147e3, 58ae59b]
feeds: [adr, blog]
---

# 계좌 샤딩 첫 단계 — 라우팅과 소유 판정 (I8 U1~U2)

## ADR 네타

### 제목 후보: "샤딩 설정을 core로 옮기지 않고 각 호스트 모듈에 얇게 둔 이유"
- **context(무슨 상황)**: v2는 계좌 잔고를 계좌 프로세스 메모리에 두고 락을 없앴다. 프로세스를 늘려 처리량이 오르는지 증명하려면, api가 계좌번호를 보고 목적지 프로세스를 고르고, 계좌 워커가 자기 담당 슬롯만 받아야 한다. 이 라우팅 표(`ShardRoutingTable`, 슬롯→endpoint)는 이미 core에 순수 로직으로 있었다(matching-worker가 체결 fan-out에 씀).
- **왜(문제·동기)**: 지시서는 이 표를 만드는 Spring 배선 클래스(`ShardRoutingProperties`·`ShardRoutingConfig`)까지 core로 옮겨 api·matching-worker·account-worker가 공유하라고 했다. 그런데 `core/build.gradle.kts`를 직접 열어보니 Spring 의존성이 0이다 — core는 프레임워크 없는 순수 라이브러리로 유지하는 게 이 프로젝트의 설계 원칙(ADR-018)이고, `ShardRoutingTable`이 지금 core에 있는 이유도 그게 Spring과 무관한 순수 함수라서다. `ShardRoutingProperties`(`@ConfigurationProperties`)·`ShardRoutingConfig`(`@Configuration`)는 Spring 클래스라 옮기면 core에 Spring 의존성이 새로 생긴다.
- **어떻게(대안·결정·트레이드오프)**: api·matching-worker·account-worker 셋이 공통으로 의존하는 모듈이 core뿐이라(각 모듈의 build.gradle.kts 확인 — account-worker는 core+account-disruptor만, matching-worker는 core+trading+matching-disruptor만) "이미 있는 모듈에 옮겨 공유"는 core로 갈 수밖에 없었는데, 그게 원칙과 부딪혔다. 대안은 세 호스트 모듈에 각자 얇은 Spring 배선(40줄 안팎)을 두는 것 — 순수 로직(`ShardRoutingTable`)은 core에 공유하고, Spring 배선만 중복한다. 대가는 이 40줄이 세 파일에 반복된다는 것. 채택했다.
- **무엇을(실제 변경·파일·커밋)**: `api/.../config/ShardRoutingProperties.java`·`ShardRoutingConfig.java` 신규(matching-worker 미러), `account-worker/.../config/ShardRoutingProperties.java`·`ShardRoutingConfig.java` 신규. 커밋 `18147e3`(api)·`58ae59b`(account-worker).
- **결과·수치**: 미측정(구조 변경, 처리량 측정은 U3 몫).

### 제목 후보2: "계좌 워커는 담당 슬롯을 표에서 찾고, 별도로 적지 않는다"
- **context(무슨 상황)**: api는 계좌 샤드 목적지 목록 전체가 필요하다(어디로 보낼지 골라야 하니까). 계좌 워커는 반대로 "내가 뭘 담당하는지" 하나만 알면 된다.
- **왜(문제·동기)**: 처음 떠오르는 방법은 계좌 워커에 `account-worker.owned-slots.from/to` 같은 새 프로퍼티를 만드는 것이다. 하지만 그러면 "이 슬롯을 누가 맡는가"라는 같은 사실이 `shard-routing.shards`(api가 읽는 표)와 `account-worker.owned-slots`(계좌 워커가 읽는 값) 두 곳에 각자 적힌다. 둘이 어긋나면 api는 슬롯 A를 워커1로 보내는데 워커1은 "내 담당 아님"으로 믿는(또는 반대) 상태가 되고, 에러 없이 조용히 전부 거부되는 — 원인 찾기 가장 어려운 모양이 된다.
- **어떻게(대안·결정·트레이드오프)**: 계좌 워커도 같은 `shard-routing.*` 설정(칸 수+범위별 목적지)을 그대로 읽는다. 그리고 자기 인테이크 채널(`transport.account-intake.channel`, 이미 갖고 있던 값 — api가 이 값을 목적지로 보낸다)과 endpoint가 일치하는 범위를 표에서 찾아 "그게 내 담당"으로 삼는다. 일치하는 범위가 없으면(설정이 어긋난 상태) 기동을 막는다(fail-fast) — 모든 주문을 거부하며 뜨는 것보다 안 뜨는 게 낫다.
- **무엇을(실제 변경·파일·커밋)**: `account-worker/.../config/ShardRoutingConfig.OwnedShard` — 인테이크 채널과 일치하는 슬롯 범위를 찾아 확정, 없으면 `IllegalStateException`. 기동 로그 2줄(설정 로드 결과, 담당 슬롯). 커밋 `58ae59b`.
- **결과·수치**: `ShardRoutingConfigTest` 3개로 검증(전체 담당 폴백/정상 매칭/불일치 시 기동 실패). fail-fast 테스트는 체크를 잠깐 빼고 실제로 컨텍스트가 정상 기동하는 것(RED, `context started successfully`인데 실패해야 하는 상황)을 확인한 뒤 복구.

### 제목 후보3: "담당 판정을 한 곳으로 모으고, 두 종류 실패로 RED를 나눠 확인한 이야기"
- **context(무슨 상황)**: 계좌 워커는 지금까지 "이 계좌가 내 것인가"를 `accounts.get(accountId) == null`로만 판정했다 — 5군데(매수·매도·매수체결·매도체결·정산 처리)에 흩어져 있었다. "존재하지 않는 계좌"와 "존재하지만 내 담당이 아닌 계좌"가 구분되지 않았다.
- **왜(문제·동기)**: 나중에 HA(대기 프로세스로 이어받기)를 만들 때 이 판정의 근거(지금은 설정에서 읽은 고정 슬롯 범위)가 실시간 표로 바뀐다. 다섯 곳에 흩어져 있으면 그때 다섯 곳을 다 고쳐야 한다. 그리고 "존재하지 않음"과 "담당 아님"을 같은 사유(`ACCOUNT_NOT_FOUND`)로 두면, 라우팅이 잘못됐을 때 그 둘을 구분 못 해 원인을 못 찾는다.
- **어떻게(대안·결정·트레이드오프)**: `AccountEventHandler.isOwned(accountId)`(계산)와 `rejectIfNotOwned(...)`(거부+로그) 한 쌍으로 판정을 모으고, 5곳 모두 이 메서드만 부르게 했다. `RejectReason.NOT_OWNED`를 신설해 `ACCOUNT_NOT_FOUND`(담당 슬롯인데 메모리에 없음)와 분리했다. 기존 생성자(사용처 다수)는 전부 "모든 계좌 담당"(슬롯 1개 표) 기본값으로 델리게이트해 하위 호환을 지켰다 — 단 이 기본값 경로를 프로덕션 배선에 잘못 연결하면 "에러 없이 워커마다 전체 계좌를 중복 처리"하게 되므로 javadoc으로 명시적으로 경고했고, 실제 프로덕션 배선(`AccountEngineConfig`)이 이 경고 대상이 아니라 진짜 shardRoutingTable을 받는 생성자를 타는지 코드로 확인했다.

  **RED를 두 단계로 나눈 이유(이 유닛의 핵심 교훈)**: 1차 테스트는 "담당 아닌 계좌"를 시드하지 않은 채로 두고 돌렸다. 판정을 빼도 그 계좌는 애초에 메모리에 없어 "계좌 없음"으로 거부됐다 — **사유만** `ACCOUNT_NOT_FOUND`로 바뀌었을 뿐, 주문 자체는 여전히 거부됐다. 이는 "사유 두 개를 분리했다"만 증명하지, "담당 판정이 실제로 막는다"는 증명이 아니었다(조정 세션 리뷰가 이 약점을 지적). 계좌 워커 둘이 같은 시드 목록 100개를 공유하는 지금 설계에서, 담당 아닌 계좌가 메모리에 없는 건 오직 시드 단계의 필터(`AccountEngineConfig`의 `engine.owns()` 체크) 하나에 의존한다 — 그 필터에 버그가 생기거나 나중에 다른 경로로 시드하게 되면, 런타임 판정이 없는 한 담당 아닌 계좌가 그냥 접수돼 두 워커가 같은 계좌를 각자 고치는(잔고가 갈라지는) 상황이 재현 안 된 채 넘어갈 뻔했다. 2차 테스트는 시드 필터를 일부러 우회해(엔진에 직접 `seed()`를 불러 담당 아닌 계좌를 메모리에 강제로 올림) 판정을 빼면 그 주문이 **그냥 accept**되는 것(`expected: <false> but was: <true>`)을 확인해, 런타임 판정이 시드 필터와 독립적으로 동작함을 증명했다.
- **무엇을(실제 변경·파일·커밋)**: `AccountEventHandler.isOwned`·`rejectIfNotOwned`, `RejectReason.NOT_OWNED`, `AccountEngine.owns()`. 테스트 `AccountEngineTest` 2개(담당_슬롯이_아니면_NOT_OWNED로_거부, 시드_필터_우회해도_담당_아니면_거부). 커밋 `58ae59b`.
- **결과·수치**: RED ①`expected: <NOT_OWNED> but was: <ACCOUNT_NOT_FOUND>` ②`expected: <false> but was: <true>`. `:account-disruptor:test` 전체(108+4개, --rerun) GREEN.

### 범위 밖으로 명시한 구멍 — 정산 재전달
담당 아닌 계좌의 정산은 이제 `NOT_OWNED`로 거부되지만, 그 정산이 올바른 워커에 다시 전달되지 않는다. 정산 컨슈머의 Kafka 그룹 아이디가 코드에 박혀 있어 워커 둘이 같은 그룹이 되고(같은 그룹 안에서 파티션이 어느 워커로 가는지에 소유 판정이 안 물려 있음), 거부된 정산은 그대로 유실된다. 이번 범위(라우팅·소유 판정)에서 안 닫은 구멍 — 후속 이슈로 트래킹 필요.

## 블로그 네타

### "설계 지시서도 틀릴 수 있다 — core 의존성을 직접 열어본 이야기"
- **훅·핵심 주장**: 조정자의 지시(공유하려면 core로 옮겨라)를 그대로 따르지 않고, 실제로 그 파일(`core/build.gradle.kts`)을 열어 근거를 확인한 뒤 반려했다. 지시가 틀렸을 수 있다는 전제로 코드를 먼저 본다.
- **context**: 계좌를 여러 프로세스로 나누려면 api·매칭·계좌 셋이 같은 라우팅 설정을 봐야 한다. "공유"라는 목적만 보면 공통 모듈(core)로 옮기는 게 자연스러워 보인다.
- **어떻게(서사·근거)**: core가 프레임워크 없는 순수 라이브러리로 남아야 한다는 원칙이 이미 여러 자리(javadoc, 이전 결정 기록)에 있었는데, 지시서는 그 사실을 확인 안 하고 "공유하려면 core"라고 단정했다. 직접 파일을 열어 Spring 의존성이 0인 것과, 세 모듈이 공통으로 의존하는 게 core뿐이라 딜레마가 진짜라는 것까지 확인한 뒤 대안(각 모듈에 얇게 분산)을 제시했다. 조정자가 확인 후 지시서 오류를 인정하고 방향을 바로잡았다.
- **재료(커밋·도식·수치)**: 커밋 `18147e3`·`58ae59b`. RED 2단계 실패 메시지(`ACCOUNT_NOT_FOUND`로 사유만 바뀐 첫 실패, `accepted=true`로 완전히 뚫린 두 번째 실패) — 테스트 하나를 강화하는 것만으로 증명 범위가 얼마나 달라지는지 보여주는 좋은 대조.
