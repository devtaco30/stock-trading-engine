---
feature: matching-order-intake-durability
date: 2026-09-16
branch: feat/matching-order-intake-durability
commits: [4dba699, 547d95e]
feeds: [adr, blog]
---

# 매칭 주문 인테이크 리플레이 (I2 U2b) — 반영 전에 죽어도 되살리기, 그리고 증명하려다 두 번 틀린 이야기

매칭 엔진이 주문을 받아 링버퍼(매칭 코어의 입구)에 넣기 전에 죽으면 그 주문을 되살릴 원본이 없던 문제를 닫는 작업이다. 앞 유닛(U1, ec)이 녹화와 위치 기록을 해뒀고, 이 작업(U2b)은 그 녹화를 실제로 다시 읽어 엔진에 먹이는 자리와, 그게 실제로 복구를 닫는다는 증명(I3)을 맡았다.

## ADR 네타

### ① MatchingOrderIntakeReplayer — AccountFillReplayer를 미러한 이유와 2007을 새로 잡은 이유

- **context**: 매칭은 주문을 Aeron(프로세스 사이 메시지를 저지연으로 나르는 전송 라이브러리)으로 받아 링버퍼에 넣고, 소비자 스레드가 꺼내 호가창에 반영하면서 자기 저널(Aeron Archive에 순서대로 기록되는 로그)에 적는다. U1이 이 인테이크 스트림(2002)을 Aeron Archive에 REMOTE(드라이버 레벨) 녹화하도록 이미 배선해뒀고, 발행자(Aeron sessionId, 계좌 샤드마다 다르다)별로 "어디까지 반영했나"를 `MatchingEventHandler`가 기억하도록도 해뒀다. 남은 건 재기동 때 그 녹화를 실제로 읽어 엔진에 넣는 코드뿐이었다 — U1 시점엔 그 자리가 `List.of()`(빈 리스트)로 비어 있었다.
- **왜**: 이 스트림은 recording(Aeron Archive가 연결 하나마다 따로 만드는 기록 단위)이 여럿 생길 수 있다 — 계좌 프로세스가 여럿이거나 계좌가 재시작할 때마다 새 세션으로 다시 붙기 때문이다. 그래서 저널 리플레이(`MatchingJournalReplayer`, 시작 시각순으로 이어 붙이면 되는 단일 흐름)와 똑같이 만들 수 없다 — 스냅샷 시점에 recording 여러 개가 동시에 "현재"일 수 있어 "먼저 것/나중 것"으로 나눌 기준이 없다. account-worker의 `AccountFillReplayer`가 이미 같은 문제(체결 스트림)를 recording마다 자기 sessionId로 위치를 찾아 그 위치부터 읽는 방식으로 풀어뒀다 — 이번 작업은 그 구조를 주문 인테이크에 그대로 적용하는 것이다.
- **어떻게**: `AccountFillReplayer`의 `readFrom`+`listRecordings`+`replayOne`+`awaitConnected` 구조를 그대로 가져왔다. `AccountFillReplayer.listRecordings`가 private이라 재사용은 못 해 새 클래스에 다시 썼다(I5 인수인계가 미리 짚어둔 지점). recording마다 자기 sessionId로 위치 맵을 찾아 그 위치부터 읽고, 맵에 없으면(스냅샷 이후 새로 붙은 계좌 샤드라는 뜻) 그 recording의 시작 위치부터 읽는다. replay 목적지 스트림(리더가 Archive에 재생을 요청할 때 여는 임시 스트림)은 **2007**로 새로 잡았다 — 계좌 체결 리플레이(6002), 매칭 저널 리플레이(2006)와 겹치면 같은 채널을 다른 리더가 동시에 열게 되어 안 된다.
  - `matchingOrderIntakeReplayedEntries` 빈은 `AeronArchive`(Archive 클라이언트)와 `Optional<StoredMatchingSnapshot>`을 받아, 스냅샷이 있으면 `orderIntakePosition()`(발행자별 위치 맵)을, 없으면 빈 맵을 replayer에 넘긴다. 빈 맵이면 replayer는 "맵에 없는 발행자"로 취급해 모든 recording을 시작 위치부터 통째로 읽는다 — 스냅샷이 없는 첫 기동에서의 하위호환 경로다.
- **무엇을**: `matching-worker/.../recovery/MatchingOrderIntakeReplayer`(신설). `MatchingOrderIntakeConfig.matchingOrderIntakeReplayedEntries` 빈 본문 교체 + WARN 로그 제거. 커밋 `4dba699`.
- **결과·수치**: `MatchingOrderIntakeReplayerPositionIntegrationTest`(신규, Aeron 통합) — RED: position 맵을 무시하도록 임시로 되돌려 재실행 → `Expecting actual: [1L, 2L, 3L] to contain exactly (and in same order): [2L, 3L] but some elements were not expected: [1L]`. 원복 후 GREEN.

### ② 이중 적용 검증을 순수 JVM 테스트로 먼저 끊은 이유, 그리고 첫 assertion이 틀렸던 경위

- **context**: 매칭 재기동 복구는 저널 리플레이(`matchingJournalRecoveredEntries`)와 인테이크 리플레이(`matchingOrderIntakeReplayedEntries`)를 순서대로 `engine.recover(...)` 두 번으로 재적용한다. 인테이크 위치는 "매칭 소비자가 이 주문을 처리하기 시작한 시점"에 기록되는데, 같은 프로세스 안에서 저널 기록(`JournalEventHandler`)이 매칭 반영(`MatchingEventHandler`)보다 먼저 도는 파이프라인이라도, 크래시 타이밍에 따라 "저널엔 이미 있는데 인테이크 위치 맵은 아직 못 따라간" 주문이 생길 수 있다 — 그러면 저널 리플레이와 인테이크 리플레이 양쪽에서 같은 주문이 다시 반영될 여지가 생긴다(LLD가 착수 전에 직접 확인하라고 짚어둔 지점).
- **왜**: 이걸 Aeron을 띄우는 무거운 통합테스트로만 확인하면 원인 규명이 느리다. `MatchingEngine.recover(Iterable<JournaledOrder>)`가 실제로 쓰는 것과 같은 경로(`OrderBook.containsOrder`의 기존 멱등 체크)를 pure-JVM 레벨에서 먼저 증명해두면 빠르고, 실패해도 스택트레이스가 짧다.
- **처음에 틀린 것**: `recover(journalEntries)` → `recover(intakeEntries=[겹치는 주문, 새 상계 주문])` → `recovered.start()` 뒤에 `assertFalse(containsOrder(orderId))`로 "이중 반영이면 상계가 덜 돼 잔량이 남을 것"을 확인하려 했다. 실행하니 FAILED — `containsOrder`가 최근 **전량체결도 true**로 치는 멱등 캐시(`OrderBook.java:106`, at-least-once 환경에서 같은 주문 중복 도착을 막기 위한 것)라는 걸 그제야 알았다. 단일 반영이든 이중 반영이든 상계 직후엔 항상 true라 이 assertion으로는 아무것도 구분하지 못했다 — 구현 버그가 아니라 관측 신호가 틀린 것.
- **고친 방법**: recover 뒤 라이브로 전환해, 원래 겹치는 주문(BUY 10)과 정확히 상계되는 매도 주문(SELL 1)을 새로 넣어 "체결 리스너가 불리는지"로 관측했다 — 이중 반영이면 상계되고도 잔량이 남아 이 매도가 체결돼야 하고, 단일 반영이면 매칭될 잔량이 없어 체결이 안 나야 한다. 이 기법은 기존 `recover_중엔_리스너가_안_불린다` 테스트가 이미 쓰던 것과 같다.
- **무엇을**: `matching-disruptor:MatchingEngineRecoveryTest`에 테스트 1건 추가. 커밋 `4dba699`.
- **결과·수치**: PASSED(기존 4건도 회귀 없음) — 별도 프로덕션 코드 변경 없이, `OrderBook.containsOrder`의 기존 멱등 체크가 이 usage 패턴에서도 지켜짐을 확인한 characterization test다.

### ③ I3 증명 — 폴링 스레드를 먼저 죽여 "반영 전에 죽는다"를 결정적으로 만든 방법

- **context**: U2b가 실제로 복구를 닫는지 증명해야 한다 — 주문을 매칭에 보내되 링버퍼에 반영되기 **전에** 죽는 상황을 만들고, 재기동 뒤 그 주문이 호가창에 살아 있는지 봐야 한다.
- **왜**: "먼저 도착하면 반영될 수도 있는" 타이밍 경합으로 짜면 flaky해진다. 이 스트림을 소비해 링버퍼에 넣는 경로는 `AeronOrderReceiver`의 폴링 스레드 하나뿐이고, Archive REMOTE 녹화는 Aeron 드라이버 레벨에서 이미지(연결의 로그 버퍼)에 직접 붙어 그 폴링 스레드와 무관하게 기록한다 — 그래서 그 폴링 스레드를 먼저 영구히 멈추면, 그 뒤 발행한 주문은 녹화엔 반드시 남지만 링버퍼엔 절대 들어가지 못한다. 경합이 아니라 항상 같은 결과가 나온다.
- **어떻게**: run1에서 컨텍스트가 뜨자마자 `AeronOrderReceiver.close()`로 폴링 스레드를 멈춘다. 그 뒤 발행한 주문의 녹화 position이 실제로 느는 걸 확인해(`archive.getRecordingPosition` 폴링) "Archive엔 durable하게 남았다"를 증명하고, `engine.containsOrder`가 false임을 확인해(폴링 스레드가 죽었으니 결정적으로 미반영) run1을 종료한다. run2(새 컨텍스트, 같은 archiveDir)에서 그 주문이 살아있는지 본다.
- **첫 번째로 틀렸던 설계 — 정상 종료 스냅샷 스위치를 껐던 것**: I6(`matching-periodic-snapshot.md`) 관례를 그대로 따라 `matching.worker.graceful-snapshot-enabled=false`로 run1을 띄웠다. 조정 세션이 코드를 직접 읽고 지적: 이 스위치를 끄면 run1이 스냅샷 파일을 안 남기고, run2의 `matchingOrderIntakeReplayedEntries`가 `Optional<StoredMatchingSnapshot>`으로 빈 값을 받아 `.orElse(Map.of())`로 **빈 맵**이 들어간다 — 빈 맵이면 replayer가 recording을 **처음부터 전부** 읽는다. 즉 그 테스트가 통과하는 건 "저장된 위치부터 이어 읽어서"가 아니라 "처음부터 전부 다시 읽어서"였다 — 이 유닛의 핵심(위치 기준 재생)을 증명하지 못하는 우연한 GREEN이었다. I6와 이 유닛은 기법이 다르다: I6는 정상 반영시킨 뒤 "종료 시점 스냅샷이 그 반영을 이미 담아버리는" 타이밍 경합이 문제였지만, 이 유닛은 애초에 반영 자체가 없어(폴링 스레드를 먼저 죽였으니) 스위치 유무가 RED 성립에 영향을 주지 않는다 — 그래서 운영 기본값(켬)으로 돌려도 안전하고, 오히려 켜야 실제 운영 경로(스냅샷 복원 → 위치 시드 → 그 위치부터 재생)를 탄다.
- **최종 구조**: 두 run 모두 `graceful-snapshot-enabled=true`. run1에서 정상 반영되는 주문(NORMAL_ORDER_ID)을 폴링 스레드가 살아있을 때 먼저 보내(저널에도, 인테이크 녹화에도 남는다) 반영을 확인한 뒤, 폴링 스레드를 죽이고 목표 주문(TARGET_ORDER_ID)을 보낸다. run1 종료 시 스냅샷은 NORMAL_ORDER_ID의 반영 위치까지만 담고(TARGET_ORDER_ID는 반영된 적이 없어 담길 수 없다), run2는 그 스냅샷에서 복원 → 위치 시드 → 그 위치부터 인테이크 재생이라는 실제 경로를 탄다.
- **이중 반영도 같이 잰다**: NORMAL_ORDER_ID는 저널에도 있고 인테이크 녹화에도 있다 — 위치 기준 재생이 틀리면 두 경로 모두에서 다시 반영될 수 있다(②의 pure-JVM 증명을 실제 Aeron 파이프라인으로 다시 검증하는 셈). run2에서 NORMAL_ORDER_ID와 정확히 같은 수량(10)의 매도 주문으로 상계해 체결 수량이 정확히 10인지 확인하고, 이어서 소량(1) 탐지 주문을 하나 더 보내 300ms 안에 추가 체결이 없는지(잔량이 안 남았는지) 확인한다.
- **무엇을**: `matching-worker/.../config/MatchingOrderIntakeRecoveryIntegrationTest`(신규). 커밋 `547d95e`.
- **결과·수치**: RED(리플레이를 `List.of()`로 되돌림, 스위치는 켠 채로) — `[주문 인테이크 리플레이(I2 U2b)로 이 주문이 복구돼야 한다] Expecting value to be true but was false`. 원복 후 GREEN. `:matching-worker:test`·`:matching-disruptor:test` 전체 — U2b 커밋 시 50개, I3 증명 커밋 시 51개, 매번 0 failures.

### ④ 이 브랜치가 닫는 건 I3이지 I2 전체가 아니다

- **context**: 이 작업이 배정될 때 "I2 U2b"로 불렸지만, 원래 I2 항목의 제목은 "계좌가 매칭으로 보내다 실패하면 주문을 버린다. 예약은 남아 돈이 묶인다"(발신측)다.
- **왜**: `decision_records/i234-transport-loss-design.md`(ec)가 이미 I2·I3·I4를 두 축(수신측 내구성=I3, 발신측 재시도=I2, 가용성=I4)으로 나눠뒀는데, 이번 작업은 그중 I3(받는 쪽, 이미 받은 주문을 못 잃는 것)만 닫는다. 발신측(`AeronMatchingOrderSender`의 발신 큐 포화·발행 실패 처리 두 자리)은 이 브랜치가 손대지 않았다.
- **어떻게**: 다음 유닛은 그 발신측이고, 착수 전에 선결정이 필요하다 — 버리지 않으려면 어딘가에서 기다려야 하는데, 계좌의 단일 쓰기 스레드가 기다리면 그 샤드의 모든 계좌가 멈춘다(dc가 매칭 쪽에서 겪은 것과 같은 모양의 트레이드오프). 조정 세션이 코드를 보고 LLD를 먼저 쓰기로 했다.
- **무엇을**: 코드 변경 없음 — 범위 기록.
- **결과·수치**: 해당 없음.

## 블로그 네타

### "assertion이 틀렸다는 걸 실패 메시지로 알아채기 — 멱등 캐시 함정"

- **훅·핵심 주장**: RED를 봤다고 다 같은 RED가 아니다. 테스트가 실패했을 때 "고쳐야 할 건 구현이 아니라 내 관측 신호"라는 걸 실패 메시지에서 바로 읽어내야, 없는 버그를 쫓느라 시간을 안 쓴다.
- **context**: 매칭 재기동 복구가 저널 리플레이와 인테이크 리플레이 두 경로로 같은 주문을 이중 반영할 수 있다는 가능성을 pure-JVM 테스트로 먼저 끊으려 했다. `OrderBook.containsOrder`로 "겹치는 주문이 사라졌는지"를 보면 될 거라 생각했다.
- **어떻게(서사·근거)**: 실행하자 `assertFalse(containsOrder(1L))`가 FAILED — 처음엔 "정말 이중 반영되는 버그인가" 하고 코드를 다시 읽었지만, `containsOrder`의 구현(`OrderBook.java:106`)이 "최근 전량체결도 true"로 치는 멱등 캐시라는 걸 발견했다. 단일 반영이든 이중 반영이든 상계 직후엔 항상 true였던 것 — 애초에 이 신호로는 무엇도 구분할 수 없었다. 라이브 전환 뒤 후속 주문으로 체결 유무를 관측하는 방식으로 바꾸자 정확히 구분됐다. 같은 함정이 I3의 큰 증명(Aeron 통합테스트)에서도 재발할 뻔했다 — 처음엔 정상 종료 스냅샷 스위치를 꺼서(I6 관례를 그대로 따라) 테스트를 짰는데, 조정 세션이 "그러면 위치 맵이 비어 recording을 처음부터 다 읽는 약한 경로로 우연히 통과한다"는 걸 코드로 확인해 잡아줬다 — 이번엔 assertion이 아니라 테스트의 전제 자체가 틀린 경우였다.
- **재료(커밋·도식·수치)**: 커밋 `4dba699`·`547d95e`. RED 메시지 두 개(position 무시 시 `[1L,2L,3L]` vs `[2L,3L]`, 리플레이 `List.of()` 시 `Expecting value to be true but was false`). `OrderBook.java:106`의 멱등 캐시 코드. 전체 회귀 50→51개, 0 failures.
