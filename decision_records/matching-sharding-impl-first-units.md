---
feature: matching-sharding-impl-first-units
date: 2026-10-29
branch: feat/stock-slot-hasher
commits: [a9e6262, 33ef83f, 7cfbf88, c8da453, d7d2f6b, 18bccbf, d46a5e1, c6476e8, 69fdd2d, 879c7d7, be7ef24, 0e43d13]
feeds: [adr, blog]
---

# 매칭 샤딩 구현 첫 묶음(U1~U7, U5b, U9d 매칭 쪽)에서 설계서와 갈라진 선택

설계서(`docs/_lld_matching_sharding.md`)가 정한 단위를 순서대로 구현하면서, 설계서에 없거나 코드를 읽고 달리 한 선택과 그 이유를 남긴다. 설계 결정 자체(거부 회신, 이동 순서, 종목 slot 개수 64)는 `matching-slot-handoff-stray-orders.md`에 있다. 이 기록은 구현 쪽 선택이다. 같은 주제의 기존 기록은 없었다(decision_records 목록과 origin/main을 확인).

## ADR 네타

### 거부한 주문의 위치를 알리는 이벤트를 새 EventType이 아니라 링버퍼 슬롯의 불리언 필드로 만든다
- **context(무슨 상황)**: 매칭 shard가 담당 아닌 주문을 접수하지 않으면 그 주문은 링버퍼를 지나지 않는다. 스냅샷에 적히는 발행자별 반영 위치(`lastAppliedOrderIntakePositions`)는 호가창에 반영한 주문의 위치뿐이라, 거부한 주문의 위치가 앞으로 나아가지 않는다. 재시작하면 녹화에서 그 주문이 다시 읽힌다(LLD 11-4절 3항).
- **왜(문제·동기)**: 거부한 주문의 발행자 sessionId와 position만 담은 이벤트를 링버퍼에 넣어 엔진 thread가 위치만 갱신하게 해야 한다. 이 이벤트를 어떤 모양으로 만들지가 문제다.
- **어떻게(대안·결정·트레이드오프)**: 대안 가는 `EventType`에 세 번째 값을 더하는 것이다. `EventType`은 core 코덱이고 `OrderCodec.decode`가 `EventType.values()[바이트]`로 읽으므로, 값을 더하면 저널·녹화를 읽는 쪽이 받아들이는 바이트 범위가 넓어진다. 손상된 type 바이트를 걸러내는 기존 시험의 전제가 바뀔 수 있다고 보고 피했다(그 시험이 쓰는 손상 값은 열어 보지 않았다 [미확인]). 대안 나는 `OrderEvent`(matching-disruptor 안의 링버퍼 슬롯)에 `positionOnly` 불리언을 두는 것이다. 이 슬롯은 외부로 나가는 형식이 아니라 코덱에 영향이 없다. 나를 택했다. 대가는 슬롯의 필드가 하나 늘고, `JournalEventHandler`와 `MatchingEventHandler`가 이 플래그를 따로 분기해야 한다는 점이다. `JournalEventHandler`는 저널에 기록하지 않고 저널 위치만 채운다. 위치를 채우지 않으면 `MatchingEventHandler`가 읽는 저널 위치가 0으로 덮인다. `MatchingEventHandler`는 호가창과 스냅샷 주기 계산(`appliedSeq`)을 건드리지 않는다.
- **무엇을(실제 변경·파일·커밋)**: `OrderEvent.setPositionOnly`, `MatchingEngine.publishPositionOnly`, `JournalEventHandler`, `MatchingEventHandler`, `AeronOrderReceiver`(거부할 때 호출). 커밋 879c7d7.
- **결과·수치**: `MatchingEnginePositionOnlyTest` 3개(위치 반영, 호가창 불변, 저널 미기록)와 `AeronOrderReceiverOwnershipTest` 1개 통과. LLD 11-4절의 시험 중 「slot 넘김」 이벤트가 필요한 1·2번과 재시작을 다시 만드는 3번은 만들지 못했다.

### 거부 회신 발신 통로는 FillOutbox를 일반화하지 않고 별도 클래스(RejectReplyPublisher)로 만든다
- **context(무슨 상황)**: 담당 아닌 주문을 계좌번호로 정해진 계좌 shard에 되돌려야 한다(LLD 4-5절, 11-2·11-3절). 체결을 보내는 `FillOutbox`는 package-private이고 `FilledTrade`·`FillCodec`에 묶여 있다.
- **왜(문제·동기)**: 인코더를 분리해 일반화하면 체결 경로(돈이 걸린 경로)를 건드리는 수정이 된다. 거부 회신은 형식이 다르고(기존 `OrderCodec` PLACE 메시지 그대로, 사유 바이트 없음) 경로의 성격도 다르다. 목적지를 모를 때 대기 목록에 두는 규칙이 있어 체결 outbox와 다르다.
- **어떻게(대안·결정·트레이드오프)**: `FillOutbox`는 endpoint마다 queue와 thread를 하나씩 두지만 `RejectReplyPublisher`는 queue 하나와 발신 thread 하나로 모든 계좌 shard 주소를 처리한다. 거부는 드문 경로라 주소별로 나눌 이유가 없다고 봤다 [측정하지 않은 판단]. 대가는 한 계좌 shard의 Publication이 막혀도 같은 대기 목록 안에서 다른 주소의 주문과 순서를 공유한다는 것이다(대기 목록을 앞에서 끝까지 돌며 보내므로 막힌 주소의 주문이 다른 주소의 주문을 막지는 않는다). 대기 목록은 크기 제한이 없다(설계서가 정하지 않음). offer 결과가 CLOSED·MAX_POSITION_EXCEEDED이면 체결 outbox처럼 매칭을 멈추지 않고 ERROR 로그를 남기고 대기 목록에 둔다.
- **무엇을(실제 변경·파일·커밋)**: `matching-worker/.../messaging/RejectReplyPublisher`, `config/MatchingRejectReplyConfig`, `config/RejectReplyPublications`, `MatchingOrderIntakeConfig`(receiver에 담당 판정·`StockSlotHasher`·발신 통로 연결). Publication은 체결용 주소 풀(`MatchingFillPublications`의 주소)마다 6003(`AeronStreamIds.REJECT_REPLY`)으로 하나씩 더 연다. Archive 녹화는 걸지 않는다. 커밋 be7ef24, c6476e8(번호 선언).
- **결과·수치**: `RejectReplyPublisherTest` 5개 통과(6003으로 한 건 나가고 6001에는 안 감, 목적지를 모를 때 보관 후 순서대로 전송, 수신자 연결 전 보관, queue 가득 참 계측, 코덱 왕복). 계좌 shard 쪽 수신과 end-to-end는 [미확인]. 거부 건수·대기 목록 크기는 [미측정].

### 서버 번호가 겹치면 계좌 워커가 서로 다른 체결을 중복으로 무시한다 — 조정 모드에서는 신원 이름을 필수로 한다
- **context(무슨 상황)**: 매칭 shard가 여럿이면 tradeId(Snowflake: 시각 << 22, nodeId << 12, 순번)에 들어가는 서버 번호(nodeId)가 shard마다 달라야 한다. 설계서 5-3절은 신원 이름(`matching-worker.instance-id`, Kafka `group.instance.id`와 같은 값) 끝 순번에 6을 더하고, 기본값 `snowflake.node-id: 4`를 지우고, 끝 순번을 읽지 못하면 기동을 막는다고 했다.
- **왜(문제·동기)**: 두 매칭 shard가 같은 서버 번호를 쓰면 같은 밀리초에 낸 체결의 번호가 같아질 수 있다. 계좌 워커의 `isDuplicateTradeId`는 서로 다른 체결을 중복으로 보고 에러 없이 무시한다(f2 지적). 돈이 걸린 문제다.
- **어떻게(대안·결정·트레이드오프)**: 설계서 그대로 하면 기본값을 지운 뒤 신원 이름 없이 뜨는 기존 시험과 단일 워커 실행이 전부 기동 실패한다. 1차 구현은 신원 이름이 비어 있으면 예전 방식(`snowflake.node-id` 설정, 없으면 호스트명 끝 숫자)으로 정했다. 이 예외가 매칭 shard를 여럿 띄우는 모드까지 열어 두는 것이 문제여서(f2 지적), `matching-shard.coordination.enabled=true`(조정 모드)인데 신원 이름이 비어 있으면 서버 번호를 정하지 않고 기동을 막도록 보완했다. 조정 모드가 꺼져 있을 때만 예전 방식을 허용한다. 신원 이름이 있으면 끝 순번 + 6이고, 끝 순번을 읽지 못하거나 결과가 1023을 넘으면 기동을 막는다. 대가: 조정 모드에서는 신원 이름 설정이 사실상 필수다. 조정 모드가 꺼진 채 매칭 shard 둘을 신원 이름 없이 띄우면 여전히 겹칠 수 있다(막지 않음).
- **무엇을(실제 변경·파일·커밋)**: `SnowflakeConfig`(bean 메서드 인자 셋), `application.yml`의 node-id 삭제. 커밋 69fdd2d, 0e43d13.
- **결과·수치**: `MatchingNodeIdTest` 8개 통과(`matching-0`→6, `matching-3`→9, `matching-7`→13, 조정 모드 켜짐+신원 없음→기동 실패 등).

### 담당 판정과 설계서 호출 모양을 정적 호출이 아니라 인스턴스 주입으로 한다
- **context(무슨 상황)**: 설계서 초안은 `StockSlotHasher.slotFor(stockCode)`를 정적 호출로 적었다.
- **왜(문제·동기)**: 종목 slot 개수가 1 미만이면 예외여야 하는 시험(U1의 4번)이 개수를 받아야 하고, 계좌 쪽 `SlotHasher`가 이미 인스턴스 방식이다.
- **어떻게(대안·결정·트레이드오프)**: `new StockSlotHasher(64).slotFor(code)`로 했고 내부에 `SlotHasher`를 두어 `(long) code.hashCode()`를 넘긴다. `fmix64`를 복사하지도, `SlotHasher`를 수정하지도 않았다. f2가 승인했고 설계서는 26이 인스턴스 표기로 고쳤다.
- **무엇을(실제 변경·파일·커밋)**: `core/.../aeron/StockSlotHasher`, 커밋 a9e6262. 이후 `ownedSlots.test(...)`는 인스턴스를 주입받는다(U4, U5, U6).
- **결과·수치**: 000001~003000 6자리 숫자 3,000개를 64개에 넣은 분포는 최대 63, 최소 32, 평균 46.88, 최대/평균 1.34배(기준 2.5배, 커밋 33ef83f). 연속 숫자라 실제 상장 종목코드의 분포는 [미확인].

### 시험이 서로 방해하던 문제를 파일 잠금으로 직렬화한다
- **context(무슨 상황)**: matching-worker 통합 시험(Aeron Archive를 쓰는 시험들)이 `recording exists for streamId=2005 channel=aeron:ipc`로 실행마다 다른 개수만큼 실패했다. 변경 없는 clean origin/main에서도 같은 시험들이 실패했다(제 변경이 원인이 아님을 이 비교로 확인).
- **왜(문제·동기)**: ec가 같은 시간에 account-worker 시험을 돌리고 있었고, 여러 세션이 한 머신에서 임베디드 Aeron·Kafka 시험을 동시에 돌린다.
- **어떻게(대안·결정·트레이드오프)**: f2의 가설(기본 Aeron 디렉터리를 공유하는 임베디드 드라이버 충돌, 원인으로 확정된 것은 아님)에 따라 f2가 만든 잠금 스크립트(`/tmp/ste-test.lock`에 파일 잠금을 걸고 그 안에서 명령을 실행)로 감싸 실행했다. 스크립트는 실행 전에 열어 내용을 읽었다. 대가: 시험이 한 번에 하나만 돌아 다른 세션이 기다린다.
- **무엇을(실제 변경·파일·커밋)**: 코드 변경 없음. 실행 규약만 바뀜.
- **결과·수치**: 잠금 안에서 matching-worker 111개, matching-disruptor 42개 전부 통과, 실패 0. 잠금 없이 돌린 이전 실행은 12개(첫 실행) → 3개 → 클래스 수가 매번 달랐다. 원인이 동시 실행이라는 확정은 [미확인](잠금 안에서 실패가 없었다는 것만 확인).

### 설계서와 다르게 한 나머지 점
- U3: 시험 브로커 주소를 환경변수 `KAFKA_BOOTSTRAP`(기본 localhost:9092)로 바꿀 수 있게 했다. 로컬 9092에 Kafka가 없어 제 전용 컨테이너(포트 19092)를 띄워 썼고 끝나고 지웠다. 생성자에 토픽 이름 오버로드를 추가해 시험마다 토픽·그룹 이름을 분리했다. 협력 방식 배정(`CooperativeStickyAssignor`) 명시는 컨슈머를 주입받는 구조라 정적 메서드 `consumerProperties(...)`에 넣었다(U4 배선이 이 메서드를 쓴다). 8번 시험이 실제로 협력 방식을 가르는지는 지정을 임시로 빼고 돌려 실패하는 것을 확인했다.
- U5: 기존 `AeronOrderReceiver` 생성자 둘은 그대로 두고(모든 주문을 담당으로 봄) 담당 판정·`StockSlotHasher`·`NotOwnedOrderListener`를 받는 생성자를 추가했다. 이후 `MatchingOrderIntakeConfig`에 연결했다(스위치 꺼짐이 기본이라 동작 불변).
- U6: 걸러내는 정적 메서드(`MatchingOrderIntakeConfig.filterOwnedEntries`)와 시험까지만 했다. `matchingOrderIntakeReplayedEntries` 빈에 연결하지 않았다. Kafka의 첫 배정은 재시작 직후 비어 있어 연결하면 전부 걸러지고, 디스크에서 복원한 목록을 빈에 공급하는 부분은 「slot 받음·넘김」 저널 이벤트가 생기는 U13 이후에 가능하다. f2가 연기를 승인했고, 스냅샷이 없는 기동의 복원 목록은 설계서 11-12절에 채택안이 들어갔다.
- U6b: 스냅샷 파일 버전을 2로 올리고 맡은 종목 slot 목록을 앞머리에 추가했다. 버전 1 파일은 `MatchingSnapshotFormatException`으로 시작을 중단한다. 쓰기 쪽 목록은 `MatchingSnapshotStore` 생성자에 공급자(Supplier)를 받게 해 기존 `write` 시그니처를 유지했다. 앱에서는 담당 판정 빈으로 만든 집합(스위치 꺼짐이면 64개 전부)을 쓴다. 복원 함수 `OwnedStockSlotRestorer`는 「slot 넘김」 이벤트가 아직 없어 넘긴 종목 slot 번호 목록을 받는 순수 함수다.
- U4: 설계서에 키 이름이 없어 `matching-shard.stock-slot-count`(기본 64)와 `matching-shard.coordination.session-timeout-ms`(기본 120000, 계좌 쪽과 같음)를 새로 정했다.

## 블로그 네타

### "시험이 어제는 실패하고 오늘은 통과하는 이유 — 같은 머신에서 세션 여럿이 Aeron 시험을 돌릴 때"
- **훅·핵심 주장**: 코드를 바꾸지 않았는데 시험 실패 개수가 실행마다 달랐다. clean origin/main에서도 같은 시험이 실패한다는 비교로 내 변경이 원인이 아님을 가렸고, 같은 시간에 다른 세션이 시험을 돌린다는 가설을 파일 잠금으로 검증했다.
- **context**: 여러 Claude 세션이 한 저장소의 다른 워크트리에서 구현·검토·측정을 동시에 한다. 임베디드 Aeron 미디어 드라이버와 Archive를 쓰는 시험이 많다.
- **어떻게(서사·근거)**: 처음 matching-worker 전체를 돌렸을 때 12개 실패. 원인을 보니 `recording exists for streamId=2005`. 기준선(clean origin/main) 워크트리를 만들어 같은 시험을 돌렸고 같이 실패했다. 실행마다 실패하는 클래스가 달랐다. f2가 가설을 주고 잠금 스크립트를 줬다. 스크립트를 실행하기 전에 읽었다(안 읽고 돌리지 않는다). 잠금 안에서는 111개 전부 통과. 단 이것이 원인 확정은 아니고(통과했다는 것까지만), 같은 Kafka 시험은 별개로 localhost:9092 브로커가 필요해 실패한다는 점을 따로 구분했다.
- **재료(커밋·도식·수치)**: 12개 → 3개 → 0개(잠금), matching-worker 111, matching-disruptor 42. 잠금 스크립트는 `/tmp/ste-test.lock` 파일 잠금.

### "서버 번호 하나가 겹치면 체결이 조용히 사라진다"
- **훅·핵심 주장**: tradeId에 들어가는 10비트 서버 번호가 두 매칭 shard에서 같으면, 받는 쪽의 중복 제거가 서로 다른 체결을 에러 없이 버린다.
- **context**: 매칭 shard를 여럿 띄우는 설계에서 설정 기본값 4를 지우고 신원 이름에서 서버 번호를 뽑게 했다.
- **어떻게(서사·근거)**: 설계서대로 하면 신원 이름 없이 뜨는 기존 시험이 모두 깨진다. 그래서 신원 이름이 없으면 옛 방식을 허용했는데, 그 예외가 샤드를 여럿 띄우는 모드까지 열어 둔다는 지적을 받았다. 조정 모드에서만 막는 것으로 좁혔다. 막히는 지점을 시험 셋으로 못 박았다(조정 모드 + 신원 없음 → 기동 실패, 있음 → 정상, 꺼짐 → 기존 동작).
- **재료(커밋·도식·수치)**: 69fdd2d, 0e43d13. tradeId 모양은 `(시각 밀리초 << 22) | (nodeId << 12) | 순번`.

## 이후 달라진 것 (같은 날 두 번째 flush)
- 시험 잠금 스크립트 경로가 `/Users/jack/Study/develop/F-lab/ste-test-lock.sh`로 옮겨졌다. 내용은 같다(`/tmp/ste-test.lock`에 파일 잠금을 걸고 그 안에서 인자 명령 실행). 옛 경로는 사라질 수 있다.
- 보관 목록이 가득 찼을 때의 거부(U9e)는 api의 HTTP 503이 아니라 계좌 엔진의 비동기 REJECTED(새 거부 사유 `MATCHING_HOLDING_FULL`)로 바뀌었다. 이유: api가 503을 내는 경우는 계좌 인테이크로 주문을 넘기지 못할 때뿐이고, 계좌 엔진의 거부는 api가 202를 낸 뒤라 HTTP 응답으로 돌아가는 경로가 없다(ec 확인). 매칭 쪽 구현에는 영향이 없다.
- 설계서 11-12절: 스냅샷이 없는 기동에서 복원 목록은 저널의 「slot 받음」·「slot 넘김」 이벤트만으로 만든다. 이벤트의 저널 항목 모양은 U13에서 정한다. 그래서 위 ADR 네타 중 U6(걸러내기를 복구 빈에 연결하지 않음)의 이유가 더 분명해졌다: 연결에 필요한 복원 목록의 출처가 U13의 저널 이벤트다.
