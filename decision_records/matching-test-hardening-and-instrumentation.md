---
feature: matching-test-hardening-and-instrumentation
date: 2026-10-30
branch: feat/stock-slot-hasher
commits: [e37f87e, 82a6d21, da1917d, 3ef5c0e, f430b9b, 6cb1eb2, af492b1, 5db028f, 0672140, 6faaee1, 971a8f4, d7fc5ed, a5d6506, ea95604, d7e7dd7, 36e72b9]
feeds: [adr, blog]
---

# 매칭 샤딩 마무리: 시험이 결함을 잡는지 변이로 확인하고, 계측을 계좌 쪽과 같은 이름으로 맞춘 구간

## ADR 네타
### 맵 읽기를 다시 시작할 때 consumer를 새로 만들고, 재생 중에는 관찰자를 부르지 않는다
- **context(무슨 상황)**: 매칭 pod는 맵 토픽(compacted, 파티션 1개)을 읽어 종목 slot별 담당 주소를 안다. 읽기가 멈추면 낡은 값을 막으려고 조회가 빈 값을 돌려주고, 다시 시작할 때는 같은 consumer로 poll을 이어갔다.
- **왜**: 처음부터 다시 읽으면 key(slot)별 마지막 값은 같지만 compaction 전에는 옛 중간 값이 토픽에 남아 있다. 재생하는 모든 레코드마다 관찰자를 부르면 HandoverCleanupTrigger가 「다른 pod의 평상시 주소 + 내가 보관 중인 slot」이라는 옛 중간 값을 보고 현재 유효한 보관본에 정리를 요청할 수 있다.
- **어떻게(대안·결정·트레이드오프)**: consumer를 Supplier로 받아 재시작 때 닫고(1초 한도) 새로 만든다. 재읽기 동안은 새 배열에만 쓰고 공개하지 않다가 끝 위치까지 따라온 뒤 이전 값과 달라진 slot만 한 번씩 통지한다. 대안(같은 consumer 유지)은 낡은 상태를 못 버리고, 대안(재생 중 통지)은 옛 중간 값으로 정리 요청이 나간다. 대가: 재읽기 동안 targetFor가 빈 값을 주므로 판정이 미뤄진다.
- **무엇을(실제 변경·파일·커밋)**: MatchingSlotMapReader(Supplier 생성자, rebuildConsumer, publishReplayed), MatchingHandoverConfig. 커밋 e37f87e, 시험 f4bb8d1.
- **결과·수치**: 단위 시험 17개(옛 중간 값이 재생돼도 보관본에 정리 요청 0건 포함). 통합 실행에서는 이 경로를 직접 타는 시험이 없다(미확인).

### 판정은 분류한 뒤 읽기가 멈추지 않았는지 다시 확인한다
- **context**: decide()는 맵 읽기가 따라왔다는 답을 받은 뒤 조회로 이동 중 항목을 분류한다.
- **왜**: 답을 받은 직후 읽는 thread가 예외를 만나면 조회가 빈 값을 주고, 빈 값은 「항목 없음 = 첫 배정」으로 읽혀 맵에 이동 중으로 남은 slot을 빈 호가창으로 켠다(호가창 이중 보유 가능). 단위 시험으로 재현(수정 전 실패: 맵에 이 pod의 평상시 값이 올라감).
- **어떻게**: 분류를 먼저 모두 하고 상태를 바꾸기 전에 mapSync.healthy()를 다시 본다. 비정상이면 분류를 버리고 slot을 대기에 둔 채 재판정. 재시작 간격 하한(100ms)을 둬 멈춤과 복구가 재확인 사이에 끝나는 틈을 닫는다.
- **무엇을**: KafkaMatchingShardAssignment.decide, MapCatchUpWaiter.healthy/resetFailures, 3ef5c0e, f430b9b, 6cb1eb2.
- **결과·수치**: DecisionTest 12개 통과. 한계: 배정 이벤트 사이에 읽기가 예외로 한 번 비정상이 됐다 복구되면 판정 쪽 연속 실패 횟수 1~2회가 이월될 수 있다(수정 안 함).

### 정리 뒤 늦게 온 요청을 (응답 주소, 이동 번호)로 중복과 새 요청으로 나눈다
- **context/왜**: 정리된 보관본의 요청은 전부 「늦은 중복」(INFO)으로 세어, 새 담당이 재시작해 다시 보낸 정당한 요청의 실패가 WARNING 카운터에 안 잡혔다.
- **어떻게**: 종목 slot별 마지막 응답 쌍을 기억해 같으면 중복, 다르면 새 요청. 이동 번호는 (nodeId<<53)|counter라 pod 사이 크기 비교가 시간 순서가 아니므로 같은지만 본다. 대가: 서로 다른 요청 둘에 응답한 뒤 첫째의 늦은 중복은 새 요청으로 보인다.
- **무엇을/결과**: HandoverResponder, d7e7dd7. ResponderBatchTest 3, TransportIntegration 18 통과.

## 블로그 네타
### "시험이 통과한다는 것과 시험이 결함을 잡는다는 것은 다르다 — 변이로 확인한 U17"
- **훅·핵심 주장**: 첫 실행에서 U17 시험 4건이 모두 통과했는데, 결함을 일부러 넣은 코드(onPartitionsLost가 회수 처리, 일련번호 비교 삭제)에서 돌려 보니 시험 둘이 단언이 아니라 ConcurrentModificationException과 120초 시간 초과로 끝났다. 통과 확인만으로는 시험 결함이 드러나지 않았다.
- **context**: lost 콜백(그룹에서 빠진 멤버가 뒤늦게 받는 콜백)이 맵에 이동 중 표시를 쓰지 않아야 한다는 규칙과, 포기된 오래된 맵 쓰기가 새 쓰기를 덮지 않아야 한다는 규칙. 둘 다 MockProducer 단위 시험만 있었다.
- **어떻게(서사·근거)**: 실제 Kafka(자체 컨테이너, 시험 잠금 안)에서 ①컨텍스트를 닫으면 64개 slot에 이동 중 표시가 올라옴(U17a) ②쓰기 thread 상태(WAITING)로 한가함을 판정하면 브로커 응답 대기와 구분이 안 돼 mapWritesIdle 접근자를 둠 ③변이를 넣어 시험이 원인이 읽히는 문구로 실패하는지 확인 ④드러난 시험 결함 둘(동시 수정 예외, lost 신호 대기 시간 초과)을 고침.
- **재료**: 커밋 af492b1, 5db028f, 0672140, 6faaee1. 정상 코드에서 35개 통과, 변이에서 M1 1개·M2 3개 실패(결정적 변형: 「기록=[m1, m2, m1;moving] … 기대 m2, 실제 m1;moving」).

### "계좌 수를 바꿨더니 접수 속도가 바뀌었다 — 병목은 계좌 엔진 thread 안의 장부 훑기였다"
- **훅·핵심 주장**: 같은 60,000건을 계좌 1개에 쌓으면 초당 약 3천 건, 300개로 나누면 약 3.9만 건. 매도 주문마다 그 계좌의 예약 장부를 통째로 훑는 비용이 계좌당 미체결 수에 비례한다.
- **context**: 주문 처리 한계 시험의 부하 모양(계좌 수와 계좌당 미체결 수)을 Jack이 정하려면 근거가 필요했다.
- **어떻게**: 총 주문 수 고정, 계좌 수 N만 1·10·30·300·3000으로 바꾸고 3회씩 번갈아 측정(매칭 pod 1·계좌 shard 1, 자체 Kafka). 초 단위로 읽다 시작·끝 부분 초 때문에 짧은 실행이 낮게 나오는 편향을 발견해, 주문별 접수 로그의 밀리초 시각으로 바꿨다. 첫 N=3000 시도는 빈 배열이 macOS bash 3.2의 set -u에서 오류가 나 계좌 워커가 안 떠 무효(재실행).
- **재료/결과(실측)**: 밀리초 평균 초당 3,091 / 9,318 / 20,555 / 39,348 / 27,603(N=1/10/30/300/3000), 15회 모두 거부 0. N=300 이후 N=3000이 낮은 원인은 확인하지 못함. 원자료 `stock-trading-engine-s0/loadtest/s0/results-20261030/`.

## 메모
- 계측 이름·단위를 계좌 쪽과 맞춤: offerBackPressureCount(BACK_PRESSURED만)/offerOtherRetryCount, ringFullWaitCount(tryNext 실패 호출 수), 보고기 줄 `matching-engine ringRemaining … fillQueueDepth … fullyFilledOrders … ordersReceived … ordersNotOwned … fills …`(matching-worker.metrics.enabled일 때만). 값이 부하에서 변하는 것은 아직 보지 않았다(미측정).
