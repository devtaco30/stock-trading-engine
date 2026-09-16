---
feature: matching-periodic-snapshot
date: 2026-09-16
branch: feat/matching-periodic-snapshot
commits: [b86e45a, e10ffeb, f8a3d8f]
feeds: [adr, blog]
---

# 매칭 엔진 러닝 중 스냅샷 (I6) — 정상 종료 경로를 꺼야 증명되는 이유

매칭 엔진(종목별 호가창을 메모리에 들고 주문을 매칭하는 프로세스)에 계좌 엔진과 같은 주기 스냅샷을 옮긴 작업이다. 세 유닛(U1 트리거+싱크 인터페이스, U2 전용 쓰기 스레드+fsync+호스트 배선, U3 주기 스냅샷만으로 복구 증명)으로 나눠 진행했다.

## ADR 네타

### ① 매칭 엔진에 러닝 중 스냅샷을 옮긴 이유와 구조

- **context**: 매칭 엔진은 저널(Aeron Archive에 순서대로 기록되는 주문 로그)과 스냅샷(그 시점 호가창 전체를 파일로 찍은 것) 둘로 복구한다. 복구 배선(스냅샷 읽기 → 저널 delta 재적용) 자체는 이미 있었다.
- **왜**: 스냅샷을 정상 종료(graceful shutdown) 시점에만 찍고 있었다. 크래시로 죽으면 직전 스냅샷 이후 저널 전체를 다시 적용해야 하고, 켜둔 시간이 길수록 복구 시간이 늘어난다. 계좌 엔진에는 이미 저널 적용 순번(appliedSeq)이 N(=10,000)에 도달할 때마다 상태를 찍는 러닝 중 스냅샷이 있었다 — 그 구조를 매칭에 옮기는 것이 이번 작업이다.
- **어떻게**: 계좌와 같은 3단 구조를 그대로 썼다.
  1. 소비자 스레드(single-writer, `MatchingEventHandler`)가 appliedSeq % N == 0이 될 때마다 books를 직접 직렬화한다(자기 상태를 자기가 만지는 도중이라 다른 스레드가 읽으면 반쪽 값을 볼 수 있어서다).
  2. 직렬화한 바이트를 SPSC 큐에 넣고 즉시 돌아간다(논블로킹). 큐가 가득 차면 그 회차를 건너뛰고 경고만 남긴다.
  3. 전용 쓰기 스레드(`MatchingSnapshotWriter`)가 큐에서 꺼내 파일에 쓰고 fsync한 뒤 "여기까지 디스크에 확정됐다"(durableSeq)를 volatile로 보고한다.

  매칭과 계좌가 다른 지점 하나 — 계좌는 이 확정 지점(durableSeq)을 "체결 번호 목록을 언제 버릴지" 판단에 쓴다(tradeId 세대 가지치기). 매칭에는 버릴 목록이 없다(호가창뿐이다). 그래서 이번엔 그 값을 노출만 해뒀다 — 나중에 매칭 쪽 디스크 회수(저널 recording 정리) 작업이 그 값을 경계로 쓸 것이다.

  구현상 필요했던 것 하나: 매칭의 이벤트 슬롯(`OrderEvent`)에는 계좌(`AccountEvent`)에 있던 저널 위치 필드가 없었다. `JournalEventHandler`(저널 기록 담당, 소비자보다 먼저 도는 핸들러)가 저널에 기록한 직후 그 위치를 슬롯에 실어주게 했다 — 소비자 스레드가 "지금까지 저널에 반영된 위치"를 알아야 스냅샷에 담는데, 그 값을 다른 스레드의 `journal.position()`을 직접 조회해서 얻지 않고 슬롯에서 읽게 한 것이다. 계좌가 같은 이유로 같은 방식을 쓴다.
- **무엇을**: `MatchingSnapshotSink`(인터페이스, matching-disruptor), `MatchingEventHandler`(appliedSeq·트리거·takeSnapshot 추가), `OrderEvent`(journaledPosition 필드), `JournalEventHandler`(기록 직후 위치를 슬롯에 씀), `MatchingSnapshotFactory`(books→스냅샷 변환 로직을 `MatchingEngine.snapshot()`과 공유하도록 추출), `MatchingSnapshotWriter`(matching-worker, 전용 쓰기 스레드), `MatchingSnapshotWriterLifecycle`, `MatchingEngine` 생성자에 싱크 파라미터 추가. 커밋 `b86e45a`(U1)·`e10ffeb`(U2).
- **결과·수치**: U1 RED 확인(트리거를 잠깐 꺼서 "N건째 정확히 1회 offer" 단위 테스트가 실패함을 확인, `expected: 1 but was: 0`). matching-disruptor 전체 24개, matching-worker 전체 24개(Aeron 통합테스트 8개 포함) 전부 회귀 없이 통과.

### ② MatchingSnapshotStore에 fsync를 넣은 이유

- **context**: `MatchingSnapshotStore.write`는 원래 `Files.write`로 임시 파일에 쓴 뒤 `ATOMIC_MOVE`로 이름을 바꾸는 방식이었다. rename 자체는 원자적이지만, 그 내용이 아직 OS 페이지 캐시에만 있고 디스크에는 안 박혔을 수 있다.
- **왜**: 러닝 중 스냅샷 쓰기 스레드가 보고하는 durableSeq("여기까지 디스크에 확정됐다")는 나중에 다른 판단의 근거로 쓰인다 — 매칭 쪽에서는 이번 범위 밖이지만(D3), 앞으로 저널 recording을 지워도 되는 경계로 쓰일 예정이다. fsync 없이 durableSeq를 올리면 "디스크에 확정됐다"는 보고가 실제로는 캐시에만 있는 상태를 가리키는 거짓 보고가 되고, 그 값을 믿고 저널 기록을 지우면 크래시 시 정작 남아있어야 할 기록이 없는 상태가 된다.
- **어떻게**: `FileChannel.open` + `channel.write` + `channel.force(true)`로 바꿨다(계좌 `AccountSnapshotStore`와 같은 방식). 이미 인코딩된 바이트를 받는 오버로드도 추가해, 쓰기 스레드가 소비자 스레드가 이미 만든 바이트를 재인코딩하지 않게 했다. 이 write 메서드는 정상 종료 경로(`MatchingSnapshotLifecycle`)도 그대로 쓰므로, 정상 종료도 그만큼(디스크에 실제로 박힐 때까지 기다리는 만큼) 느려진다 — 이건 의도한 대가다.
- **무엇을**: `MatchingSnapshotStore.write(long, byte[])` 오버로드 추가, 기존 `write(long, MatchingSnapshot)`는 인코딩 후 그 오버로드에 위임. 커밋 `e10ffeb`.
- **결과·수치**: 기존 `MatchingSnapshotStoreTest` 3건 회귀 없이 통과(동작은 그대로, 내부 구현만 fsync 추가).

### ③ graceful-snapshot-enabled 스위치를 만든 이유 (이 작업의 핵심)

- **context**: U3의 목표는 "정상 종료를 거치지 않고, 러닝 중 스냅샷만으로 복구되는지"를 증명하는 것이었다.
- **왜**: 정상 종료(graceful stop) 스냅샷을 켜둔 채로 이 테스트를 짜면 증명이 안 된다는 걸 시도하다 알았다 — 러닝 중 스냅샷 트리거를 꺼도, 프로세스가 정상 종료할 때 `MatchingSnapshotLifecycle.stop()`이 같은 `engine.snapshot()`으로 그 시점의 정확한 최종 상태를 한 번 더 찍어버린다. 그러면 "주기 스냅샷이 도는지 안 도는지"와 무관하게 재기동은 항상 성공한다 — RED를 만들 수가 없다(트리거를 꺼도 테스트가 계속 GREEN으로 남는다). 이 저장소의 다른 통합 테스트는 전부 같은 JVM 안에서 Spring 컨텍스트를 열고 닫는 방식이라, 진짜 크래시(별도 프로세스를 `kill -9`)를 흉내내는 방식은 전례가 없어 무겁다고 판단해 뺐다.
- **어떻게**: `matching.worker.graceful-snapshot-enabled` 프로퍼티(기본값 true)를 추가하고, `MatchingSnapshotLifecycle` 빈에 `@ConditionalOnProperty`를 걸었다. 이 값을 false로 주면 그 빈 자체가 Spring 컨텍스트에 등록되지 않아, `context.close()`를 불러도 종료 시점 스냅샷이 안 찍힌다 — 컨텍스트 자체는 정상적으로 닫히고 다른 리소스(Aeron 포트·파일 락)는 깨끗이 정리되므로, 같은 JVM에서 바로 다음 컨텍스트(재기동)를 띄울 수 있다.
- **⚠️ 운영에서는 절대 끄면 안 된다.** 이 값을 false로 두고 실제로 운영하면, 프로세스가 정상 종료할 때도 그 시점의 최종 스냅샷이 안 남는다 — 다음 기동이 마지막 러닝 중 스냅샷(최대 10,000건 전) 이후 구간을 전부 저널에서 다시 읽어야 해 복구가 그만큼 길어진다. 기본값이 true라 실수로 꺼질 일은 없지만, "종료를 빠르게 하려는" 스위치로 오해되면 안 된다는 점을 프로덕션 코드의 javadoc에도 명시했다.
- **무엇을**: `MatchingSnapshotConfig.matchingSnapshotLifecycle`에 `@ConditionalOnProperty(name = "matching.worker.graceful-snapshot-enabled", havingValue = "true", matchIfMissing = true)`. 테스트 `MatchingPeriodicSnapshotRecoveryIntegrationTest`. 커밋 `f8a3d8f`.
- **결과·수치**: 테스트 시나리오는 run1(graceful=false)에서 실제 주문 1건 + 더미 이벤트 9,999건으로 저널 적용 순번을 정확히 10,000까지 올리고, `MatchingSnapshotWriter.durableSeq()`가 10,000에 도달할 때까지(=fsync 완료) 기다린 뒤 종료 → 스냅샷 파일이 존재하는지 확인(이게 RED를 가르는 지점이다) → run2(graceful=true, 기본값)를 다시 띄워 그 주문이 복원되는지 확인하는 방식이다.

  **정상 GREEN**: PASSED.
  **RED 확인**(주기 트리거를 `if (false && ...)`로 잠깐 무력화): "15초 안에 러닝 중 스냅샷 durableSeq가 10000에 도달하지 않음(현재 0)"으로 정확히 그 지점(run1 단계)에서 실패. run2조차 실행되지 못했다 — 트리거가 없으면 스냅샷 파일 자체가 안 생긴다는 증거다.
  **되돌린 뒤 재확인**: PASSED.
  전체 회귀: matching-worker 24개(Aeron 통합테스트 8개 포함, 기존 graceful 종료 테스트 `MatchingSnapshotRecoveryIntegrationTest` 포함) 전부 통과.
  임시 폴더(`aeron-*`) 개수: U3만 3회 실행(GREEN·RED·GREEN) 210 → 215(+5). 그 뒤 matching-worker 전체 24개 테스트 1회 실행 215 → 232(+17). 아무도 정리하지 않는 문제로 별도 트랙(I10)에 등록됨.

### ④ N=10,000은 계좌 값을 그대로 가져온 임시값

- **context**: 러닝 중 스냅샷을 몇 건마다 찍을지 정하는 N.
- **왜**: 계좌 엔진은 이미 실측(`decision_records/tradeid-snapshot-interval.md`)으로 N=10,000을 확정해뒀다.
- **어떻게**: 매칭에도 같은 값 10,000을 그대로 썼다 — 실측 없이 가져온 임시값이라는 걸 코드 주석에 남겼다. 매칭은 이벤트 성격(주문 유입)이 계좌(주문+체결+정산 혼재)와 달라 최적 주기가 다를 수 있지만, 이번 작업의 목표는 메커니즘을 만드는 것이었고 실측 재산정은 후속 과제로 남긴다.
- **무엇을**: `MatchingEventHandler.SNAPSHOT_INTERVAL_JOURNAL_ENTRIES = 10_000L`.
- **결과·수치**: 미측정(의도적으로 후속으로 미룸).

## 블로그 네타

### "증명할 수 없는 테스트를 증명 가능하게 만들기 — 정상 종료 경로를 꺼야 했던 이유"

- **훅·핵심 주장**: 기능을 다 만들고 나서 "이게 진짜 동작한다"는 걸 증명하려는 순간, 증명 자체가 불가능하다는 걸 발견할 때가 있다. 이번엔 "주기 스냅샷만으로 복구되는가"를 시험하려는데, 정상 종료가 매번 더 확실한 최종 스냅샷을 대신 찍어버려서 시험 대상(주기 스냅샷)이 있으나 없으나 결과가 같아지는 상황이었다.
- **context**: 매칭 엔진이 크래시 시 저널 전체를 재적용해야 하는 문제를 풀려고 계좌 엔진처럼 주기 스냅샷을 옮겼다. 트리거·전용 쓰기 스레드·fsync까지는 순조로웠다.
- **어떻게(서사·근거)**: 마지막 유닛에서 "정상 종료 없이 재기동해도 복원되는지" 테스트를 짜다가, 정상 종료 스냅샷이 켜져 있으면 트리거를 꺼도 테스트가 계속 통과한다는 걸 발견했다 — 같은 `engine.snapshot()` 호출이 종료 시점에 한 번 더 도니까. 진짜 크래시(별도 프로세스를 강제 종료)를 시도하는 대신, 종료 시점 스냅샷을 만드는 빈 하나를 프로퍼티로 뺄 수 있게 만들어 문제를 풀었다. RED 확인에서 "15초 타임아웃, durableSeq 0"이라는 정확한 실패 지점이 나왔을 때 이 스위치가 필요했다는 게 증명됐다.
- **재료(커밋·도식·수치)**: 커밋 `b86e45a`·`e10ffeb`·`f8a3d8f`. RED 수치(기대 10000, 실제 0, 15초 타임아웃). `matching.worker.graceful-snapshot-enabled` 프로퍼티와 `@ConditionalOnProperty` 코드. 회귀 24개 전부 통과, 임시 폴더 증가 관측치(+5, +17).
