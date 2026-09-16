---
feature: latency-trigger-failure-modes
date: 2026-09-16
branch: feat/e2e-latency-histogram-hooks (feat/c7-load-harness → main에 흡수됨)
commits: [a770e2e]
feeds: [adr, blog]
---

# 접수 지연 측정 트리거 파이프라인의 실패모드 점검·수정

## ADR 네타

### 왜 "쓰고→지운다" 순서를 지키면서도 삭제 실패를 안전하게 만들었나

- **context(무슨 상황)**: v1/v2 전 과정 측정(`v1-v2-e2e-measurement.md`, `measurement-instrumentation-permanent.md`)의 끝점①(접수·예약) 지연을 `LatencyHistogram`으로 측정하고, 부하 드라이버가 파일 트리거(고정 경로에 파일이 생기면 그 내용=목적지 경로로 스냅샷을 쓰고 트리거 파일을 지움 — 드라이버는 그 삭제를 완료 신호로 폴링)로 워밍업+3패스 경계마다 스냅샷+리셋을 받는 구조를 만든 뒤, 2b가 "통합 전에 실패모드를 직접 재현해서 확인하라"고 지시했다(경계 유실·폴링 주기·삭제 실패·핫패스 예외 전파·연속 4패스 누적, 5가지).
- **왜(문제·동기)**: 이 트리거 메커니즘은 타이밍에 의존하는 새 코드였고, 여기서 샘플이 새면 "숫자는 나오는데 다른 패스 것이 섞인" 형태로 조용히 틀려서 통합 후에 발견하면 원인 규명 비용이 훨씬 크다.
- **어떻게(대안·결정·트레이드오프)**: 5가지 중 2가지에서 실제 버그를 찾았다.
  1. **삭제 실패 시 재처리(가장 심각)**: 최초 구현은 트리거 감지 즉시 `Files.move`(ATOMIC_MOVE)로 다른 이름(`.processing`)에 옮긴 뒤 처리하는 "claim" 방식이었다 — 삭제 실패가 재처리로 이어지는 걸 막으려는 의도였다. 그런데 이 방식은 **드라이버가 기대하는 "트리거 사라짐 = 목적지 파일이 다 써졌다"는 타이밍 계약을 깬다** — move가 목적지 write보다 먼저 일어나 원본 경로가 먼저 사라지기 때문이다. 이걸 직접 코드 리뷰가 아니라 **테스트로 재현해서** 잡았다: 목적지 파일을 읽었더니 빈 파일이었다(Jackson `MismatchedInputException: No content to map`). 그래서 순서는 dc 스펙 그대로 "쓰고 → 지운다"로 되돌리고, 대신 `lastHandledDestinationPath` 필드로 "직전에 처리한 목적지 경로와 같은가"를 기억해, 같은 경로가 다시 보이면(=삭제만 실패해 트리거가 그대로 남아 다음 폴에서 또 보인 상황) 히스토그램 재기록 없이 삭제만 재시도하게 했다. 삭제가 영원히 실패해도 이미 쓴 정상 스냅샷 파일은 다시 안 건드린다.
     - **비블로킹 트레이드오프(39 리뷰)**: `lastHandledDestinationPath`를 삭제 성공 후 비우면(clear) "같은 경로 재사용에도 안전"해지지만, 실제로 clear를 넣어봤더니 **내 검증 테스트 자체가 깨졌다** — "삭제 실패 재현"이 곧 "같은 목적지 경로를 다시 씀"이라 clear를 넣으면 그 재현이 "새 패스"로 오인식됐다. 두 시나리오(삭제 실패 재도착 vs 경로 재사용)는 파일 시스템 신호만으로는 구분 불가능하다는 걸 실측으로 확인하고, clear는 넣지 않았다 — 대신 "패스마다 목적지 경로가 달라야 한다"를 드라이버 쪽 계약으로 두 클래스(`AccountLatencyDumpLifecycle`·`OrderLatencyDumpLifecycle`) javadoc에 명시했다.
  2. **핫패스 예외 전파**: `LatencyHistogram.record()`는 v2에서 계좌 엔진 single-writer 소비자 스레드의 `handleBuy`/`handleSell` 안에서 `listener.onAccepted` 호출 **직전에** 불린다. 여기서 `Recorder.recordValue`가 예상 못한 `RuntimeException`을 던지면 — 이미 `state.tryReserve`로 예약(margin 차감)까지 끝난 주문이 `onAccepted` 통지도, 매칭 발신(`forwardPlace`)도 못 받고 그대로 붕 뜬다(돈 — 클라이언트는 응답을 못 받고, 매칭 엔진은 이 주문의 존재를 영영 모른다). 다른 예외 타입이면 Disruptor의 fail-fast 핸들러가 소비자 스레드 자체를 죽여 그 샤드의 전체 주문 처리가 멎을 수도 있었다. `record()`·`snapshotAndReset()` 둘 다 내부에서 `RuntimeException`을 잡아 로그만 남기고 빈 결과로 대체하도록 방어를 추가했다 — "계측 부가 기능이 본 기능(주문 처리)을 해치면 안 된다"는 원칙을 코드로 못 박은 것.
  - 나머지 3가지(경계 유실·폴링 주기·연속 4패스 누적)는 코드 변경 없이 문제없음을 확인했다: 경계 유실은 `org.HdrHistogram.Recorder`가 `WriterReaderPhaser` 기반으로 "그 순간까지 기록된 것만, 원자적으로" 보장해 라이브러리 차원에서 이미 막혀 있었고(손실·중복 불가능), 폴링 주기(200ms)는 패스 길이(30~50초) 대비 오차 <1%로 무시 가능, 연속 4패스 누적은 워밍업+3패스(5·3·7·2건)를 실제로 연속 트리거해 패스마다 그 구간 표본만 담기는 걸 실측했다.
- **무엇을(실제 변경·파일·커밋)**: `LatencyHistogram.record()`/`snapshotAndReset()`에 try-catch(RuntimeException) 방어 추가, `AccountLatencyDumpLifecycle`/`OrderLatencyDumpLifecycle.handleTrigger`를 "쓰고→지운다" + `lastHandledDestinationPath` 가드로 재작성, 목적지 파일도 `.tmp`에 쓴 뒤 `Files.move`(ATOMIC_MOVE)로 원자적 rename(드라이버가 트리거 삭제 직후 바로 읽어도 일부만 써진 파일을 못 보게). 폴링 루프도 한 회차 예외로 스레드 자체가 안 죽게 감쌌다. 커밋 `a770e2e`(core+account-worker+order-engine 3모듈 동시 수정, 대응 테스트 15개 포함 — `LatencyHistogramTest`에 극단값 무예외 테스트, 양쪽 `*DumpLifecycleTest`에 삭제 실패 재현·빈 트리거·4연속 패스 테스트).
- **결과·수치**: `:core:test :account-disruptor:test :order-engine:test --rerun-tasks` 전부 BUILD SUCCESSFUL(신규 테스트 15개 포함). `:account-worker:test`의 Aeron Archive 통합 테스트는 "recording exists for streamId=4005" 에러로 실패했으나, `git stash`로 이 fix 유무와 무관하게 재현됨을 확인 — 같은 머신에서 여러 세션이 동시에 Aeron Archive 테스트를 돌려 생기는 전역 리소스 경합(2b가 fleet 이슈로 별도 트랙 처리, 이 코드 변경과 무관). 39 리뷰 승인(비블로킹 노트 1건, 위에서 트레이드오프로 기록) 후 커밋, dc 브랜치(`feat/c7-load-harness`)에 병합되어 main까지 도달했다(dc 확인: "전체 빌드 GREEN, 드라이버가 이미 패스마다 다른 목적지 경로를 써서 문제없이 맞음").

## 블로그 네타

### "타이밍에 의존하는 코드는 코드 리뷰로 못 잡는다 — 테스트가 실제로 잡은 버그 두 개"

- **훅·핵심 주장**: 파일 기반 트리거+원자적 이동으로 "더 안전하게" 만들려던 시도가 실은 암묵적 타이밍 계약을 깼다. 고쳤다고 생각한 게 새 버그를 만든 사례 — 그리고 그걸 코드를 눈으로 다시 읽어서가 아니라 **실제로 돌려본 테스트가** 잡았다(목적지 파일을 읽었더니 비어 있었다).
- **context**: 부하 측정을 재기동 없이 한 프로세스에서 워밍업+3패스 연달아 돌리려면, 실행 중에 "지금까지 쌓인 걸 꺼내고 리셋"할 방법이 필요했다 — 파일 트리거를 고른 이유(HTTP 서버 없이도 됨)와 그 선택이 만든 새로운 동시성 문제들.
- **어떻게(서사·근거)**: 2b가 "코드만 읽지 말고 실제로 재현되는 테스트를 쓰라"고 명시적으로 지시한 배경, 5가지 실패모드를 하나씩 테스트로 밀어붙이다 "더 안전하게 하려던 수정이 오히려 타이밍 계약을 깬" 걸 발견한 순간, 그리고 "삭제 실패"와 "경로 재사용"이 파일시스템 신호만으로는 원리적으로 구분 안 된다는 걸 깨닫고 완벽한 해법 대신 "계약으로 명시"를 택한 판단.
- **재료(커밋·도식·수치)**: 커밋 `a770e2e`. before/after 코드 대비(claim 방식 vs 쓰고→지운다+가드 방식) 도식으로 표현 가능. `lastHandledDestinationPath` 상태 다이어그램(정상 흐름 vs 삭제 실패 흐름) 좋은 소재.
