---
feature: content-poison-guard
date: 2026-09-13
branch: fix/content-poison-guard
commits: [43d91cd, 6d59fbb, a100481]
feeds: [adr, blog]
---

# content-poison 방어 — business 예외 격리가 실질, decode 예외는 Aeron이 흡수하고 있었다

## ADR 네타
### ADR — content-poison 방어: 진짜 위험은 business 예외뿐, decode 예외는 위험이 아니었다
- **context(무슨 상황)**: v2 엔진은 크래시 후 저널을 replay해 복구한다. 잘못된 내용·손상된 바이트가 처리 중 예외를 던지면, 그게 저널에 박혀 있어 재시작 replay 때 또 죽는 무한루프(poison-on-replay)를 우려했다. 3층 방어(구조=codec / 값=business / 도메인=격리)를 설계했다.
- **왜(문제·동기)**: 계좌 핸들러가 매칭 핸들러(`MatchingEventHandler:53-57`, 도메인 예외 catch)와 달리 catch가 없어, `AccountState`의 `IllegalStateException`(예약 없는 체결 등)에 스레드가 죽고 recover가 같은 엔트리로 또 죽는 구조가 실제로 있었다.
- **어떻게(대안·결정·트레이드오프)**: 착수해보니 세 층의 위험이 서로 달랐다. ①**business 예외(U1)**: disruptor 링에서 recover가 핸들러를 직접 호출하는 경로라 진짜로 재현됐다(TDD red 확인) — 계좌 핸들러에 매칭 대칭 catch를 넣어 격리·폐기로 닫았다. ②**decode 예외(U2·U3)**: 손상 바이트가 codec.decode에서 던지는 예외인데, 구현 중 실측으로 **Aeron `Image.poll`이 이 예외를 내부 try-catch로 이미 삼키고 있음을 소스로 확인**했다(`Subscription.poll`도 `Image.poll`에 위임, 커스텀 errorHandler 미설정이라 기본 핸들러가 stderr로만 출력). 즉 크래시·replay abort·폴 스레드 사망은 이 경로에서 애초에 일어나지 않았다. 원래 LLD가 인용한 조사가 이 부분에서 틀렸다.
- **무엇을(실제 변경·파일·커밋)**: U1 `43d91cd`(`AccountEventHandler.onEvent`에 `catch (IllegalArgumentException | IllegalStateException)` + 로그, 매칭 대칭, recover 자동 커버) · codec `tryDecode` `6d59fbb`(손코덱 3종에 구조 검증 후 무효면 `Optional.empty()`) · U2 `a100481`(replayer가 `tryDecode` empty면 skip+로그). **U3(폴 스레드)는 생략** — decode 예외가 위험이 아니라 판명됐고, 남는 근거(로그 가시성)는 토이 프로젝트로 강조할 가치가 없어 Jack이 중단 결정.
- **결과·수치**: poison-on-replay 무한루프는 U1이 닫았다(실질 방어). U2는 조용히 삼켜지던 손상 fragment를 우리 로거로 드러내는 가시성 개선(이미 커밋, 해롭지 않아 유지). 전체 모듈 회귀 그린(워커 보고, 재실행 시 통과 — `AccountFillIntegrationTest` 기존 Kafka 플레이키 1회는 변경과 무관).

## 블로그 네타
### "가정을 검증했더니 틀렸다 — 라이브러리가 이미 예외를 삼키고 있었다"
- **훅·핵심 주장**: "이러면 터질 것"이라는 설계 가정을 코드로 재현하려다, 실은 통신 라이브러리가 그 예외를 이미 삼키고 있었음을 발견했다. 방어 코드를 짜기 전에 "정말 안 막혀 있나"를 실측하는 게 먼저다.
- **context**: 손상 바이트가 decode에서 예외를 던져 replay/폴 스레드를 죽인다고 가정하고 guard를 넣으려 했다.
- **어떻게(서사·근거)**: TDD로 red(예외 재현)를 확인하려는데 이미 green이었다. 디버그 로그로 추적하니 예외는 던져지지만 replay가 안 죽었다. Aeron 소스(`Image.java`의 `poll`)를 열어보니 FragmentHandler 호출을 내부 try-catch로 감싸 `errorHandler.onError`로 넘길 뿐 호출자로 전파하지 않았다. 가정이 틀렸음을 소스로 확정했다.
- **재료(커밋·도식·수치)**: Aeron 1.48 `Image.java`·`Subscription.java`, `AccountJournalReplayer`·`AccountOrderReceiver`. 커밋 `a100481`(가시성 목적으로만 유지).

### "poison-on-replay는 어디서 진짜였나 — decode가 아니라 business 예외"
- **훅·핵심 주장**: 같은 "잘못된 입력"도 어느 층에서 예외가 나느냐에 따라 위험이 완전히 다르다. 무한루프의 진짜 자리는 decode(라이브러리가 삼킴)가 아니라 business 로직(우리가 직접 호출)이었다.
- **context**: 저널을 업무보다 먼저 기록하는 게이팅(`handleEventsWith(journal).then(business)`) + recover가 핸들러를 직접 호출하는 구조.
- **어떻게(서사·근거)**: business 예외는 disruptor 링에서 recover가 직접 부르는 경로라 catch가 없으면 매 재시작 죽는다(계좌/매칭 핸들러 비대칭이 원인). decode 예외는 Aeron 폴 루프 안이라 라이브러리가 삼킨다. 그래서 방어는 business 층(핸들러 catch, 매칭 대칭)에 집중하는 게 맞았다.
- **재료(커밋·도식·수치)**: `AccountEventHandler:47-61`(catch 추가 `43d91cd`), `AccountState:151·155·219·223·249`, `MatchingEventHandler:53-57`(대칭 참조). 도식 `docs/_content_poison_prevention_lld.html`.
