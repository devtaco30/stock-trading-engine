---
feature: fork1-udp-transition
date: 2026-09-15
branch: feat/fork1-udp-transition
commits: [b2a1e80, 348a838, e6f0c06]
feeds: [adr, blog]
---

# fork1 — Aeron ipc → 실제 UDP 전환 (U1~U3a)

C5(엔진을 별도 프로세스로 펼치고 실제 UDP로 잇기)의 첫 실구현. api·account-worker·matching-worker는 이미 각자 Spring Boot 앱인데, 그 사이 데이터 홉이 `aeron:ipc`(같은 프로세스 전용)라 별도로 띄우면 서로 안 붙었다. 3b(로컬 3-JVM 라이브 왕복)는 프로젝션 트랙 우선으로 보류.

## ADR 네타

### ADR: Aeron 전송을 config 외부화로 ipc→udp 전환 (홉당 고정 endpoint)
- **context(무슨 상황)**: v2 엔진 호스트들이 이미 별도 앱(ADR-018)이지만 그 사이 3개 데이터 홉(주문 4004·매칭 2002·체결 6001)이 `aeron:ipc` 하드코딩 상수라 크로스프로세스 연결이 안 됐다. 실제 UDP로 이어야 "네 서비스가 처음부터 끝까지 실제로 도는" 지점(C5)을 연다.
- **왜(문제·동기)**: 채널이 코드 상수라 환경별(로컬 localhost / K8s DNS) endpoint를 못 바꾼다. 발신·수신 양쪽에 상수가 복제돼 어긋날 위험도.
- **어떻게(대안·결정·트레이드오프)**:
  - **데이터 3홉만 대상**. 저널·스냅샷·리플레이 스트림(2005/2006/4005/4006/6002)은 워커 내부 전용이라 제외.
  - **채널·스트림 ID·Archive 제어채널을 코드 상수 → Spring 속성으로 외부화, 기본값은 현행(`aeron:ipc`/`localhost:8010`)**. 그래서 기존 in-process 테스트는 그대로 GREEN(동작 불변), udp는 실행 프로파일에서만 오버라이드. localhost→컨테이너 DNS 승격이 코드 변경 없이 됨.
  - **스트림 ID를 core로 통합**(`AeronStreamIds`) — 5곳 복제 제거. udp 전환으로 어차피 다 건드리는 시점이라 같이.
  - **홉당 고정 endpoint 하나(단일 샤드)**. accountId 라우팅 맵·P 샤딩은 fork2.
  - **검증 = 로컬 3-JVM 먼저 → docker-compose 승격**. UDP 홉 자체를 먼저 de-risk하고 컨테이너 네트워킹 복잡도는 나중. (K8s DNS는 C5 앵커지만 first-light가 먼저.)
- **무엇을(실제 변경·파일·커밋)**:
  - U1 `b2a1e80`: 3 데이터 채널·2 Archive 제어채널 `@Value` 외부화, 스트림 ID `core/aeron/AeronStreamIds` 통합, package-visible 상수 `DEFAULT_*` 개명.
  - U2 `348a838`: 앱별 `application-udp.yml`(endpoint 20040/20020/20060, Archive 제어 8010/8011 분리).
  - U3a `e6f0c06`: 실제 `gradle bootRun`으로 띄우려다 발견한 pre-existing 갭 3개 수정(아래 블로그 네타).
- **결과·수치**: 세 앱 다 `gradle bootRun --args='--spring.profiles.active=udp'`로 clean 기동, `lsof`로 udp 바인딩·포트 무충돌 확인. **부하·지연 미측정.** 3b(라이브 왕복) 보류. 크로스프로세스 체결 복구(`AccountFillReplayer` readFrom의 "단일 recording 가정")는 C6 몫 — fork1 3b는 fresh boot 라이브라 그 경로 안 탐(§6 명시).

## 블로그 네타

### "별도 앱인데 안 붙는다 — aeron:ipc를 udp로 바꾸며 드러난 숨은 기동 갭 3개"
- **훅·핵심 주장**: 모듈을 별도 앱으로 만들어 뒀어도, "실제로 각각 프로세스로 띄우는" 순간에야 그동안 아무도 이 앱들을 standalone으로 안 띄웠다는 사실이 드러난다. 세 개의 기존 갭이 그때 한꺼번에 튀어나왔다.
- **context**: v2 워커들(api·account·matching)을 처음으로 각각 `gradle bootRun`으로 띄워 UDP로 잇기.
- **어떻게(서사·근거)**: ①`bootRun`에 Agrona/Unsafe `--add-opens` 플래그가 없어(test 태스크에만 있었다) 세 앱 다 기동조차 안 됨 ②`AccountWorkerProperties.seedAccounts()`가 unset이면 NPE ③api `sql.init`이 레포에 없는 `data-scenario1-test.sql`을 참조해 실패. 셋 다 udp/Aeron과 무관한 pre-existing 갭인데 "실제로 띄우기"가 강제로 드러냈다. + 채널을 config로 외부화해 "로컬 localhost→docker DNS를 코드 변경 없이 승격"하게 만든 이야기.
- **재료(커밋·도식·수치)**: 커밋 b2a1e80·348a838·e6f0c06, LLD `docs/_fork1_udp_transition_lld.md`, `lsof` udp 바인딩 검증. 수치 미측정.
