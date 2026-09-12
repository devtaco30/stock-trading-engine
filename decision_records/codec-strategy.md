---
feature: codec-strategy
date: 2026-09-12
branch: fix/content-poison-guard
commits: []
feeds: [adr, blog]
---

# 코덱 전략 — SBE를 평가하고 손코덱을 유지하기로 (오버엔지니어링 회피)

> content-poison 방어를 설계하다 SBE 이관을 진지하게 검토했다(리서치 + 파일럿까지). 결론은 손코덱 유지다. 이 문서는 "왜 있는 표준을 안 썼나"의 근거다 — 그게 이 결정의 핵심 가치다.

## ADR 네타

### ADR — 와이어 코덱은 손코덱을 유지한다 (SBE 평가 후 기각, ADR-013 유지·보강)
- **context(무슨 상황)**: v2는 Aeron이 바이트를 나르고 encode/decode를 손으로 짰다(ADR-013, 흐름 학습 우선으로 수동 선택). content-poison(잘못된 바이트·unknown enum ordinal이 decode에서 throw→저널에 박힘→replay 무한루프) 방어를 손코덱 위에 넣으려던 참에, 그게 SBE가 이미 자동화하는 것의 손 재구현임을 깨닫고 SBE 이관을 검토했다.
- **왜(문제·동기)**: "라이브러리가 있는데 손으로 짠 걸 유지하는 게 맞나"라는 물음. 특히 지금 짜려던 enum ordinal 검증은 SBE의 `sbe.decode.unknown.enum.values` 옵션이 codegen으로 만들어주는 바로 그것이었다.
- **어떻게(대안·결정·트레이드오프)**: SBE를 파일럿까지 만들어 검증하고(Gradle codegen 배선 + `AccountOrderCodec` 스키마 이관 + SBE_UNKNOWN 동작 확인) 1차 소스로 저울질했다. 결론 — 우리 케이스에서 SBE의 고유 이득이 거의 없다. ①성능: 이미 Agrona 저수준 API로 짜서 SBE도 같은 Agrona로 내려가 런타임 이득이 불확실하다(SBE 벤치의 우위는 전부 protobuf/텍스트 대비고, 잘 짠 수동 바이너리 대비가 아니다). ②SBE 본령(스키마 진화·다자 상호운용): 우리는 단일 JVM IPC·1인·메시지 3~5종·스키마 안정이라 효용이 약하다(SBE 실채택처는 전부 거래소 공개 프로토콜). ③content-poison 방어: SBE 없이 손코덱 `tryDecode`(구조 검증 + 최후 catch) 몇 줄로 얻는다. 반대로 무는 비용은 실질이다 — codegen 빌드 단계, 레이아웃 자유 상실(고정 blockLength라 PLACE/CANCEL 분기를 별 메시지로 쪼개야 함), flyweight 재사용 제약(replay가 List에 못 담아 record 복사), codec 3개 전면 재작성 + 배선 7곳. → 손코덱 유지 + `tryDecode`로 poison만 막는다. SBE는 스키마 진화·다언어·UDP 상호운용이 실제로 필요해지면 재평가한다.
- **무엇을(실제 변경·파일·커밋)**: SBE 파일럿 브랜치(`refactor/sbe-codec-migration`)와 파일럿 커밋은 폐기했다. content-poison 방어는 `fix/content-poison-guard`에서 손코덱 위에 구현한다 — `tryDecode`(codec 구조 검증, 무효는 예외 대신 `Optional.empty()` 반환) + 계좌 핸들러 도메인 예외 격리(U1, 매칭 `MatchingEventHandler:53-57` 대칭) + replay·폴 스레드 decode guard(U2·U3). 도식 `docs/_content_poison_prevention_lld.html`. ADR-013(수동 코덱 선택)은 대체되지 않고 유지·보강된다.
- **결과·수치**: 미측정(구현 전). SBE 파일럿으로 확인한 사실 — SBE_UNKNOWN이 손코덱 `OrderSide.values()[byte]`와 같은 실패 지점을 sentinel로 바꿔준다는 것, first-party Gradle 플러그인이 없어 `JavaExec`로 `SbeTool`을 직접 부른다는 것. 이 사실들은 재평가 시 재사용한다.

## 블로그 네타

### "있는 라이브러리를 안 쓰기로 한 판단 — SBE를 파일럿까지 하고 손코덱을 유지한 이유"
- **훅·핵심 주장**: 표준 도구가 있다고 늘 쓰는 게 판단이 아니다. 그 도구가 푸는 문제를 내가 실제로 가졌는지 따지고, 안 가졌으면 안 쓰는 것도 판단이다. 단 그 판단은 근거가 있어야 한다 — "몰라서 안 씀"과 "따져보고 안 씀"은 다르다.
- **context**: content-poison 방어를 손코덱 위에 짜려다 SBE 이관을 검토. 리서치 + 파일럿(Gradle 배선·스키마·SBE_UNKNOWN 확인)까지 하고 기각.
- **어떻게(서사·근거)**: SBE의 본령은 다자 상호운용(거래소 공개 프로토콜)과 잦은 스키마 진화인데 우리(단일 JVM·1인·스키마 안정)엔 효용이 약하다. 성능은 이미 Agrona 손코덱이라 이득이 불확실(SBE 벤치는 protobuf 대비). content-poison은 `tryDecode` 몇 줄로 막는다. 무는 비용(codegen·레이아웃 경직·flyweight 제약·전면 재작성)이 이득보다 크다. → 손코덱 유지. "파일럿까지 해보고 안 쓴" 것이라 근거가 단단하다.
- **재료(커밋·도식·수치)**: 폐기한 SBE 파일럿(`refactor/sbe-codec-migration`의 Gradle 배선·`account-order-schema.xml`), ADR-013, 리서치 1차 소스(Real Logic SBE wiki, Martin Thompson, Databento), 도식 `docs/_content_poison_prevention_lld.html`.

### "poison-on-replay — fail-fast + replay의 진짜 구멍"
- **훅·핵심 주장**: 저널+리플레이로 크래시를 복구하는 시스템에서, 잘못된 내용 하나가 무한 재시작 루프를 만든다. 인프라 장애엔 옳은 fail-fast가 내용 문제엔 독이 된다.
- **context**: 계좌 엔진은 저널을 업무보다 먼저 돌린다(게이팅). poison이 저널에 durable하게 박힌 뒤 업무에서 throw → 크래시 → 재생이 같은 엔트리로 또 크래시.
- **어떻게(서사·근거)**: 매칭은 도메인 예외를 잡아 폐기(`MatchingEventHandler:53-57`)하는데 계좌는 안 잡는 비대칭이 핵심. decode는 업무 핸들러보다 앞이라 핸들러 catch로 못 잡는 별도 구멍. throw를 인프라 장애로만 좁히고 나쁜 내용은 격리해야 replay가 통과한다. 3층 방어(구조=codec tryDecode / 값 이상치=business / 도메인 불변식=격리).
- **재료(커밋·도식·수치)**: `AccountEngine`의 `handleEventsWith(journal).then(business)`, `AccountEventHandler:47-61`, `AccountState:151·155·219·223·249`. 도식 `docs/_content_poison_prevention_lld.html`.
