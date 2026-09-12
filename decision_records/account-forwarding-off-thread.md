---
feature: account-forwarding-off-thread
date: 2026-09-09
branch: feat/disruptor-matching-core
commits: [5562106]
feeds: [adr, blog]
---

# 계좌 발신을 로직 스레드 밖으로 — single-writer는 I/O를 하지 않는다

## ADR 네타
### ADR-024 계좌 발신을 로직 스레드 밖으로 (LMAX: single-writer는 I/O 안 함)
- **context(무슨 상황)**: 계좌가 accept한 주문을 매칭으로 발신하는 구간(②-b)이다. LLD를 "핸들러(계좌 단일 컨슈머 스레드) 안에서 동기 Aeron offer(재시도하고 실패 시 throw)"로 짰다.
- **왜(문제·동기)**: 매칭 구독자가 없으면 offer가 5초 블로킹 후 throw하고, fail-fast가 엔진 스레드를 통째로 죽인다(통합 테스트가 다 깨졌다). best-effort-log로 바꿔도 유령 주문(계좌에는 accepted인데 매칭엔 없는 주문)이 남는다. 근본 원인은 단일 writer가 네트워크 I/O를 직접 한다는 것이다(LMAX 위반). Jack이 잡았다 — 갈 방향이 LMAX인데 왜 이렇게 됐느냐.
- **어떻게(대안·결정·트레이드오프)**: 발신을 로직 스레드 밖으로 뺀다. accept 시 출력 큐(Agrona `OneToOneConcurrentArrayQueue`, SPSC)에 offer만 하고, 전용 publisher 스레드가 드레인해서 발신한다. 백프레셔·재시도는 그 스레드가 맡는다. 로직 스레드는 논블로킹 offer만 한다. 대가는 직접 발신이 best-case에서 더 빠른데 그걸 포기한다는 점인데, 직접 발신은 백프레셔 때 tail이 폭증해서 엔진이 죽는다. 스레드 분리 대가는 링 핸드오프 수십 ns(Disruptor)이고 tail은 평평하다.
- **무엇을(실제 변경·파일·커밋)**: 1단계 커밋 `5562106`. 매칭이 없어도 엔진이 생존하는 걸 확인했다.
- **결과·수치**: 잔여 gap(큐 full·구독자 없음으로 드롭)은 2단계 durability(저널 + 리플레이)가 닫는다. 직접 발신이 더 빠른 건 best-case뿐이고 백프레셔 때 tail이 폭증하면 사망이다. 스레드 분리 대가는 링 핸드오프 수십 ns이고 tail은 평평하다. 도식은 `docs/_as_is_to_be_matching_forward.html`에 있다.

## 블로그 네타
### "핫패스에서 I/O 하면 안 되는 이유 — 단일 writer는 발신을 하지 않는다"
- **훅·핵심 주장**: 단일 writer 스레드가 accept 직후 동기로 네트워크 offer를 하면, 구독자가 없을 때 그 블로킹이 엔진 스레드를 죽인다. LMAX의 출력 스테이지처럼 발신을 로직 스레드 밖으로 빼야 한다.
- **context**: 계좌가 accept한 주문을 매칭으로 발신하는 구간. LLD를 동기 offer로 짰다가 엔진이 죽은 지점.
- **어떻게(서사·근거)**: 동기 offer → 구독자 없음 → 5초 블로킹 → throw → 엔진 스레드 사망(테스트 전멸). best-effort-log로 바꾸면 유령 주문. 근본은 단일 writer가 네트워크 I/O를 직접 한다는 것. LMAX 출력 스테이지 패턴 — SPSC 큐 offer만 하고 전용 publisher 스레드가 드레인. 직접 발신은 best-case만 빠르고 백프레셔 때 tail 폭증으로 죽는다, 스레드 분리 대가는 링 핸드오프 수십 ns.
- **재료(커밋·도식·수치)**: 1단계 `5562106`. 링 핸드오프 수십 ns. 도식 `docs/_as_is_to_be_matching_forward.html`.
