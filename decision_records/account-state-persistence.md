---
feature: account-state-persistence
date: 2026-09-08
branch: feat/disruptor-matching-core
commits: []
feeds: [adr, blog]
---

# 계좌 상태 영속 — DB 조회모델 프로젝션

## ADR 네타
### ADR-021 계좌 상태 영속 (DB 조회모델 프로젝션)
- **context(무슨 상황)**: v2의 잔고·보유를 어디에 durable하게 두고 어떻게 조회할지 정하는 자리다. v2는 주문 검증에서 DB 비관적 락을 뺐고 검증을 account-worker 인메모리 single-writer로 옮겼다. 그러면 영속은 어떻게 하느냐가 남는다.
- **왜(문제·동기)**: 처음에 "로그가 유일한 원천이고 DB는 없다"고 단정했다. 이건 오버클레임이었다. Jack이 반증했다 — 무거래 신규 계좌는 접을 이벤트가 없는데 로그만으로 그 상태를 어떻게 만드느냐. event sourcing은 상태를 이벤트의 누적으로 복원하는데, 이벤트가 하나도 없는 계좌는 복원할 재료 자체가 없다. v2에서 DB I/O를 뺀 건 핫패스(느린 지점)에서만이지 아예 안 쓴다는 뜻이 아니었다.
- **어떻게(대안·결정·트레이드오프)**: 계좌 상태를 DB 조회모델(materialized view)로 둔다. account-worker가 인메모리 authority이고, full-state 출력 이벤트(accountId 키 + seq)를 낸다. 이걸 별도 프로젝션 컨슈머(대안 ii, write-behind 대안 i와 대비)가 받아 DB에 upsert한다. 들어온 seq가 저장된 seq 이하면 거부해서 stale 갱신을 막는다. 프로젝션은 매 이벤트(per-event)마다 하고, 출력이 full-state라 중간 이벤트를 coalesce해도 안전하다. 계좌 생성은 DB에 직접 insert(잔고 0), 입출금은 account-worker를 경유(single-writer)한다. LMAX의 "I/O를 로직 스레드에서 분리" 원칙을 따른다.
- **무엇을(실제 변경·파일·커밋)**: 계좌 상태 = DB 조회모델. account-worker = 인메모리 authority. full-state 출력 이벤트 → 프로젝션 컨슈머 → DB upsert(stale 거부). 전제는 두 경로 정렬(C5-4)과 핸드오프(C6). 도식은 `docs/_v2_account_state_persistence.html`에 있다. 관련 메모 [[v2 계좌 상태 영속 모델]].
- **결과·수치**: DB는 핫패스 밖에서 로드·조회·변경 영속을 맡고, 검증만 인메모리에서 한다. 비관적 락을 안 쓰는 게 single-writer의 존재 이유다. 안전망은 낙관적 seq(stale 거부)다. full-state를 택한 이유는 Jack이 준 시나리오로 설명된다 — DB가 seq 160일 때 163이 먼저 도착하고 161이 나중에 온다. delta 방식이면 161·162를 스킵해서 돈이 샌다. 순서를 강제하면 유실 시 무한 대기다. full-state + stale 거부는 두 경우 모두 무해하고 자가 치유된다. (정성적 근거, 실측 수치 없음)

## 블로그 네타
### "로그가 진실의 원천? 무거래 계좌가 반증한다"
- **훅·핵심 주장**: event sourcing에서 "로그가 유일한 진실의 원천"이라는 명제는 무거래 신규 계좌 앞에서 깨진다. 접을 이벤트가 없는 계좌는 로그로 복원할 수 없다. DB 조회모델이 필요하다.
- **context**: v2 계좌 상태 영속 방식 선택. "DB 없이 로그만"으로 단정했다가 반증당한 지점.
- **어떻게(서사·근거)**: 정직한 실패 서사 — Claude가 "DB 없음"으로 오도했고 Jack이 무거래 계좌 반례로 잡았다. event sourcing의 한계, DB 프로젝션이 왜 필요한지. v2에서 DB를 뺀 건 핫패스에서만이지 전부가 아니라는 점.
- **재료(커밋·도식·수치)**: 도식 `docs/_v2_account_state_persistence.html`. full-state + stale 거부 설계.

### "락 없는 계좌의 시퀀스 정합성 — full-state vs delta"
- **훅·핵심 주장**: 락을 없앤 계좌 상태를 DB에 반영할 때, 이벤트를 delta로 흘리면 순서가 뒤집힐 때 돈이 새거나 무한 대기한다. full-state 출력 + stale 거부가 순서 문제를 자가 치유한다.
- **context**: account-worker가 낸 상태 이벤트를 프로젝션 컨슈머가 DB에 반영하는 경로. 이벤트 도착 순서가 보장되지 않는다.
- **어떻게(서사·근거)**: DB seq 160, 163이 먼저 오고 161이 나중에 오는 시나리오. delta면 161·162 스킵으로 돈이 샌다. 순서를 강제하면 유실 시 무한 대기다. full-state는 최신 seq만 이기게 두면 되므로 순서 뒤집힘·유실 양쪽에 무해하다.
- **재료(커밋·도식·수치)**: 163/161 시나리오. 도식 `docs/_v2_account_state_persistence.html`.
