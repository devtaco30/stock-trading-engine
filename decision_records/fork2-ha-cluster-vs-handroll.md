---
feature: fork2-ha-cluster-vs-handroll
date: 2026-09-16
branch: docs/ec-flush-2026-09-16
commits: []
feeds: [adr, blog]
---

# fork2 HA — Aeron Cluster를 쓸까 손으로 짤까 (ADR-034 재검토·정정)

> 설계·리뷰 세션이 C(계좌 프로세스 페일오버 = fork2 HA) 착수 전에 "차라리 검증된 서드파티(Aeron Cluster)에 다 맡기자"는 갈림을 공식 문서로 검증하고, ADR-034의 사실 오류를 하나 정정한 세션. 코드 변경 0. 입력 = `docs/_v2_remaining_work_design.html`, `decision_records/c5-multiprocess-coordination.md`(ADR-034).

## ADR 네타

### ADR-035 fork2 HA — Aeron Cluster 재검토 후에도 손조립 유지 (ADR-034 보강·일부 정정)

- **context(무슨 상황)**: v2는 계좌 하나를 프로세스 하나만 고치는 single-writer 구조라 락을 없앴다(측정: v1 p50 12ms 대 v2 1.9ms). 대가로 그 프로세스가 죽으면 그 계좌는 거래가 멈춘다. 이걸 대기 프로세스가 이어받게 하는 게 fork2(HA)이고, 남은 다섯 묶음 중 코드 0·가장 불확실(6유닛). 착수 직전 Jack이 "손으로 standby+fencing 짜지 말고 검증된 서드파티에 다 기록·읽기 맡기는 게 낫지 않냐"고 물었다. 이 갈림을 닫는 세션.

- **왜(문제·동기)**: ① 손으로 짠 fencing(옛 주인이 GC로 멈췄다 되살아나 같은 계좌를 또 고치는 split-brain 차단)은 정합성을 틀리기 쉽고, 로컬 kill 데모 5회 GREEN으로는 진짜 정합성을 증명하기 어려운 부류다. ② ADR-034가 이미 Aeron Cluster를 기각했는데, 그 기록 안에서 두 문장이 서로 어긋나 있었다 — 기각 문단은 "Cluster가 손조립한 Disruptor 링버퍼 코어(+저널+replay+스냅샷)를 대체·은폐한다"고 적고, ★핵심 구분 문단은 "링버퍼(노드 안·ns)와 Raft(노드 간·ms)는 다른 레이어라 대체가 아니라 보완·공존"이라고 적었다. Jack이 "Cluster 써도 Disruptor 안 죽는다며"로 이 모순을 잡았다. 어느 쪽이 맞는지 공식 문서로 확정해야 결정이 선다.

- **어떻게(대안·결정·트레이드오프)**:

  **먼저 "서드파티에 다 맡기기"를 데이터/제어 평면으로 갈랐다.**
  - 핫패스(주문마다 계좌 read/write)를 외부 저장소(Redis·DB)로 빼면 = 주문마다 네트워크 왕복이 붙어 v1 병목으로 회귀. 측정한 1.9ms 우위를 반납. → 기각.
  - 제어 평면(누가 뭘 맡나·페일오버 조정)과 내구성 로그는 서드파티가 정석이고, v2는 **이미 그렇게 설계돼 있다**(조정=Kafka 컨슈머 그룹, 로그=Aeron Archive). 손으로 짜는 건 standby 저널 tailing + 승격 fencing 둘뿐.

  **"HA까지 통째로 서드파티" = Aeron Cluster(Raft). 공식 문서(aeron.io)로 확인한 사실:**
  - 로그(=저널): 입력 명령을 Raft로 순서 합의해 단일 로그로 만들고 노드마다 복제·아카이브. → v2 손조립 저널이 이 역할.
  - 복구/replay: "logs are replayed (e.g. for node recovery purposes)" — 재기동 시 자동. → `AccountJournalReplayer`가 이 역할.
  - 스냅샷: 트리거·`onTakeSnapshot()`(ExclusivePublication 제공)·`onStart()` 복원까지 Cluster가 하고, 앱은 직렬화(SBE)만 구현. → v2의 스냅샷 주기·lifecycle·durableSeq(방금 설계한 1-3)가 Cluster 몫, 남는 건 코덱뿐.
  - 페일오버: Raft 리더 선출. → fork2 전부(standby·fencing·조정)가 이것.
  - **결정타(theaeronfiles, Aeron 실제 클래스 `ClusteredServiceAgent`·`BoundedLogAdapter`와 일치)**: ClusteredService는 순서 잡힌 메시지를 "without intermediate buffering like a Disruptor" 받는다. Cluster의 로그+어댑터+에이전트가 단일 스레드에 직접 넘긴다. → **v2에서 링버퍼가 하던 순서화·전달 역할까지 Cluster가 대체한다.**

  **정정**: ADR-034 ★핵심 구분("링버퍼와 Raft는 다른 레이어, 공존")은 공식 문서 기준 틀렸다. 기각 문단("Cluster가 코어+저널+replay+스냅샷 대체")이 맞다. 링버퍼는 개념적으로 다른 층이 맞지만, Aeron Cluster 앱에서는 Disruptor를 앞에 두지 않고 Cluster가 전달을 맡으므로 "공존"이 아니다.

  **결정(Jack)**: Aeron Cluster 채택 안 함, fork2 손조립(A).
  - 후자(Cluster) 이익이 적음: v2 핵심 학습(락 없는 엔진·저널·스냅샷)은 이미 벌었고 수치까지 났다. Cluster는 그 메커니즘을 블랙박스로 가리고, HA만 얹는 게 아니라 엔진을 ClusteredService로 다시 앉히는 척추 재작성이다(병용 불가, either/or). Raft 운영 학습은 진짜지만 프로덕션 정합성이 목표일 때 값이 붙지, 로컬 kill 데모가 목표인 지금 상황엔 안 맞고 기존 v2 서사("락 vs single-writer, 손으로 만든 깊이")를 무디게 한다.
  - A는 증거를 낸다. 그리고 증거가 유닛별로 갈린다:
    - **3-1**(슬롯 소유 + api 라우팅 배선) → 샤딩 기울기 **수치**(계좌 워커 1개 vs 2개 처리량). 지금 못 잰 이유가 api가 목적지 하나로만 보내서다(4-3). 저위험·앞 유닛. v2 샤딩 주장 중 유일하게 수치 없는 조각.
    - **3-2~3-5** → kill 데모(정합성 pass/fail). 손조립 fencing caveat가 여기.
  - ⏳ 미정: A-전체(3-1~3-5) vs A-수치먼저(3-1만 먼저 뽑고 kill 데모는 결과 보고).

- **무엇을(실제 변경·파일·커밋)**: 코드 변경 0(순수 설계·리뷰). 착수 시 코드로 확인한 구멍 — account-worker는 슬롯 개념 없이 seed-accounts로만 소유 판단(`AccountWorkerProperties`), api는 `ShardRoutingTable`을 안 쓰고 endpoint 하나로 보냄(`AccountOrderPublishConfig`), Archive replay는 유한 길이만 있고 라이브 tail 없음(`AccountJournalReplayer`), Kafka 리스너에 리밸런스 콜백 인프라 없음, 부분 드레인 API 없음. `ShardRoutingTable`(slot→endpoint 순수함수 hash%P, fmix64로 Snowflake 하위비트 쏠림 방지)는 구현 완료.

- **결과·수치**: 미측정(설계 결정). 참조 수치 = v2 p50 1.9ms 대 v1 12ms(측정 완료, `_c7_v1_v2_throughput_report.html` 9절). 3-1이 뚫을 샤딩 기울기 수치는 아직 없음.

## 블로그 네타

### "검증된 프레임워크를 안 쓰기로 한 이유 — Aeron Cluster를 공식 문서까지 읽고 손조립을 택하다"
- **훅·핵심 주장**: "HA는 검증된 걸 써라"가 상식이지만, 학습 프로젝트에서 그 상식이 언제 뒤집히는지를 Aeron Cluster로 실증했다. Cluster는 저널·replay·스냅샷·페일오버를 다 주고, ClusteredService는 "without a Disruptor"로 링버퍼의 전달 역할까지 가져간다 — 즉 내가 손으로 배운 것 전부를 대체한다. 이미 그 메커니즘을 손으로 짜서 수치까지 낸 시점에서, Cluster로 바꾸는 건 배움을 블랙박스로 덮는 일이다.
- **context**: fork2(HA) 착수 직전 "차라리 서드파티에 다 맡기자"는 갈림을 공식 문서로 검증한 과정. ADR-034 안의 자기모순(링버퍼가 죽냐 공존하냐)을 잡고 공식 문서로 정정한 것이 서사의 반전.
- **어떻게(서사·근거)**: 데이터 평면(핫패스=외부저장소면 v1 회귀) vs 제어 평면(조정·로그=서드파티가 정석, 이미 그럼) 구분 → Cluster가 실제로 뭘 주는지 공식 문서 인용(로그·replay·스냅샷·Raft선출·"without a Disruptor" 전달) → ADR-034 두 문장의 모순과 어느 쪽이 맞았나 → "채택하면 엔진을 ClusteredService로 재작성=either/or지 병용 아님" → 학습 가치로 판단(핵심 이미 벌었음 + 서사 무뎌짐).
- **재료**: aeron.io 공식 문서(replicated-state-machines·raft-consensus), theaeronfiles clustered-service, ADR-034(`c5-multiprocess-coordination.md`), `ShardRoutingTable.java`, `_v2_remaining_work_design.html`.

### "분산 ≠ 합의, 그리고 링버퍼는 정말 다른 레이어인가 — ADR을 내가 쓴 문장으로 반박당한 이야기"
- **훅·핵심 주장**: ADR-034에서 "분산은 샤딩이라 합의가 필요 없고, 링버퍼(ns)와 Raft(ms)는 다른 레이어라 공존한다"고 적었는데, 정작 Aeron Cluster 공식 문서를 읽으니 ClusteredService는 링버퍼 없이 Cluster 로그에서 직접 메시지를 받는다. "다른 레이어라 공존"은 개념적으론 맞아도 실제 Cluster 앱에선 틀렸다 — Cluster가 전달을 맡으면 Disruptor를 앞에 둘 자리가 없다.
- **context**: Jack이 "Cluster 써도 Disruptor 안 죽는다며"라고 내 ADR의 두 문장을 부딪쳐 물었고, 확인 전에 약한 쪽(공존)에 기대 답했다가 공식 문서로 정정한 과정. 검증 전 단정 → 문서로 정정의 실례.
- **어떻게(서사·근거)**: 복제(합의 필요)와 샤딩(합의 불필요)의 구분은 유효 / 하지만 "링버퍼와 Raft 공존"은 전달 계층에서 깨진다(ClusteredServiceAgent+BoundedLogAdapter가 단일 스레드 전달) / LMAX Disruptor와 Aeron Cluster는 같은 저자(Martin Thompson) 계보라 Cluster가 Disruptor의 분산·Raft판이다 / 그래서 대체지 보완이 아니다.
- **재료**: aeron.io replicated-state-machines, theaeronfiles clustered-service(`ClusteredServiceAgent`·`BoundedLogAdapter`), ADR-034 두 문단 원문, Martin Thompson의 Disruptor·Aeron·Cluster 계보.
