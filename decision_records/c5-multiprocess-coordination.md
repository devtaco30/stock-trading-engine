---
feature: c5-multiprocess-coordination
date: 2026-09-14
branch: feat/fill-aeron-migration
commits: []
feeds: [adr, blog]
---

# C5 — 멀티프로세스 전송·라우팅·HA·조정 (설계)

> 설계 단계 기록(미구현). v2 단일프로세스(aeron:ipc)를 여러 프로세스로 펼치는 C5의 전송·샤딩·페일오버·조정을 fork별로 정한 세션. 유닛 착수 전 "결정→근거→LLD" 순서를 지키려 fork를 먼저 못 박음. 도식: `docs/_c5_scoping.html`·`_c5_transport_options.html`·`_c5_routing_map.html`·`_c5_kafka_group_coordination.html`·`_c5_layers_and_nodes.html`.

## ADR 네타

### ADR-034 C5 전송·조정 — 샤딩으로 합의를 피하고, 조정은 Kafka 그룹에 편승

- **context(무슨 상황)**: v2 계좌·매칭·정산은 지금 한 JVM 안 스레드고 그 사이가 `aeron:ipc`(같은 프로세스 전용)라, 별도 프로세스로 띄우면 안 붙는다. C5 = 이걸 별도 JVM으로 펼치고 실제 UDP로 잇는 단계. 전송·라우팅·HA·조정을 여기서 정한다. 앵커 = 샤딩(ADR-031 고정슬롯+정적라우팅)·전송 하이브리드(ADR-033 핫패스 Aeron/정산 Kafka).

- **왜(문제·동기)**: ①워커가 각자 임베디드 MediaDriver라 크로스프로세스가 실제로 안 붙음(통합테스트가 원격측을 같은 프로세스서 흉내). ②페일오버는 미래 대비가 아니라 지금 강제되는 필수 — 프로세스는 죽고, 그 슬롯 계좌를 안전하게(split-brain 없이) 넘겨야 함(ADR-031도 "리밸런싱 필요"). ③그 조정을 뭐로 하나 — 정적 ConfigMap은 kubelet ~2분 staggered 전파라 라이브 플립 시 sender 맵 뷰가 어긋나 split-brain(잔고 오염).

- **어떻게(대안·결정·트레이드오프)**:

  **fork1 전송**: 1:1은 unicast(`endpoint=host:port`). 체결 fan-out(1건→매수·매도 두 계좌)은 unicast 2번(라우팅맵으로 목적지 계산) — multicast는 K8s 미지원 탈락, MDC는 목적지가 체결마다 달라 부적. 드라이버=임베디드 per-워커(현행 연장, 외부 공유는 C7 튜닝 시). 주소=K8s DNS+정적 맵. **Archive 녹화=수신자(계좌) 측 REMOTE**(계좌 복구=자기 Archive replay 자족, 발신자측이면 매칭 Archive 원격 의존).

  **fork2 라우팅맵+HA**: 
  - 분산 스킴 = 샤딩(ADR-031): `hash(accountId)%P` 고정 슬롯(P 크게, Redis Cluster 16384·Kafka 파티션 근거) + 정적 라우팅. 계좌는 %P라 새 계좌가 기존 슬롯에 떨어질 뿐 — 종목의 상장/폐지 같은 per-entity churn 없음. 바뀌는 건 슬롯→노드(스케일·페일오버)뿐.
  - HA 축 = **warm standby 채택.** cold(안정신원 재시작+replay)는 그 슬롯 수 초 불가용 + sender never-drop/재시도/멱등 필수라(안 하면 다운 중 입력 유실) 트레이딩에 부담 → Jack이 warm 택. warm = 대기 노드가 주 노드 저널을 계속 tailing해 데워두고 즉시 인수.
  - warm → 페일오버 시 슬롯 주인 endpoint가 바뀜(맵 동적) + "단일 소유 fencing"(옛 주인 되살아나도 둘 다 주인 아님) 필요. → **조정 = Kafka 컨슈머 그룹 재사용**(새 코디네이터 etcd/ZK 안 지음, Kafka는 하이브리드로 이미 상주). 그룹 코디네이터가 멤버십·장애감지(session timeout)·재배정·단일소유 fencing, standby=follower(저널 tailing), compacted 토픽=맵. **이게 Kafka Streams standby-replica 패턴 그대로**(num.standby.replicas + 그룹 리밸런스).
  - **Aeron Cluster 기각(유지).** Cluster=Raft 합의 기반 복제 상태머신. 데이터 순서 합의는 우리에게 불필요 — 샤딩으로 한 계좌=한 노드(single-writer)라 노드 간 맞출 순서가 없고, 순서는 노드 안 로컬 링버퍼가 잡음. Cluster를 쓰면 안 쓰는 합의를 떠안고, 우리가 학습으로 손조립한 Disruptor 링버퍼 코어(+저널+replay+스냅샷)를 로그/합의로 대체·은폐한다. 필요한 건 control-plane 단일소유 조정뿐 → Kafka 그룹으로 충분.

  **★핵심 구분(이 세션의 개념 정리)**: 분산 ≠ 합의. 분산엔 두 갈래 — 복제(같은 상태 여러 노드→순서 동의 필요→Raft)와 샤딩(다른 상태 다른 노드→공유상태 없음→합의 불필요). 우리는 샤딩이라 데이터 분산은 샤딩+라우팅으로, 합의 없이 한다. Raft가 필요한 건 복제형(단일 writer 없는)뿐. 그리고 링버퍼(노드 안·스레드 사이·ns·순서화)와 Raft(노드 간·네트워크·ms·복제 합의)는 **다른 레이어** — 대체가 아니라 보완(공존). warm standby의 promotion fencing만 control-plane 조정이 필요(드묾).

- **무엇을(실제 변경·파일·커밋)**: 설계만, 코드 미구현. fork1·2 확정, fork5(api 진입점)가 다음. fork2 잔가지(P값·standby 저널 tailing 배선·슬롯↔Aeron 스트림 구독 연결·PV/StatefulSet 상태도달·compacted 맵 발행 형식)는 유닛 LLD 때 확정. 도식 5종(docs/, 커밋 제외).

- **결과·수치**: 미측정(설계 단계). 근거 리서치 — Aeron+Kafka 병용은 실전(디지털자산 거래소 Aeron Sequencer로 hot path DB 제거 µs 달성 / Confluent+글로벌 투자은행 Kafka durable sub-5ms 160만 msg/s), Kafka Streams가 컨슈머 그룹으로 stateful task 배정+standby+changelog replay(우리 조정 절반의 청사진), Redis Cluster 16384 슬롯 마이그레이션(MIGRATING/IMPORTING/ASK), KIP-429 협력 리밸런스·KIP-441 스무스 스케일링. ⚠️"Aeron 데이터 + Kafka 그룹 조정"의 정확한 조합을 하는 named 공개 사례는 못 찾음(트레이딩사는 아키텍처 비공개) — 두 검증된 조각의 합성이며 Kafka Streams가 조정 절반의 선례.

## 블로그 네타

### "분산은 합의가 아니다 — 샤딩으로 Raft를 피한 이야기"
- **훅·핵심 주장**: "분산 시스템 = 합의(Raft) 필요"는 흔한 오해다. 합의가 필요한 건 복제(같은 상태를 여러 노드에)뿐이고, 샤딩(다른 상태를 다른 노드에)은 공유 상태가 없어 합의할 게 없다. 우리는 계좌를 accountId로 샤딩해 한 계좌=한 노드(single-writer)로 만들어, 데이터 순서를 노드 안 링버퍼로 잡고 노드 간 합의를 아예 피했다.
- **context**: C5 멀티프로세스 설계에서 "Raft 안 쓰면 분산 어떻게?"라는 질문을 받고 분산의 두 갈래를 가른 과정.
- **어떻게(서사·근거)**: 복제 vs 샤딩 / Kafka(파티션+leader-follower, 합의는 메타데이터만)·Redis Cluster(슬롯+gossip)·샤딩 DB가 다 데이터 합의 없이 분산 / 우리 링버퍼(노드 안 순서, ns)와 Raft(노드 간, ms)는 다른 레이어라 대체 아님 / 합의가 진짜 필요한 자리는 warm standby promotion fencing(control-plane, 드묾)뿐이고 그건 Kafka 그룹으로.
- **재료**: `_c5_layers_and_nodes.html`, Kafka/Redis/샤딩DB 사례, 링버퍼 vs Raft 레이어 도식.

### "Aeron 시스템의 조정 — Cluster를 안 쓰고 이미 있는 Kafka 그룹에 편승"
- **훅·핵심 주장**: Aeron 시스템에서 HA/조정의 정석은 Aeron Cluster(Raft)지만, 그건 우리가 학습으로 손조립한 Disruptor 링버퍼 코어를 통째 대체한다. 우리는 샤딩이라 데이터 합의가 필요 없고, warm standby의 페일오버 조정만 필요한데 — 그건 하이브리드로 이미 상주하는 Kafka의 컨슈머 그룹(Kafka Streams standby 패턴)에 편승하면 새 코디네이터 없이 얻는다.
- **context**: cold vs warm 페일오버를 따지다 warm을 고르고, warm이 요구하는 단일소유 fencing을 뭐로 할지(Cluster/etcd/Kafka그룹) 비교한 과정.
- **어떻게(서사·근거)**: Cluster의 layer(전송→Archive→Cluster)와 그것이 대체하는 것(링버퍼 코어) / "분산 ≠ 합의" 구분 / Kafka 그룹을 조정 전용으로 편승(파티션 직접 assign, external work sharding)은 문서화된 기법 / Kafka Streams standby가 완전 선례 / data plane=Aeron·control plane=Kafka 분리.
- **재료**: `_c5_kafka_group_coordination.html`, Aeron Cluster README·KIP-441, Kafka Streams standby.

### "정적 맵이 언제 깨지나 — cold vs warm 페일오버와 라우팅 맵"
- **훅·핵심 주장**: "라우팅 맵을 정적으로 고정한다"가 맞냐 틀리냐는 HA 선택에 달렸다. cold 페일오버(안정신원 재시작)면 맵이 안 바뀌어 정적으로 충분하지만, warm standby면 페일오버마다 주인이 바뀌어 맵이 동적이어야 한다. HA 축을 안 따지고 "고정"으로 닫은 게 실수였다.
- **context**: 라우팅 맵 유지 방법을 정하다 ConfigMap staggered 전파(split-brain)→코디네이터→Cluster까지 스파이럴한 뒤, "맵이 언제 바뀌나 = HA 선택"으로 수렴한 과정.
- **어떻게(서사·근거)**: ConfigMap은 원자적·동시 갱신 아님(kubelet ~2분) / 맵이 바뀌는 경우는 계획 리샤딩(오프라인)과 페일오버(warm일 때만) 둘 / cold=맵 정적, warm=맵 동적 / "고정" 반사는 HA 축을 안 따진 dismiss였다는 자기 교훈.
- **재료**: `_c5_routing_map.html`, ConfigMap 전파 지연 근거, cold/warm 트레이드오프.
