## 문제

v2 매칭 코어의 **내구성·복제·페일오버**를 무엇으로 얻을 것인가. 그리고 그 내구성의 **저널·복구 배관을 직접 만들 것인가, 검증된 것을 쓸 것인가.**

실제 저지연 거래소(Coinbase 등)가 쓰는 **Aeron Cluster**(Raft 기반 복제 상태머신 + Aeron Archive)를 검토했다. 이걸 쓰면 페일오버·스냅샷 라이프사이클·durable 복제 로그를 프레임워크가 준다.

전제 둘:
- **Disruptor는 v2의 학습 핵심**이다. v2는 "LMAX식 인메모리 저지연 매칭을 직접 구현하며 링버퍼·mechanical sympathy·lock-free를 이해한다"가 목표다.
- 반대로 **저널의 파일 I/O·리플레이 배관 자체는 "주식 트레이딩 시스템 구축"의 부가 요소**다 — 관심사는 복구 흐름(저널→리플레이→호가창 재구성)이지 로그 파일 포맷이 아니다.

계좌 축·주변부는 이미 Kafka로 가기로 했다(ADR-018).

## 대안

Aeron은 세 층(기본 전송 / Archive / Cluster)으로 나뉜다. 내구성을 어느 층에서 얻느냐로 셋을 놓고 본다.

1. **Aeron Cluster (Raft 복제 상태머신) + Archive** — 매칭을 `ClusteredService`로 감싸고 순서·저널·복제·페일오버·스냅샷을 클러스터가 관리. 개발자는 상태 직렬화(`onTakeSnapshot`/`onStart`)만.
2. **Disruptor(자체) + 자체 파일 저널(직접 구현) + Aeron 기본 전송** — 저널 파일 포맷·flush·replay를 손으로 짬.
3. **Disruptor(자체) + Aeron Archive(저널·리플레이) + 자체 스냅샷 + Aeron 기본 전송** — 저널 배관은 Archive(검증됨)에 맡기고, 호가창 스냅샷만 직접. Cluster는 안 씀.

## 트레이드오프

### Cluster를 안 쓰는 이유 (대안 1 기각)
- **Cluster는 Disruptor의 역할을 흡수한다.** Raft 복제 로그가 "순서 보장 + 저널 게이팅"을 대신하고 `onSessionMessage`로 시퀀싱된 입력을 넘긴다 → 우리가 만든 Disruptor 매칭의 순서·저널 부분이 클러스터로 넘어가 **v2 학습 핵심(Disruptor)이 잠식**된다.
- **복제 상태머신·내구성은 Kafka가 이미 제공한다.** Kafka는 내구성 있는 복제 순서 로그이고, 상태머신이 그 로그를 리플레이해 재구성·페일오버하는 것이 로그 기반 상태머신 복제다(Kafka Streams changelog와 같은 방식). Aeron Cluster의 Raft가 주는 것을 계좌 축은 Kafka로 이미 얻는다 → 중복.
- **Cluster의 스냅샷만 떼어 쓸 수 없다(검증됨).** 스냅샷은 `ClusteredService`(onTakeSnapshot/onStart)에 묶여 있어 단독 API가 없다("you must adopt the entire service programming model"). 스냅샷만 빌리려 해도 Raft·시퀀싱 모델이 통째로 딸려와 위 잠식이 재발한다.

### Archive는 쓰는 이유 (대안 3 > 대안 2)
- **Archive는 Cluster와 다르다 — 매칭 로직을 안 건드린다.** Archive는 스트림을 디스크에 durable 기록하고 **position부터 replay**하는 "기록 층"일 뿐이다(검증됨: record / replay-from-position / truncate / replicate 제공, 2025년 현역). 매칭 코어(Disruptor)는 그대로 두고 저널링 배관만 offload한다. 그래서 Cluster 기각 논리(Disruptor 잠식)가 Archive엔 **적용되지 않는다.**
- **공수 절감.** 주문은 어차피 Aeron으로 매칭에 들어온다(`AeronOrderReceiver`). 그 입력 스트림을 Archive에 record하면 파일 포맷·flush·offset·replay 로직을 직접 안 짜도 된다. 관심사(복구 흐름)는 남고 부가 배관만 준다.
- **인프라 증분이 작다.** C5에서 실 UDP 전송에 MediaDriver가 어차피 필요하다. Archive는 ArchivingMediaDriver로 그 위에 얹는 증분이다.

### 대안 3에서 우리가 직접 짜는 몫
- **스냅샷은 Archive가 안 준다(검증됨) — 우리가 만든다.** Archive는 스트림 record/replay만이고 앱 상태 스냅샷은 범위 밖이다. 표준 패턴 = 주기적으로 호가창 상태를 직렬화 + 그 시점 Archive position 기록 → 복구 시 스냅샷 로드 후 그 position부터 replay. 이 스냅샷은 도메인(내 TreeMap/Deque 호가창)이라 offload할 성질도 아니고, 복구 학습에서 오히려 남기고 싶은 부분이다.

## 결정

**대안 3을 택한다. Cluster는 안 쓰고, 매칭 저널은 Aeron Archive로, 스냅샷은 직접 만든다.**

- **매칭 = Disruptor(핵심) + Aeron Archive(저널·리플레이 배관) + 자체 스냅샷(호가창 덤프 + Archive position).** 단일 노드. 복구 = 스냅샷 로드 → Archive를 그 position부터 replay → 호가창 재구성.
- **Aeron의 역할 = 기본 전송(µs) + Archive(내구 기록·리플레이).** Cluster(Raft)는 안 쓴다.
- **계좌 = Kafka를 내구성 복제 로그로.** 명령 로그(account-fills·예약) 리플레이로 재구성, 페일오버는 컨슈머 그룹 + 리플레이.
- **주변부(정산·조회) = Kafka.**

근거 세 줄: (1) Cluster는 Disruptor 순서·저널 역할을 흡수해 학습 핵심을 잠식하고, 스냅샷만 떼어 쓸 수도 없다(검증됨). (2) 저널 파일 I/O·replay 배관은 부가 요소라 검증된 Archive에 맡기고, 관심사인 복구 흐름·스냅샷만 직접 만든다. (3) Archive는 Cluster와 달리 매칭 로직을 안 건드려 학습 핵심을 지킨다.

## 결과

- Disruptor가 v2 핵심으로 유지된다. Aeron = 전송 + Archive(저널). 스냅샷·복구 흐름은 직접 구현 = 학습으로 남는다. 내구성 배관은 Archive(매칭) / Kafka(계좌).
- 매칭 HA는 "단일 노드 + Archive 리플레이 + 자체 스냅샷 복구"(C6). 핫스탠바이가 필요해지면 LMAX식 복제 핸들러(백업 노드에 입력 복제)를 손으로 붙이는 확장 과제로 남긴다 — Aeron Cluster 없이 가능.
- Cluster를 안 쓴다고 개념을 안 배우는 건 아니다. "왜 실제 거래소는 Cluster를 쓰는데 우리는 Archive + 자체 스냅샷으로 가는가"의 트레이드오프가 학습·서술 자산이다.

### 검증 (2026-09-08, Aeron 공식 wiki)
- **Aeron Archive** = durable record + replay-from-position("Replays are requested from a position and for a length"), 현역(2025-01 최종수정), 앱 상태 스냅샷은 범위 밖. → 저널·리플레이 배관으로 채택.
- **Aeron Cluster 스냅샷** = ClusteredService(onTakeSnapshot/onStart)에 묶임, 단독 사용 불가; Raft가 입력을 시퀀싱. → 스냅샷만 차용 불가 확인, Cluster 기각 유지.

### 미검증 / 리스크
- Archive 리플레이 기반 매칭 복구 시간(스냅샷 주기에 좌우) — 미측정.
- 계좌 상태의 Kafka 리플레이 복구 시간(로그 길이에 좌우) — 미측정.
- ArchivingMediaDriver 구성·운영 복잡도(embedded vs 별도 프로세스) — 미착수(C6).

### 참고 (리서치 2026-09-08)
- Aeron Archive: https://github.com/real-logic/aeron/wiki/Aeron-Archive
- Aeron Cluster Tutorial: https://github.com/real-logic/aeron/wiki/Cluster-Tutorial
- Coinbase 매칭 엔진 Aeron Cluster 사례: https://aeron.io/case-studies/coinbase-cloudnative-crypto-exchange-aeron-cluster/
- LMAX 아키텍처(Disruptor 파이프라인·복제): https://martinfowler.com/articles/lmax.html
