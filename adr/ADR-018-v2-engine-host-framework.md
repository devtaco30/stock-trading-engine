## 문제

v2 매매 엔진은 `api → order-engine → [Aeron] → matching-engine → [Kafka] ↔ settlement` 파이프라인이고, `order-engine`·`matching-engine`·`settlement`은 각각 별개 프로세스(앱)로 Docker 컨테이너로 스케일아웃한다. 계좌는 accountId, 매칭은 stockCode로 샤딩하며 각 샤드는 single-writer다. 매칭·계좌 코어는 이미 프레임워크 0 라이브러리(`matching-disruptor`·`account-disruptor`)로 확정돼 있다(ADR-014). 남은 결정은 **그 코어를 실행하는 각 엔진 호스트 앱을 무엇으로 만드느냐**다.

이 결정에는 두 가지 실요구가 걸려 있다:

- **Kafka 컨슈머 그룹 기반 스케일아웃 + 리밸런스 상태 핸드오프.** 파티션(=계좌 묶음)이 인스턴스 간 재배정될 때, 잃는 인스턴스는 그 계좌들의 인메모리 상태를 스냅샷 후 쓰기를 멈추고, 얻는 인스턴스는 로드한 뒤 처리해야 한다(single-writer 무겹침).
- **양방향 Kafka.** matching→order-engine(체결 반영)·matching→settlement(줄 것), settlement→order-engine(T+2 정산 결과, 받을 것).

ADR-017은 이 문제를 "격리된 `account-disruptor` 하나를 혼자 띄우는 진입점"으로 좁게 잡아, plain `main()` + raw kafka(`Properties`) + 하드코딩 시드로 결정했다. 그러나 실제 목표가 위 샤딩 다중 프로세스라면 요구가 달라지고 결론도 달라진다. 이 ADR은 그 범위에서 재검토한다.

## 대안

1. **raw kafka-clients + plain `main()`** (ADR-017이 택한 것) — `Properties`로 컨슈머 구성, `CountDownLatch` 대기, `addShutdownHook` 종료.
2. **Spring Boot + Spring Kafka.** 각 엔진을 Spring Boot 앱으로, Kafka 배선은 Spring Kafka로.
3. **Quarkus + SmallRye Reactive Messaging.** 빌드타임 DI·네이티브 지향.

세 경우 모두 disruptor 매칭 루프(µs)와 Aeron 수신은 프레임워크 밖 손 배선으로 두고, 코어는 라이브러리로 사용하는 전제는 같다. 프레임워크 선택은 가장자리(Kafka 배선·기동·설정·운영)에만 영향을 준다.

## 트레이드오프

리서치로 확인한 사실(2026-09-08, 출처는 아래):

| 기준 | 1) raw + plain main | 2) Spring Boot + Spring Kafka | 3) Quarkus + SmallRye |
|---|---|---|---|
| 네이티브 이미지 (Aeron/Disruptor 호환) | 무관(안 씀) | AOT 지원(Spring도 됨), 다만 미채택 | **막힘**: MediaDriver 크래시(open)·Disruptor `Unsafe`, e2e 성공 사례 미확인 |
| 기동·풋프린트 (24/7 엔진 유효성) | 가장 가벼움 | 무겁지만 24/7엔 실익 작음 | 가벼운 편(수치 신뢰도 낮음), 24/7엔 실익 작음 |
| 리밸런스 상태 핸드오프 제어 | 다 되나 전부 손코딩 | **가장 자연스러움**: 커밋 전/후 2단 훅 | 됨, 리액티브 모델과 동기 배리어 조율 부담 |
| Kafka 운영 안전망(커밋·재시도·헬스·메트릭) | 전부 재구현 | 성숙·1급 | 있으나 명령형 제어엔 우회 |
| v1 생태계 일관성 | 없음 | v1이 이미 Spring Boot | 새 스택 |
| 실무 적합성 | 낮음(컨슈머마다 손코딩 안 함) | 높음 | 중간 |
| 학습 | 밑바닥 보임 | 실무 표준 | 빌드타임 DI·네이티브(단 Aeron 리스크) |

핵심 논거:

- **네이티브가 Quarkus의 최대 명분인데 이 프로젝트에선 세 겹으로 무력하다.** (1) Aeron MediaDriver의 네이티브 컴파일 크래시 이슈(oracle/graal#8418)가 열려 있고 Disruptor는 `sun.misc.Unsafe` 정적 초기화로 GraalVM 자동 대체가 깨질 수 있다. Aeron·Agrona·Disruptor 모두 GraalVM 검증 목록에 없고, 이 셋을 포함한 end-to-end 네이티브 빌드 성공 사례를 찾지 못했다. (2) Spring Boot 3+도 AOT로 네이티브를 1급 지원하므로 "네이티브 = Quarkus 전유물"은 2026 기준 틀리다. (3) 기동·풋프린트 이득은 서버리스 콜드스타트·pod 밀도 얘기라 24/7로 도는 매칭 엔진엔 실익이 작다.
- **리밸런스 상태 핸드오프는 이 설계에서 가장 어려운 부분인데 Spring Kafka가 여기에 가장 잘 맞는다.** `ConsumerAwareRebalanceListener`가 `onPartitionsRevokedBeforeCommit` / `AfterCommit` **2단 훅**을 제공해 "인메모리 상태 스냅샷 → 오프셋 커밋 → 소유권 이전" 순서를 코드로 명확히 표현한다. 콜백이 `Consumer`를 직접 넘겨줘 `commitSync`/`seek`를 바로 부를 수 있고, 정적 멤버십(`group.instance.id`)을 컨테이너가 인지한다. SmallRye는 단일 `onPartitionsRevoked`만 있고 리액티브 파이프라인이라, 리밸런스 콜백의 동기 배리어 전에 인플라이트 처리를 비웠음을 보장하는 코드를 직접 조율해야 한다.
- **필요한 Kafka 제어(수동 커밋·정적 멤버십·cooperative-sticky·concurrency=1)는 두 프레임워크 다 프레임워크 안에서 된다** — raw kafka-clients로 내려갈 필요 없다. 즉 "세밀 제어 때문에 raw" 논리는 성립하지 않는다.

### 페일오버·복구 시간에서 빠른 부팅의 값 (별도 검토)

**인스턴스가 죽어 새로 띄울 때(페일오버)** 나 **부하 급증에 새 인스턴스를 투입할 때(탄력 확장)** 부팅 속도가 복구 시간(MTTR)에 직접 들어간다. 정상 운영 중에는 들어가지 않는다. 이 관점에서 네이티브 초고속 부팅은 매력적이다. 그러나 이 시스템에 한해선 세 가지가 그 값을 깎는다:

- **복구 시간의 병목은 상태 재구성이다.** 새 인스턴스가 샤드를 넘겨받으면 인메모리 상태(매칭=호가창, order-engine=계좌 예약·잔고)를 스냅샷 + journal replay로 다시 만들어야 한다. 프레임워크 부팅은 JVM 수 초 vs 네이티브 ~50ms 차이지만, 상태 재구성은 계좌·주문 볼륨에 따라 수십 초가 될 수 있어 부팅 최적화는 복구 시간의 일부만 줄인다.
- **그 네이티브 부팅은 매칭/order 엔진엔 애초에 못 쓴다** — Aeron/Disruptor가 막아서(프레임워크 무관). "페일오버 위해 네이티브" 카드가 이 두 엔진엔 열리지 않는다.
- **빠른 복구의 실제 레버는 부팅 속도가 아니다**: (1) 살아있는 인스턴스로 컨슈머 그룹 리밸런스 — 여유 용량만 있으면 새 부팅 없이 남은 인스턴스가 파티션을 넘겨받음(단 정적 멤버십은 죽은 멤버 복귀를 `session.timeout.ms`만큼 기다린 뒤 재배정 → 안정성↔페일오버 속도 트레이드오프), (2) 웜 스탠바이(미리 떠 있는 예비 인스턴스)로 콜드 부팅 회피, (3) 빠른 상태 재구성(스냅샷 주기·journal 크기 관리). 프레임워크 부팅 속도는 부차적이다.

결론: 페일오버·복구 시간은 실재하는 고려다. 다만 이 엔진에서 그 시간을 좌우하는 것은 리밸런스·웜 스탠바이·상태 재구성이고, 네이티브 부팅이 줄이는 몫은 작다. 이 관점도 Quarkus/네이티브를 결정적으로 밀어주지 못한다.

## 결정

**대안 2(Spring Boot + Spring Kafka)를 택한다. ADR-017의 결정(`account-disruptor` 안 plain `main()` + raw kafka)을 대체한다.**

- 각 v2 엔진(`order-engine`·`matching-engine`·`settlement`)은 **별도 Spring Boot 앱(새 모듈)**이다. v1 앱은 건드리지 않는다(v1 무손상).
- **코어는 프레임워크 0 라이브러리로 유지하고, `account-disruptor`·`matching-disruptor` 안에 `main()`·Kafka 배선을 넣지 않는다.** 실행 진입점과 Kafka는 이들을 의존으로 감싸는 Spring Boot 호스트 앱에 둔다. (ADR-017은 코어 모듈 안에 `main()`을 넣었는데, 이는 "격리 코어 혼자 띄우기"라는 좁은 범위에서 나온 것이다.)
- Kafka 다리는 Spring Kafka로 배선한다: `ConsumerAwareRebalanceListener`(커밋 전/후 2단 훅)로 상태 핸드오프, `AckMode.MANUAL_IMMEDIATE` 수동 커밋, `group.instance.id` 정적 멤버십, `CooperativeStickyAssignor`, `concurrency=1`.
- **Aeron 수신·disruptor 매칭 루프는 Spring 밖에서 손으로 배선하고 코어는 앱이 의존하는 라이브러리로 둔다** — µs 지연 코어라 스프링이 관여할 자리가 아니다.
- **네이티브 이미지는 채택하지 않는다.**

**raw(대안 1) 기각**: 실무에서 컨슈머마다 poll·오프셋·리밸런스·재시도·헬스를 손으로 짜지 않는다. 리밸런스 훅 자체는 raw에도 있으나, 커밋 안전망·설정·운영 기능을 전부 재구현하는 비용이 학습 이득을 넘어선다.

**Quarkus(대안 3) 기각**: 네이티브가 Aeron/Disruptor로 막히고·Spring도 되고·24/7엔 실익이 작으며(위 세 겹), 리밸런스 상태 핸드오프 같은 명령형 제어에 SmallRye의 리액티브 모델이 조율 부담을 더한다. 남는 우위(풋프린트)는 수치 신뢰도가 낮고 위 항목들을 넘지 못한다.

## 결과

- 각 엔진이 Spring Boot 앱으로 뜨고, 코어는 그 안에 순수 라이브러리로 얹힌다. 코어의 프레임워크 0 성격은 유지된다.
- ADR-017의 하드코딩 시드·단일 인스턴스 전제는 폐기된다 — 계좌는 accountId 파티션 키로 샤딩되고 시드도 샤드별로만 넣는다.
- **리밸런스 seamless 핸드오프는 계좌 상태 durable store(스냅샷+journal)가 전제**이며 아직 없다(C6에 계좌 축 포함으로 확정). 그 전까지 스케일 변경은 "의도적 리샤딩(잠깐 멈춤)"으로 낮춰 잡는다.
- Aeron 다리(order-engine→matching)는 Kafka와 달리 컨슈머 그룹이 없어 stockCode→컨테이너 라우팅 맵을 보내는 쪽이 직접 관리해야 한다(별도 설계).
- **페일오버·복구 전략은 리밸런스·웜 스탠바이·상태 재구성으로 잡는다**(위 별도 검토 참조). 부팅 최적화가 필요하면 JVM 모드의 Spring AOT + CDS(Spring Boot 3.3+)로 소폭 당기되, Aeron/Disruptor를 쓰는 매칭·order 엔진은 네이티브 이미지를 시도하지 않는다.
- 정적 멤버십 `session.timeout.ms`는 "재시작 안정성 ↔ 페일오버 속도"를 가르는 파라미터로, 스펙에서 명시적으로 정한다.

### 미검증 / 리스크

- Quarkus·Spring Boot의 RSS·기동 절대 수치: 벤치마다 상충(한쪽은 Quarkus 우위, 다른 쪽은 역전), 방법론 공개된 자체 벤치 전에는 미검증.
- SmallRye의 `CooperativeStickyAssignor` 명시 지원: 공식 문서에서 확인 못함(설정 pass-through로 전달은 가능).
- 최신 버전 문자열(Quarkus 3.39.1 / Spring Boot 4.1.1): 릴리스 추적 블로그 기준, 공식 릴리스 페이지 최종 대조는 부분 미검증.

### 재검토 트리거

- 서버리스·elastic scaling(잦은 콜드스타트, pod 밀도)이 실제 요구가 되면 네이티브·Quarkus를 재론한다.

### 출처

- GraalVM 네이티브 검증 라이브러리 목록: https://www.graalvm.org/native-image/libraries-and-frameworks/
- Aeron MediaDriver 네이티브 크래시(open): https://github.com/oracle/graal/issues/8418
- Disruptor `Unsafe` 정적 초기화 대체 문제: https://github.com/LMAX-Exchange/disruptor/issues/345 , https://github.com/elastic/apm-agent-java/issues/4312
- Spring Boot 네이티브(AOT) 공식 지원: https://docs.spring.io/spring-boot/reference/packaging/native-image/index.html
- Spring Kafka 리밸런스 리스너(2단 훅): https://docs.spring.io/spring-kafka/reference/kafka/receiving-messages/rebalance-listeners.html
- Spring Kafka 수동 커밋·정적 멤버십·concurrency: https://docs.spring.io/spring-kafka/reference/kafka/receiving-messages/message-listener-container.html
- SmallRye Kafka 수동 커밋·리밸런스 리스너: https://smallrye.io/smallrye-reactive-messaging/4.14.0/kafka/receiving-kafka-records/ , https://smallrye.io/smallrye-reactive-messaging/4.1.1/kafka/consumer-rebalance-listener/
