# ADR-013 주문 전달을 Kafka에서 Aeron으로

## 문제

v2 매칭 코어의 주문 입력을 인메모리 피더(테스트가 `publishPlace` 를 직접 호출)에서 실제 전달 계층으로 바꿔야 한다. v1 은 `API → Kafka(JSON) → 매칭` 이고, 주문 경로에 브로커·디스크·복제가 끼어 지연이 밀리초(ms)급이다. v2 주문 경로는 저지연이 필요하다(정산=돈 경로는 Kafka 를 유지).

## 대안

1. **Kafka 유지 + 바이너리 직렬화(Avro/Protobuf).** 인코딩만 바꾼다.
2. **Aeron(IPC/UDP) 전달 + 수동 바이너리 코덱**.
3. **Aeron + SBE(Simple Binary Encoding, 스키마 기반 코덱)**.

## 트레이드오프

- **Aeron vs Kafka**: Aeron 은 브로커·디스크가 없어 지연이 마이크로초(µs)급이지만 기본적으로 저장하지 않는다(휘발). Kafka 는 디스크+복제로 내구성을 주는 대신 ms 급. 속도 차이는 브로커·디스크·복제가 있는지에서 나온다. 인코딩(JSON vs 바이너리)의 몫은 그보다 작다. 그래서 대안 1(Kafka+바이너리)로는 지연이 크게 줄지 않는다.
- **수동 코덱 vs SBE**: 수동은 단순하고 바이트 흐름이 눈에 보이지만 스키마 진화에 취약하다(enum ordinal 의존, 필드 추가 시 양쪽 수동 수정). SBE 는 스키마 기반 zero-copy 로 정석이지만 codegen 빌드 단계가 붙는다.
- **IPC 먼저 vs UDP**: IPC(공유메모리)는 배선이 단순하나 같은 머신 한정. UDP 는 머신 경계를 넘지만 바이트 순서 고정 등 추가 처리가 필요하다.

## 결정

대안 2 를 택한다. Aeron 을 주문 전달 계층으로 도입하되, 코덱은 **수동 바이너리로 시작**(SBE 는 유예 — 흐름 학습이 우선)하고 통신은 **IPC 로 시작**(UDP 는 이후 단위). 내구성은 새로 만들지 않고 기존 journal 핸들러(Unit 2, journal replay 복구)가 책임진다. Kafka 는 지우지 않고 **정산(돈) 경로로 유지**한다.

## 결과

- `matching-disruptor` 에 Aeron 1.48 의존 추가. Aeron/Agrona 가 JDK 17 내부 클래스(`jdk.internal.misc.Unsafe`·`sun.nio.ch`)에 접근해, 테스트 JVM 에 `--add-opens` 두 개가 필요하다(main·JMH JVM 에서 돌릴 때도 동일 플래그 필요).
- `OrderCodec`(`JournaledOrder` ↔ 바이트, PLACE/CANCEL) · `AeronOrderReceiver`(폴링 스레드 → 디코딩 → `publishPlace`)로 인메모리 피더를 대체했다. 단일 폴링 스레드만 발행하므로 `ProducerType.SINGLE` 전제가 유지된다.
- 테스트 3종(IPC 왕복 · 코덱 라운드트립 · Aeron end-to-end 매칭) 추가, 모듈 10 테스트 통과.
- 저지연의 실측(µs 수준)은 하지 않았다 — 지연 벤치는 이후 단위 몫이고, 이 결정의 근거는 아키텍처(브로커·디스크 제거)다.
