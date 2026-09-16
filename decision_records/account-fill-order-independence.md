---
feature: account-fill-order-independence
date: 2026-09-16
branch: fix/account-fill-replay-multi-source
commits: [8f4e7d5]
feeds: [adr, blog]
---

# 계좌 체결 반영은 적용 순서와 무관하다 (I1 U0)

## ADR 네타
### 여러 recording을 순서 보장 없이 되읽어도 안전한 근거

- **context(무슨 상황)**: 계좌 워커는 매칭에서 오는 체결을 받는 즉시 자기 Aeron Archive에
  REMOTE로 녹화한다(`AccountFillIntakeConfig:71`). Archive는 연결(publication image)마다
  recording을 따로 만드는데, 매칭 프로세스가 둘 이상이면 recording도 둘 이상 생긴다.
  기존 `AccountFillReplayer.findRecording()`은 그중 마지막 하나만 읽어, 나머지 recording에
  든 체결이 재기동 복구에서 빠지는 문제가 있다(I1). 이 flush는 그 수정(U1~U3) 착수 전에
  전제 하나를 검증한 U0의 기록이다.

- **왜(문제·동기)**: 여러 recording을 전부 읽게 고치면, recording들이 원래 어떤 순서로
  도착했는지는 복원할 수 없다(Archive는 recording 단위 시작·정지 시각만 가질 뿐, 서로 다른
  recording에 든 체결들 사이의 도착 순서는 남지 않는다). 그래서 "체결을 어떤 순서로 적용해도
  최종 잔고·보유·미수금이 같다"는 성질이 반드시 성립해야 U1~U3의 설계(순서를 신경 쓰지 않고
  전부 읽기)가 안전하다. 이 성질은 `AccountState.applyBuyFill`의 텔레스코핑 주석에만 근거로
  적혀 있었고 실제로 검증하는 테스트는 없었다.

- **어떻게(대안·결정·트레이드오프)**: `AccountState`에 대한 새 단위테스트
  `AccountFillOrderIndependenceTest`를 작성해, 매수 부분체결 3건(수량 2/5/3, 합계 10)을
  도착순·역순·뒤섞은 순서로 각각 별개의 `AccountState` 인스턴스에 적용하고 결과를 비교했다.
  가격 101·증거금률 0.45로 골랐는데, `101 × 10 × 0.45 = 454.5`라 매 부분체결마다
  `reservedAmount`의 반올림(UP)이 실제로 끼어드는 조합이기 때문이다 — 반올림이 안 끼는 조합
  (예: 딱 나누어떨어지는 수)으로 검증하면 순서 무관이 우연히 성립하는 것과 실제로 성립하는 것을
  구분하지 못한다. 매도는 보유 수량만 다루고 반올림이 없어 한 케이스(도착순 vs 뒤섞음)만
  추가했다.

- **무엇을(실제 변경·파일·커밋)**: 프로덕션 코드 변경 없음. 테스트 파일 1개 추가 —
  `account-disruptor/src/test/java/.../domain/AccountFillOrderIndependenceTest.java`(커밋
  `8f4e7d5`). `./gradlew :account-disruptor:test --tests AccountFillOrderIndependenceTest
  --rerun-tasks`로 직접 실행해 2개 테스트 모두 PASSED 확인.

- **결과·수치**: 매수 3케이스(도착순·역순·뒤섞음) 모두 잔고·보유·미수금이 원 단위까지 동일,
  세 순서 모두 마지막 체결에서 예약 잔량이 0이 되어 예약 엔트리가 제거되는 경로를 밟았다(장부에서
  지워짐 확인 = `reservedMargin()` 0). 매도 2케이스도 보유·매도예약잔량 동일. 성립 이유는
  코드상 `applyBuyFill`이 매 호출마다 `reservedBefore - reservedAfter`(그 순간의 남은 예약액
  기준)만큼만 증거금을 떼는 구조라, 여러 번의 차감을 합하면 시작 예약액과 끝 예약액(0)의 차이로
  텔레스코핑되어 중간에 어떤 순서로 나눠 뗐는지가 합계에 영향을 주지 않기 때문이다. 매칭
  프로세스가 하나뿐이던 지금까지는 recording도 하나뿐이라 이 문제 자체가 드러나지 않았다.

## 블로그 네타
### "체결 반영 순서를 왜 신경 쓰지 않아도 되는가 — 텔레스코핑과 멱등키로 순서를 지우는 설계"

- **훅·핵심 주장**: 분산 시스템에서 "정확한 도착 순서를 복원해야 한다"는 요구는 종종
  설계를 어렵게 만드는 가정 자체가 틀렸을 수 있다 — 계좌 체결 반영은 순서 대신 최종 상태만
  맞으면 되는 연산이라, 애초에 순서를 복원할 필요가 없었다.

- **context**: 계좌 워커가 매칭 프로세스마다 별도 Aeron recording으로 체결을 받는데,
  재기동 복구가 그중 하나만 읽던 버그(I1)를 고치려면 여러 recording을 순서 없이 전부 읽어야
  했다. "순서를 못 지키는데 괜찮은가"라는 질문에 코드를 읽고 답을 검증한 과정.

- **어떻게(서사·근거)**: ①왜 recording이 여러 개로 나뉘는지(Archive는 연결 단위로 recording을
  만든다) ②왜 순서를 복원할 수 없는지(recording은 시작·정지 시각만 가짐) ③그런데도 안전한
  이유 셋 — 잔량 차감은 텔레스코핑(중간 분배 순서 무관), tradeId 멱등(중복 재도착 방어),
  예약은 저널 replay가 항상 먼저 끝나 있음(체결이 예약보다 먼저 올 수 없음) ④이 가정을
  "주석"이 아니라 "테스트"로 고정한 이유 — 반올림 같은 비선형 요소가 끼면 이론과 실제가
  갈릴 수 있어서, 일부러 딱 안 떨어지는 숫자로 검증했다.

- **재료(커밋·도식·수치)**: 커밋 `8f4e7d5`. `AccountState.applyBuyFill`의 텔레스코핑 주석
  (파일 내 "이 체결로 예약이 얼마나 줄어드는지를 먼저 스냅샷 찍는다" 부분)을 코드 인용으로
  쓸 수 있다. 수치는 위 ADR 네타의 "결과·수치" 절 그대로(가격 101·증거금률 0.45·수량
  2/5/3).
