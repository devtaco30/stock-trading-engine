# ADR-003: 비관적 락 획득 순서 — 데드락 방지

## 문제

매수/매도/체결 처리에서 Account와 Holding row에 비관적 락을 잡는다.
락 획득 순서가 일관되지 않으면 두 트랜잭션이 서로의 락을 기다리는 데드락이 발생할 수 있다.

예시 시나리오:
```
트랜잭션 1 (매수 체결): Account 락 획득 → Holding 락 대기
트랜잭션 2 (매도 주문): Holding 락 획득 → Account 락 대기
→ 교착 상태
```

## 결정

**락 획득 순서를 항상 Account → Holding 으로 고정한다.**

모든 트랜잭션이 동일한 순서로 락을 잡으면 순환 대기가 발생하지 않아 데드락이 원천 차단된다.

## 적용 기준

| 경로 | 락 순서 |
|---|---|
| 매수 주문 (`placeBuyOrder`) | Account → (Holding 없음) |
| 매도 주문 (`placeSellOrder`) | (Account 없음) → Holding |
| 매수 체결 (`fillBuyOrderPartially`) | Account → Holding |
| 매도 체결 (`fillSellOrderPartially`) | (Account 없음) → Holding |

매도 주문/체결은 Account를 건드리지 않아 현재는 안전하다.
단, 향후 매도 시 Account 접근이 추가될 경우 반드시 Account → Holding 순서를 지켜야 한다.

## 규칙

- Account와 Holding을 모두 락 잡아야 하는 경우 **반드시 Account 먼저**
- 새로운 트랜잭션 경로 추가 시 이 순서를 코드 리뷰에서 확인
- 순서가 바뀌는 변경은 데드락 위험 신호로 간주
