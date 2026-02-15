# PostgreSQL 선정 근거

이 문서는 주식 거래 엔진 프로젝트에서 **운영 DB로 PostgreSQL을 쓰는 것이 합당한 이유**를 정리한다.

---

## 1. 프로젝트가 요구하는 DB 특성

이 엔진의 도메인과 문서에서 도출한 요구사항이다.

| 요구사항 | 근거 |
|----------|------|
| **금액 정합성** | README: "금액 정합성 (BigDecimal 1원 단위)". 잔액·증거금·미수금이 1원 단위로 정확해야 함. |
| **ACID** | 주문 접수 → 체결 → Holding/Unpaid 생성, 정산 배치에서 계좌·주문·미결제를 한 트랜잭션으로 갱신. 중간 실패 시 전부 롤백되어야 함. |
| **관계형 모델** | Account–User, Account–Order, Order–Unpaid, Account–Holding 등 FK 관계가 명확하고, JPA 엔티티 설계가 RDB 전제. |
| **정밀 숫자 타입** | 엔티티에서 금액 필드에 `precision`/`scale` 명시 (예: `balance` 19자리 0소수, `heldMargin` 15자리 0소수). 부동소수점 사용 시 오차 불가. |
| **동시성** | 여러 계좌의 주문·체결·정산이 동시에 일어나므로, 읽기/쓰기 동시에 안전한 동시성 제어 필요. |
| **배치** | 결제일 배치(03:00), 강제청산 배치(05:00) 등 대량 읽기·갱신. RDB 트랜잭션·락 시맨틱이 필요. |

따라서 **RDB 중에서 ACID·정밀 숫자·동시성**을 일관되게 보장하는 DB가 적합하다.

---

## 2. ACID 보장

- **PostgreSQL**: 스토리지 엔진/설정과 무관하게 **모든 구성에서 ACID**를 보장한다.  
  → 증권 계좌·정산처럼 트랜잭션 경계가 중요한 도메인에서, “설정 실수로 ACID가 깨질 여지”를 줄일 수 있다.

- **MySQL**: **InnoDB, NDB Cluster** 등 특정 스토리지 엔진을 쓸 때만 ACID compliant하다는 설명이 공식 비교 문서에 있다.  
  (참고: [AWS – The difference between MySQL and PostgreSQL](https://aws.amazon.com/compare/the-difference-between-mysql-vs-postgresql/))

금융·정산에 가까운 이 프로젝트에서는 “어떤 엔진을 쓰든” ACID가 보장되는 쪽이 유리하다.

---

## 3. 금액·정밀 계산 (NUMERIC)

- **PostgreSQL 공식 문서** (8.1. Numeric Types)에서는:
  - `numeric` 타입을 **“monetary amounts and other quantities where exactness is required”** 에 특히 권장한다고 명시하고,
  - **“If you require exact storage and calculations (such as for monetary amounts), use the numeric type instead.”** (부동소수점 대신)라고 적고 있다.  
  (참고: [PostgreSQL – Numeric Types](https://www.postgresql.org/docs/current/datatype-numeric.html))

- 이 프로젝트는 잔액·증거금·미수금·주문가를 **BigDecimal + precision/scale** 로 1원 단위 정확도를 요구하므로, DB에서도 **정확한 고정소수/정수 연산**이 필요하다. PostgreSQL의 NUMERIC(DECIMAL)과 그 문법이 이 요구와 맞다.

---

## 4. 동시성 (MVCC)

- **PostgreSQL**은 **MVCC(Multiversion Concurrency Control)** 를 기본으로 사용해, 여러 트랜잭션이 동시에 읽기·쓰기를 해도 일관된 스냅샷을 보장한다.  
  → 주문·체결·정산이 한 DB에서 동시에 일어나는 이 엔진의 접근 패턴과 맞다.

- **MySQL**도 InnoDB에서 MVCC를 지원하지만, “ACID/MVCC가 모든 구성에서 동일하게 적용되는가”는 PostgreSQL 쪽이 비교 문서에서 더 단순하게 설명된다.

금융성 트랜잭션에서 **예측 가능한 격리 수준·동시성**이 중요하므로, MVCC를 기본으로 하고 문서화가 명확한 DB가 유리하다.

---

## 5. 정리

| 기준 | PostgreSQL이 이 프로젝트에 맞는 이유 |
|------|--------------------------------------|
| ACID | 구성과 무관하게 ACID 보장 → 정산·주문·계좌 갱신의 원자성 보장에 유리. |
| 금액 정합성 | 공식 문서에서 금액·정확한 계산에 NUMERIC 권장 → 1원 단위 정합성 요구와 일치. |
| 동시성 | MVCC 기본 제공 → 다수 주문·체결·배치의 동시 처리에 적합. |
| 관계형·트랜잭션 | RDB + JPA 설계와 맞고, 복수 테이블 걸친 트랜잭션·배치 처리에 적합. |

**결론**: 증권 계좌·T+2 정산·미수금·강제 청산을 다루는 이 엔진의 요구사항(금액 정합성, ACID, 정밀 숫자, 동시성)에 비추어 **운영 DB로 PostgreSQL을 쓰는 것이 합당하다.**
