# 📱 NPL 공동투자 플랫폼

> F-Lab 멘토링 프로젝트 - Java/Spring Boot 기반 NPL(부실채권) 공동투자 플랫폼

## 🎯 프로젝트 개요

개인 투자자들이 NPL(Non-Performing Loan, 부실채권)에 소액으로 공동 투자하여 수익을 추구하는 플랫폼입니다.
기관만 참여하던 NPL 시장에 개인들이 조각투자 방식으로 진입할 수 있는 서비스를 제공합니다.

### 핵심 가치
- 💰 **높은 수익률**: NPL 투자 평균 수익률 10~30%
- 🤝 **공동 투자**: 소액(10만원~)으로 분산 투자 가능
- 🔒 **법적 준수**: 자본시장법, 온투법 기준 준수
- ⚡ **고성능**: 대규모 동시성 제어 및 실시간 처리

---

## 📚 문서

### 1. [NPL 투자 개념 정리](docs/1_NPL_투자_개념정리.md)
- NPL이란 무엇인가?
- 투자 방법 및 수익 구조
- 장점과 리스크
- 시장 동향 (2024~2026)

### 2. [NPL 플랫폼 시나리오](docs/2_NPL플랫폼_시나리오.md)
- 서비스 개요 및 법적 구조
- 유저 시나리오 (성공/실패 케이스)
- 트래픽 특성
- 유통 시장 (2차 거래)

### 3. [딥다이브 가치 평가](docs/3_NPL프로젝트_딥다이브_가치평가.md)
- 다룰 수 있는 기술 주제
- 다른 프로젝트와 비교
- 프로젝트 스케일 옵션
- 기술 스택 검증

---

## 🏗️ 기술 스택 (예정)

### Backend
- **Language**: Java 21
- **Framework**: Spring Boot 3.x
- **Database**: PostgreSQL
- **Cache**: Redis
- **Message Queue**: (TBD)

### 주요 기술 주제
- ⚡ **동시성 제어**: Redis 분산 락, Optimistic/Pessimistic Lock
- 🔄 **트랜잭션 관리**: Saga Pattern, Event Sourcing
- 📊 **대량 데이터 처리**: Spring Batch, BigDecimal 정밀 연산
- 🎯 **실시간 처리**: WebSocket, SSE
- 🔐 **데이터 정합성**: Audit Trail, 이중 장부

---

## 📐 아키텍처 (예정)

### Phase 1: 청약 + 배당 정산
```
사용자 → 청약 → 입금 → 경매 낙찰 → 배당 정산
```

### Phase 2: 유통 시장 (Optional)
```
사용자 ↔ 지분 거래소 (실시간 호가, 매칭 엔진)
```

---

## 🚀 프로젝트 목표

### 백엔드 딥다이브 주제
1. **대규모 동시성 제어**
   - 49명 정원에 10만 명 동시 접근 처리
   - 상품 100개 동시 오픈 시 독립적 락 관리

2. **복잡한 금융 트랜잭션**
   - 투자 → 입금 → 경매 → 배당까지 긴 라이프사이클
   - Saga Pattern을 통한 보상 트랜잭션

3. **정밀한 정산 로직**
   - BigDecimal을 활용한 1원 단위 정확성
   - 수천 명 동시 배당 처리 (Spring Batch)

4. **금융 데이터 정합성**
   - Event Sourcing을 통한 감사 추적
   - 불변 이벤트 저장 및 시점 복원

---

## 📅 개발 일정 (예정)

- **Phase 1**: 기본 청약 시스템 (4주)
- **Phase 2**: 배당 정산 시스템 (3주)
- **Phase 3**: 유통 시장 (Optional, 4주)

---

## 👥 팀

- **멘티**: [박종혁](https://github.com/your-github)
- **멘토**: F-Lab 멘토
- **조직**: [F-Lab](https://github.com/f-lab-edu)

---

## 📝 License

This project is for educational purposes as part of F-Lab mentoring program.

---

## 🔗 참고 자료

- [F-Lab 공식 사이트](https://f-lab.kr)
- [F-Lab GitHub](https://github.com/f-lab-edu)
