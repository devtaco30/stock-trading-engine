---
feature: shutdown-timeout
date: 2026-09-14
branch: main
commits: [8908d3a, 90a65f5]
feeds: [adr, blog]
---

# Disruptor.shutdown() 무한 대기 차단 — 타임아웃+halt, 그리고 죽은 소비자를 어디서 죽여야 재현되나

## ADR 네타

### ADR: 소비자 fail-fast 시 Disruptor.shutdown() 무한 대기 방지 (타임아웃+halt)
- **context(무슨 상황)**: 계좌/매칭 엔진은 LMAX Disruptor 위에 있고, 예상 못한 예외는 fail-fast(`AccountExceptionHandler`·`MatchingExceptionHandler`가 rethrow)로 소비자를 멈춘다. Spring 컨텍스트 종료 시 `AccountEngine.shutdown()`이 항상 불리는데, 내부가 타임아웃 없는 `disruptor.shutdown()`이었다.
- **왜(문제·동기)**: 소비자 스레드가 죽으면 `disruptor.shutdown()`이 `while(hasBacklog())` 스핀에서 영원히 멈춘다(죽은 스레드 시퀀스가 다시 안 올라옴). 실제로 2b-1b 저널 fail-fast 추가 후 이 경로가 닿기 쉬워졌고, 테스트에서 shutdown이 14~24분 걸린 사례. 로컬/단독 컨테이너에선 재시작조차 못 하고 멈춘 채 남는다.
- **어떻게(대안·결정·트레이드오프)**: `disruptor.shutdown(5, TimeUnit.SECONDS)`로 호출하고 `TimeoutException`이 나면 `disruptor.halt()`로 강제 정지. 정책(Jack 확정): 타임아웃 5초 / halt만 하고 정상 반환(예외 밖으로 안 던짐, `System.exit` 안 씀 — 프로세스 종료는 호스트[K8s liveness] 몫). 코어는 프레임워크-0 라이브러리 유지. 두 엔진 대칭.
- **무엇을(실제 변경·파일·커밋)**: `AccountEngine.shutdown()`·`MatchingEngine.shutdown()`에 `SHUTDOWN_TIMEOUT_SECONDS=5` 상수 + try/catch(TimeoutException→halt). 커밋 `8908d3a`(워커 구현), main 머지 `90a65f5`. Disruptor 4.0.0의 `shutdown(long,TimeUnit) throws com.lmax.disruptor.TimeoutException`·`halt()` javap로 실측 확인.
- **결과·수치**: 죽은 소비자 앞 shutdown이 5초 안에 반환(타임아웃→halt). RED(pre-fix)에서 8초 await 초과로 실패, GREEN에서 통과 — 직접 실행 확인.

## 블로그 네타

### "죽은 소비자를 어디서 죽여야 Disruptor.shutdown()이 멈추나 — hasBacklog의 함정"
- **훅·핵심 주장**: "소비자가 죽으면 shutdown이 무한 대기한다"는 맞지만, **아무 소비자나 죽인다고 재현되지 않는다.** 체인의 마지막 핸들러를 죽이면 오히려 shutdown이 바로 반환된다. 저널(앞단) 소비자를 죽여야 뒷 핸들러가 영원히 블록돼 hang이 재현된다.
- **context**: shutdown-hang 수정의 RED 테스트를 짜다, LLD대로 "매칭/체결 리스너를 죽이는" 시나리오로는 수정 전에도 테스트가 통과(hang 재현 안 됨)하는 걸 발견. Disruptor 4.0.0 소스를 직접 열어 원인 규명.
- **어떻게(서사·근거)**: `BatchEventProcessor.run()`의 finally가 `running.set(IDLE)` — 죽은 소비자는 IDLE로 빠져 `hasBacklog()` 검사 대상에서 제외된다. 그래서 체인의 **마지막**(hasBacklog가 실제로 보는 대상) 자신이 죽으면 검사에서 빠져 shutdown이 바로 반환. 진짜 hang은 **저널(1단계) 소비자**가 죽었을 때 — 저널 자신은 빠지지만, 뒤에 물린 비즈니스/매칭 핸들러가 죽은 저널의 시퀀스를 영원히 기다리며 **블록된 채(안 죽고 running=true)** 멈춰 hasBacklog가 계속 true. 수정의 `halt()`가 `sequenceBarrier.alert()`로 그 블록된 핸들러를 깨워 종료시킨다. 테스트는 `PoisonJournal`/`PoisonAccountJournal`(append에서 예외)로 저널 계층을 죽여 재현. 그리고 테스트가 red에서 스스로 안 매달리게 shutdown을 daemon 스레드에서 돌리고 engine 참조를 null로(tearDown 재-shutdown 방지).
- **재료(커밋·도식·수치)**: `8908d3a`. Disruptor 4.0.0 `BatchEventProcessor.run()` finally(running IDLE)·`halt()`(HALTED+alert) 소스. RED 8s 타임아웃 실패 → GREEN 통과 실측.
