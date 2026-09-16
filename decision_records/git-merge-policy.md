---
feature: git-merge-policy
date: 2026-09-16
feeds: [adr]
---

# main은 --no-ff로만 움직이고, 검증 안 끝난 측정·실험 브랜치는 main에 넣지 않는다

## 문제 (실제 사고)

측정 트랙(`feat/c7-load-harness`)이 검증이 끝나기 전에 main에 병합됐고, fast-forward라 병합 커밋도 충돌도 경고도 없이 조용히 넘어가 아무도 못 봤다. 그 뒤 한동안 main과 진행 중이던 브랜치가 같은 커밋을 가리키는 상태로 갔다 — "무엇이 언제 main에 들어갔는가"의 경계가 사라졌다.

## 어떻게 찾았나

main HEAD 커밋 메시지가 "Merge branch 'main' into feat/c7-load-harness"인 게 이상해서(보통 그 문구는 main을 feature로 당길 때 남지, main의 tip에 남을 문구가 아니다) 39가 점검을 요청했고, 2b가 `git rev-list --count main...feat/c7-load-harness`로 양방향 0(두 브랜치가 정확히 같은 커밋)을 확인해 fast-forward였음을 특정했다.

## 결정

- 측정·실험 브랜치는 **검증(빌드·테스트·리뷰)이 끝나기 전에 main에 넣지 않는다.**
- **main은 `--no-ff`로만 움직인다** — fast-forward 병합 금지.

## 왜 이 규칙인가

`--no-ff`면 병합이 커밋 하나로 남아 "무엇이 언제 들어갔는지"가 히스토리에 보인다. fast-forward는 그 경계를 지운다 — 검증 안 된 브랜치가 조용히 main에 섞여도 병합 커밋이 없어 아무도 못 본다. 이번이 정확히 그 사례였다. 병합 커밋 하나가 "이 시점에 이 트랙이 통째로 들어갔다"는 기록이자 리뷰·되돌리기의 단위가 된다.

## 결과

- 이 사고의 정리는 `git reset --hard`로 사고 병합을 되돌린 뒤(원래 main 커밋 `2544421`로 복귀) `git merge --no-ff feat/c7-load-harness`로 다시 넣어(병합 커밋 `448dd47`) 병합 경계를 복원했다. 상세 절차는 [[dc-handoff-2026-09-16]] 1절.
- 후속으로 main에 올리는 것(이 정책 기록·dc 인수인계 문서 포함)도 전부 `--no-ff`로, 로컬만 둔다(push는 Jack 판단).
- 관련: [[dc-handoff-2026-09-16]](이 사고의 정리 완료 기록·측정 재현 런북), [[measurement-instrumentation-permanent]].
