# 5fd8496 기준 기능별 브랜치 만들기 (cherry-pick)

## 0. 미리 할 일: 미커밋 작업 보관

지금 작업 트리(Redis, Snowflake relops, AccountDetailData 등)를 잃지 않으려면 **먼저 한 번 커밋**해 둔다.

```bash
git add -A
git status   # 확인
git commit -m "chore: Redis 수동 설정, Snowflake relops, 계좌 DTO 이동 등"
```

이렇게 하면 그 커밋 해시(예: `xxxxxxxx`)가 생긴다. 아래에서 9번째 커밋으로 쓰거나, 별도 브랜치에만 cherry-pick하면 된다.

---

## 1. feature/api-base를 5fd8496으로 되돌리기

```bash
git checkout feature/api-base
git reset --hard 5fd8496
```

이후 `feature/api-base`는 5fd8496 상태만 남고, 위 8개(+ 방금 만든 1개) 커밋은 로컬에만 해시로 남아 있다.

---

## 2. 기능별 브랜치 + cherry-pick (예시)

**기준**: 모두 `5fd8496`에서 브랜치를 만들고, 해당 기능 커밋만 cherry-pick.

아래는 **기능 단위 예시**다. 하나로 묶고 싶은 커밋만 골라서 쓰면 된다.

| 브랜치 이름 (예시) | cherry-pick 할 커밋 (오래된 순) |
|--------------------|----------------------------------|
| `feature/scenario-domain` | `3bed36d` |
| `feature/account-refactor` | `f964020` → `552578a` |
| `feature/test-scenario` | `6043b16` |
| `feature/auth` | `0e2cfe8` |
| `feature/snowflake` | `ddff084` |
| `feature/api-implementation` | `030f24b` |
| `feature/local-setup` | `a006298` |
| `feature/redis-config` | (0단계에서 만든 커밋 해시) |

**실행 예 (한 브랜치)**:

```bash
# 5fd8496에서 브랜치 생성 후 해당 커밋만 cherry-pick
git checkout -b feature/auth 5fd8496
git cherry-pick 0e2cfe8
git push -u origin feature/auth
```

**여러 커밋 한 번에**:

```bash
git checkout -b feature/account-refactor 5fd8496
git cherry-pick f964020 552578a
git push -u origin feature/account-refactor
```

---

## 3. 한 번에 돌리는 예시 (원하면 조합 변경)

```bash
# 1) 미커밋 커밋 (해시 기억해 둠)
git add -A && git commit -m "chore: Redis 수동 설정, Snowflake relops, 계좌 DTO 이동 등"
BACKUP_COMMIT=$(git rev-parse HEAD)

# 2) feature/api-base 되돌리기
git checkout feature/api-base
git reset --hard 5fd8496

# 3) 기능별 브랜치 (이름/커밋은 원하는 대로 수정)
git checkout -b feature/scenario-domain 5fd8496
git cherry-pick 3bed36d
git push -u origin feature/scenario-domain

git checkout -b feature/account-refactor 5fd8496
git cherry-pick f964020 552578a
git push -u origin feature/account-refactor

git checkout -b feature/test-scenario 5fd8496
git cherry-pick 6043b16
git push -u origin feature/test-scenario

git checkout -b feature/auth 5fd8496
git cherry-pick 0e2cfe8
git push -u origin feature/auth

git checkout -b feature/snowflake 5fd8496
git cherry-pick ddff084
git push -u origin feature/snowflake

git checkout -b feature/api-implementation 5fd8496
git cherry-pick 030f24b
git push -u origin feature/api-implementation

git checkout -b feature/local-setup 5fd8496
git cherry-pick a006298
git push -u origin feature/local-setup

# 4) Redis/설정 정리 브랜치 (0단계 커밋)
git checkout -b feature/redis-config 5fd8496
git cherry-pick $BACKUP_COMMIT
git push -u origin feature/redis-config
```

---

## 4. 참고

- **충돌 나면**: `git status`로 확인 후 수정 → `git add` → `git cherry-pick --continue`. 취소는 `git cherry-pick --abort`.
- **브랜치/커밋 구성**은 위 표를 보고 필요한 것만 골라서 쓰면 된다.
- **feature/api-base**는 1단계에서 `reset --hard 5fd8496` 하면 그 시점으로만 남고, 원격에 이미 푸시된 이력이 있으면 `git push --force`가 필요할 수 있다 (협업 중이면 팀과 합의 후 진행).
