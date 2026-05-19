#!/bin/bash
# 세션 시작 시 원격 브랜치와 동기화 — worktree 생성 전 로컬이 stale하지 않도록
if [ "${CLAUDE_CODE_REMOTE:-}" != "true" ]; then
  exit 0
fi

git fetch origin 2>/dev/null || true
git pull --ff-only 2>/dev/null || true
