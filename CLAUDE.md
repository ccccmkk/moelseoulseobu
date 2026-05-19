# CLAUDE.md — STEP 프로젝트

## 프로젝트 개요

**STEP (Seobu Together Empathy Platform)**
서울 서부고용복지플러스센터 내부 커뮤니티 플랫폼. PWA 형태의 단일 페이지 앱.

- 프런트엔드: `index.html` (단일 파일, 인라인 CSS/JS)
- 백엔드: `worker.js` (Cloudflare Workers, v2.1.5)
- 배포: `wrangler.toml` → `band-archive-api.cm99i.workers.dev`

---

## 기술 스택

| 영역 | 기술 |
|------|------|
| 런타임 | Cloudflare Workers |
| DB | Cloudflare D1 (SQLite) — `band-archive-db` |
| 스토리지 | Cloudflare R2 — `band-archive` (9GB 한도) |
| AI | Workers AI (Llama 3.3 70B) → Gemini 2.5 Flash → Claude (폴백 순서) |
| 배포 도구 | Wrangler CLI |

---

## 파일 구조

```
index.html        # 프런트엔드 전체 (SPA, 인라인 CSS+JS)
worker.js         # 백엔드 전체 (API 라우터 + DB 초기화 + AI 로직)
wrangler.toml     # Cloudflare 배포 설정
sw.js             # 서비스 워커 (PWA 오프라인 지원)
manifest.json     # PWA 메타데이터
scripts/
  law-prefetch.js # 법령 데이터 사전 적재 스크립트
```

---

## 주요 API 엔드포인트

### 인증
- `POST /api/login` — 로그인 (세션 토큰 발급)
- `POST /api/verify-session` — 세션 검증
- `DELETE /api/sessions` — 로그아웃

### 게시판 / 커뮤니티
- `GET/POST /api/posts` — 게시글 목록 / 작성
- `GET /api/posts/search` — 게시글 검색
- `GET/POST /api/likes` — 좋아요
- `GET/POST /api/chat` — 실시간 채팅
- `GET/POST /api/events` — 이벤트
- `GET/POST /api/kudos` — 칭찬 카드

### 법령 검색
- `GET /api/law-search` — law.go.kr 검색 (D1 캐시 24h)
- `GET /api/law-content` — 법령 본문 조회
- `POST /api/law-ask` — AI 법령 질의응답

### 관리자
- `GET /api/admin/login-logs` — 접속 이력
- `POST /api/admin/law-cache` — 법령 캐시 초기화
- `GET/POST /api/users`, `/api/users/bulk` — 회원 관리
- `GET /api/roles` — 역할 조회

### AI 에이전트 (Cron)
- `POST /api/agent/health/post` — 건강 정보 글 자동 게시 (10:00 / 16:00 KST)
- `POST /api/agent/health/reply` — 댓글 자동 답변 (10분마다)

### 기타
- `POST /api/upload` — 이미지 업로드 (R2)
- `GET /api/news` — 뉴스 RSS (Google News, 10분 캐시)
- `GET/POST /api/restaurants` — 식당 목록 / 추가
- `GET/POST /api/quiz` — 퀴즈
- `POST /api/ladder` — 사다리 게임
- `GET/POST /api/contests`, `/api/photo-contests` — 이달의 직원 / 사진 콘테스트
- `GET /api/mileage`, `/api/profiles`, `/api/notifications` — 마일리지 / 프로필 / 알림
- `GET /api/usage` — R2 스토리지 사용량
- `GET /api/newsletters` — 소식지 목록
- `POST /api/newsletters` — 소식지 등록 (관리자)
- `DELETE /api/newsletters/:id` — 소식지 삭제 (관리자)

---

## DB 스키마 (주요 테이블)

`worker.js:137` `initDB()` 함수에서 `CREATE TABLE IF NOT EXISTS`로 자동 생성됨.

| 테이블 | 설명 |
|--------|------|
| `users` | 회원 (id, name, password, status) |
| `sessions` | 로그인 세션 토큰 |
| `user_roles` | 역할 (user / sub_admin / admin) |
| `posts` | 게시글 (blocks: JSON) |
| `comments`, `comment_replies` | 댓글 / 대댓글 |
| `likes`, `comment_likes` | 좋아요 |
| `chat_messages` | 채팅 메시지 |
| `events` | 이벤트 |
| `kudos` | 칭찬 카드 |
| `monthly_contests`, `nominations`, `contest_votes` | 이달의 직원 콘테스트 |
| `photo_contests`, `photo_submissions`, `photo_votes` | 사진 콘테스트 |
| `restaurants`, `restaurant_reviews`, `restaurant_votes` | 식당 |
| `quiz_*`, `quiz_series` | 퀴즈 |
| `news_cache` | 뉴스 RSS 캐시 |
| `settings` | 전역 설정 (key-value) |
| `usage` | R2 바이트 사용량 |
| `claude_usage`, `gemini_usage` | AI 토큰 사용량 추적 |
| `login_logs` | 접속 이력 |
| `user_presence` | 온라인 상태 |
| `newsletters` | 소식지 (title, pages: JSON 이미지 URL 배열) |

---

## AI 폴백 체인

`worker.js:24` `callAI()` 함수:
1. **Cloudflare Workers AI** — Llama 3.3 70B (무료, 기본)
2. **Gemini 2.5 Flash** — `GEMINI_API_KEY` 환경 변수 필요, `settings` 테이블로 on/off
3. **Claude** — `CLAUDE_API_KEY` 환경 변수 필요

---

## Cron 스케줄

| 표현식 | 실행 시각 (KST) | 동작 |
|--------|-----------------|------|
| `0 1 * * *` | 매일 10:00 | 건강 정보 글 자동 게시 |
| `0 7 * * *` | 매일 16:00 | 건강 정보 글 자동 게시 |
| `*/10 * * * *` | 10분마다 | 댓글 자동 답변 |

---

## 배포

### 자동 배포 (GitHub Actions)

`main` 브랜치에 `worker.js` 또는 `wrangler.toml` 변경이 푸시되면 **자동으로 Cloudflare에 배포**됨.

```
.github/workflows/deploy.yml
  트리거: push to main (worker.js, wrangler.toml 변경 시)
  사용: cloudflare/wrangler-action@v3
  시크릿: CLOUDFLARE_API_TOKEN (GitHub Secrets에 등록)
```

> **주의**: `index.html`만 변경된 경우 자동 배포가 트리거되지 않음.
> worker.js와 함께 변경되거나, 수동으로 배포해야 함.

### 수동 배포

```bash
# 로컬 개발
npx wrangler dev

# 프로덕션 배포
npx wrangler deploy
```

### GitHub Actions 워크플로 목록

| 파일 | 트리거 | 동작 |
|------|--------|------|
| `deploy.yml` | main push (worker.js/wrangler.toml) | Cloudflare Worker 배포 |
| `law-prefetch.yml` | 매주 일요일 03:00 KST / 수동 | 법령 데이터 D1 캐시 갱신 |

### 환경 변수 (Secrets)

Wrangler Dashboard 또는 아래 명령으로 설정:
```bash
npx wrangler secret put GEMINI_API_KEY
npx wrangler secret put CLAUDE_API_KEY
```

GitHub Secrets (Actions에서 사용):
- `CLOUDFLARE_API_TOKEN` — Wrangler 배포용
- `LAW_CACHE_TOKEN` — 법령 캐시 갱신용

---

## 주의사항

- `worker.js`는 단일 파일 2100줄 이상. 라우팅은 `if (p === '/api/...')` 체인 방식 (`worker.js:251~`).
- `index.html`도 단일 파일 SPA. 페이지 전환은 `.page.active` CSS 클래스로 제어.
- 법령 API는 `LAW_OC` 환경 변수(law.go.kr OC 코드) 필요 (`wrangler.toml`에 평문 설정됨).
- R2 저장 한도 9GB 초과 시 업로드 거부 (`MAX_BYTES = 9 * 1024 * 1024 * 1024`).
- 세션 토큰은 `Authorization: Bearer <token>` 헤더 또는 쿼리 파라미터 `?token=`으로 전달.
