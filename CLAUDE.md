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

## Claude 작업 규칙

### 세션 시작 시 필수: 브랜치 동기화

**작업 전 항상 먼저 실행:**

```bash
git fetch origin && git pull --ff-only
```

> **이유**: 컨텍스트 압축(세션 재시작) 후 로컬 브랜치가 remote보다 뒤처진 상태일 수 있다.
> 이 상태에서 Agent(worktree)를 생성하면 **구버전 파일 기준으로 작업**해 이후 커밋이 그 사이 변경 내용을 덮어써버린다.
> (실제로 PR #78~90 의 index.html 변경이 PR #91 에 의해 전부 유실된 사고 발생)
>
> SessionStart 훅(`.claude/hooks/session-start.sh`)이 자동으로 실행하지만,
> **에이전트를 생성하기 직전**에도 수동으로 확인할 것.

### 브랜치 & PR 필수 절차

작업 완료 후 **항상 아래 순서를 따른다**:

1. 변경 파일을 커밋하고 작업 브랜치에 push
2. **반드시 PR을 생성한다** (`mcp__github__create_pull_request` 사용)
   - base: `main`, head: 현재 작업 브랜치
   - 제목: 작업 내용을 한 줄로 요약 (한국어)
   - 본문: 변경 사항 요약 + 테스트 포인트
3. PR URL을 사용자에게 알려준다

> PR을 만들지 않으면 사용자가 변경 내용을 GitHub에서 확인하거나 머지할 수 없다.
> 따라서 push 후 PR 생성은 **선택이 아니라 필수**다.

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

## API 시스템 구성

### 런타임 환경

| 항목 | 값 |
|------|-----|
| 플랫폼 | Cloudflare Workers (V8 isolate, 전 세계 엣지) |
| Worker 이름 | `band-archive-api` |
| 배포 URL | `band-archive-api.cm99i.workers.dev` |
| DB | D1 (SQLite) — `band-archive-db` |
| 스토리지 | R2 — `band-archive` (최대 9GB) |
| AI | Workers AI 바인딩 (`env.AI`) |

### 환경 변수 전체 목록

| 변수명 | 종류 | 설명 |
|--------|------|------|
| `DB` | 바인딩 | Cloudflare D1 데이터베이스 |
| `R2` | 바인딩 | Cloudflare R2 버킷 |
| `AI` | 바인딩 | Cloudflare Workers AI |
| `LAW_OC` | vars (평문) | law.go.kr OpenAPI OC 코드 |
| `GEMINI_API_KEY` | secret | Google Gemini API 키 |
| `ANTHROPIC_API_KEY` | secret | Anthropic Claude API 키 |
| `LAW_CACHE_TOKEN` | secret | 법령 캐시 수동 갱신 인증 토큰 |
| `MOEL_LLM_TOKEN` | secret | MOEL LLM API 인증 토큰 |
| `MOEL_ORG_CODE` | secret | MOEL 조직 코드 (선택) |

### CORS 정책

```js
'Access-Control-Allow-Origin': '*'
'Access-Control-Allow-Methods': 'GET,POST,PUT,DELETE,PATCH,OPTIONS'
'Access-Control-Allow-Headers': 'Content-Type'
```

- 인증은 `Authorization` 헤더가 **아닌** `?token=` 쿼리 파라미터로 전달 (CORS 제약)
- 모든 응답(`json()` 헬퍼)에 자동 포함

### AI 폴백 체인 (`callAI()` — `worker.js:24`)

```
1순위: Cloudflare Workers AI  (Llama 3.3 70B, 무료)
2순위: Gemini 2.5 Flash       (GEMINI_API_KEY 필요, settings.gemini_fallback_enabled)
3순위: Claude Haiku 4.5       (ANTHROPIC_API_KEY 필요, settings.claude_enabled)
모두 실패 시 null 반환
```

- 각 단계는 독립 try-catch, 실패해도 다음 단계로 진행
- Gemini/Claude 사용량은 `gemini_usage`, `claude_usage` 테이블에 기록

### 뉴스 API 구조 (`/api/news`)

- 소스: Google News RSS (`news.google.com/rss/search`)
- 캐시: D1 `news_cache` 테이블, TTL 10분
- fetch 타임아웃: 8초
- **stale fallback**: fetch 실패 시 만료된 캐시라도 반환 (빈 화면 방지)
- 카테고리: `labor` / `local` / `health` / `law`

### 라우팅 구조 (`worker.js:251~`)

- 방식: `if (p === '/api/...' && m === 'METHOD')` 체인
- 정적 라우트를 동적 라우트보다 앞에 배치 (`/api/posts/search` > `/api/posts/:id`)
- 이미지 서빙: `GET /img/*` → R2에서 직접 스트리밍

### 주요 외부 의존성

| 서비스 | 용도 | 비고 |
|--------|------|------|
| Google News RSS | 뉴스 피드 | 10분 캐시, 타임아웃 8초 |
| law.go.kr OpenAPI | 법령/판례 검색 | `LAW_OC` 코드 필요 |
| Gemini API | AI 폴백 1순위 | `generativelanguage.googleapis.com` |
| Anthropic API | AI 폴백 2순위 | `api.anthropic.com` |

---

## 주의사항

- `worker.js`는 단일 파일 2100줄 이상. 라우팅은 `if (p === '/api/...')` 체인 방식 (`worker.js:251~`).
- `index.html`도 단일 파일 SPA. 페이지 전환은 `.page.active` CSS 클래스로 제어.
- 법령 API는 `LAW_OC` 환경 변수(law.go.kr OC 코드) 필요 (`wrangler.toml`에 평문 설정됨).
- R2 저장 한도 9GB 초과 시 업로드 거부 (`MAX_BYTES = 9 * 1024 * 1024 * 1024`).
- 세션 토큰은 `Authorization: Bearer <token>` 헤더 또는 쿼리 파라미터 `?token=`으로 전달.
