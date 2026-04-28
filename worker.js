// v2.1.5
const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET,POST,PUT,DELETE,OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type',
};
const MAX_BYTES = 9 * 1024 * 1024 * 1024;
const LAW_INFLIGHT = new Map(); // 동일 쿼리 동시 요청 중복 제거

function json(data, status = 200) {
  return new Response(JSON.stringify(data), {
    status, headers: { ...CORS, 'Content-Type': 'application/json' }
  });
}

function fetchTimeout(url, options = {}, ms = 9000) {
  const ctrl = new AbortController();
  const tid = setTimeout(() => ctrl.abort(), ms);
  return fetch(url, { ...options, signal: ctrl.signal }).finally(() => clearTimeout(tid));
}

// Workers AI → Gemini → Claude 순서로 시도하는 통합 AI 헬퍼
async function callAI(systemPrompt, userMessage, env, opts = {}) {
  const { type = 'general', maxTokens = 8192 } = opts;
  const _now = () => Math.floor(Date.now() / 1000);
  const _date = () => new Date(Date.now() + 9 * 3600 * 1000).toISOString().slice(0, 10);

  // 1. Cloudflare Workers AI (주 - 무료, IP 차단 없음)
  if (env.AI) {
    try {
      const wRes = await env.AI.run('@cf/meta/llama-3.3-70b-instruct-fp8-fast', {
        messages: [
          ...(systemPrompt ? [{ role: 'system', content: systemPrompt }] : []),
          { role: 'user', content: userMessage },
        ],
        max_tokens: Math.min(maxTokens, 4096),
      });
      const text = wRes?.response || '';
      if (text) return { text, model: 'workers-ai' };
    } catch (e) { console.error('[Workers AI]', e.message); }
  }

  // 2. Gemini 폴백 (관리자가 허용한 경우)
  const geminiSetting = await env.DB.prepare("SELECT value FROM settings WHERE key='gemini_fallback_enabled'").first();
  const geminiAllowed = !geminiSetting || geminiSetting.value !== 'false';
  if (geminiAllowed && env.GEMINI_API_KEY) {
    try {
      const gResp = await fetchTimeout(
        `https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key=${env.GEMINI_API_KEY}`,
        {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            contents: [{ role: 'user', parts: [{ text: (systemPrompt ? systemPrompt + '\n\n' : '') + userMessage }] }],
            generationConfig: { maxOutputTokens: maxTokens, thinkingConfig: { thinkingBudget: 0 } },
          }),
        }, 20000
      );
      if (gResp.ok) {
        const gData = await gResp.json();
        const text = gData?.candidates?.[0]?.content?.parts?.[0]?.text || '';
        if (text) {
          const tokIn = gData.usageMetadata?.promptTokenCount || 0;
          const tokOut = gData.usageMetadata?.candidatesTokenCount || 0;
          await env.DB.prepare('INSERT INTO gemini_usage(id,tokens_in,tokens_out,calls,updated_at) VALUES(1,?,?,1,?) ON CONFLICT(id) DO UPDATE SET tokens_in=tokens_in+?,tokens_out=tokens_out+?,calls=calls+1,updated_at=?')
            .bind(tokIn, tokOut, _now(), tokIn, tokOut, _now()).run();
          await env.DB.prepare('INSERT INTO gemini_usage_daily(date,type,tokens_in,tokens_out,calls) VALUES(?,?,?,?,1) ON CONFLICT(date,type) DO UPDATE SET tokens_in=tokens_in+excluded.tokens_in,tokens_out=tokens_out+excluded.tokens_out,calls=calls+1')
            .bind(_date(), type, tokIn, tokOut).run();
          return { text, model: 'gemini', tokensIn: tokIn, tokensOut: tokOut };
        }
      }
    } catch (e) { console.error('[Gemini]', e.message); }
  }

  // 3. Claude 폴백 (관리자가 허용한 경우)
  const claudeSetting = await env.DB.prepare("SELECT value FROM settings WHERE key='claude_enabled'").first();
  const claudeAllowed = !claudeSetting || claudeSetting.value !== 'false';
  if (claudeAllowed && env.ANTHROPIC_API_KEY) {
    try {
      const aiResp = await fetchTimeout('https://api.anthropic.com/v1/messages', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'x-api-key': env.ANTHROPIC_API_KEY, 'anthropic-version': '2023-06-01' },
        body: JSON.stringify({
          model: 'claude-haiku-4-5-20251001',
          max_tokens: Math.min(maxTokens, 8096),
          ...(systemPrompt ? { system: systemPrompt } : {}),
          messages: [{ role: 'user', content: userMessage }],
        }),
      }, 20000);
      const aiData = await aiResp.json();
      const text = aiData.content?.[0]?.text || '';
      if (text) {
        const tokIn = aiData.usage?.input_tokens || 0;
        const tokOut = aiData.usage?.output_tokens || 0;
        await env.DB.prepare('INSERT INTO claude_usage(id,tokens_in,tokens_out,calls,updated_at) VALUES(1,?,?,1,?) ON CONFLICT(id) DO UPDATE SET tokens_in=tokens_in+?,tokens_out=tokens_out+?,calls=calls+1,updated_at=?')
          .bind(tokIn, tokOut, _now(), tokIn, tokOut, _now()).run();
        return { text, model: 'claude', tokensIn: tokIn, tokensOut: tokOut };
      }
    } catch (e) { console.error('[Claude]', e.message); }
  }

  return null;
}

// 마일리지 적립
async function addMileageDB(env, userId, delta) {
  if (!userId || !delta) return;
  await env.DB.prepare(
    'INSERT INTO user_mileage(user_id,points) VALUES(?,?) ON CONFLICT(user_id) DO UPDATE SET points=ROUND(points+?,10)'
  ).bind(userId, delta, delta).run();
}

// RSS 파싱
function parseRSS(xml) {
  const out = [];
  const blocks = xml.match(/<item>([\s\S]*?)<\/item>/g) || [];
  for (const b of blocks.slice(0, 40)) {
    const tag = (t) => {
      const cd = new RegExp(`<${t}[^>]*><!\\[CDATA\\[([\\s\\S]*?)\\]\\]><\\/${t}>`, 'i').exec(b);
      if (cd) return cd[1].trim();
      const pl = new RegExp(`<${t}[^>]*>([^<]*)<\\/${t}>`, 'i').exec(b);
      return pl ? pl[1].trim() : '';
    };
    let link = '';
    const lm = /<link>([\s\S]*?)<\/link>/i.exec(b);
    if (lm) link = lm[1].replace(/<!\[CDATA\[/, '').replace(/\]\]>/, '').trim();
    const title = tag('title');
    const pubDate = tag('pubDate');
    const source = tag('source') || '';
    if (title) out.push({ title, link: link || tag('guid'), pubDate, source });
  }
  return out;
}

let _dbReady = false;
async function initDB(env) {
  if (_dbReady) return;
  const tables = [
    `CREATE TABLE IF NOT EXISTS posts (id TEXT PRIMARY KEY, author TEXT, blocks TEXT, created_at INTEGER, like_count INTEGER DEFAULT 0)`,
    `CREATE TABLE IF NOT EXISTS usage (id INTEGER PRIMARY KEY, bytes INTEGER DEFAULT 0)`,
    `CREATE TABLE IF NOT EXISTS likes (post_id TEXT, user_id TEXT, PRIMARY KEY(post_id, user_id))`,
    `CREATE TABLE IF NOT EXISTS comments (id TEXT PRIMARY KEY, post_id TEXT, author TEXT, content TEXT, created_at INTEGER)`,
    `CREATE TABLE IF NOT EXISTS comment_likes (comment_id TEXT, user_id TEXT, PRIMARY KEY(comment_id, user_id))`,
    `CREATE TABLE IF NOT EXISTS comment_replies (id TEXT PRIMARY KEY, comment_id TEXT, author TEXT, content TEXT, created_at INTEGER)`,
    `CREATE TABLE IF NOT EXISTS events (id TEXT PRIMARY KEY, author TEXT, type TEXT, title TEXT, content TEXT, created_at INTEGER)`,
    `CREATE TABLE IF NOT EXISTS user_roles (user_id TEXT PRIMARY KEY, role TEXT DEFAULT 'user')`,
    `CREATE TABLE IF NOT EXISTS user_mileage (user_id TEXT PRIMARY KEY, points REAL DEFAULT 0)`,
    `CREATE TABLE IF NOT EXISTS user_profiles (user_id TEXT PRIMARY KEY, avatar_url TEXT)`,
    `CREATE TABLE IF NOT EXISTS post_keywords (post_id TEXT PRIMARY KEY, keyword TEXT)`,
    `CREATE TABLE IF NOT EXISTS users (id TEXT PRIMARY KEY, name TEXT DEFAULT '', password TEXT NOT NULL, status TEXT DEFAULT 'active', created_at INTEGER)`,
    `CREATE TABLE IF NOT EXISTS sessions (token TEXT PRIMARY KEY, user_id TEXT, created_at INTEGER)`,
    `CREATE TABLE IF NOT EXISTS news_cache (category TEXT PRIMARY KEY, data TEXT, cached_at INTEGER DEFAULT 0)`,
    `CREATE TABLE IF NOT EXISTS user_presence (user_id TEXT PRIMARY KEY, last_seen INTEGER DEFAULT 0)`,
    `CREATE TABLE IF NOT EXISTS chat_messages (id TEXT PRIMARY KEY, author TEXT, content TEXT, created_at INTEGER)`,
    `CREATE TABLE IF NOT EXISTS claude_usage (id INTEGER PRIMARY KEY, tokens_in INTEGER DEFAULT 0, tokens_out INTEGER DEFAULT 0, calls INTEGER DEFAULT 0, updated_at INTEGER DEFAULT 0)`,
    `CREATE TABLE IF NOT EXISTS gemini_usage (id INTEGER PRIMARY KEY, tokens_in INTEGER DEFAULT 0, tokens_out INTEGER DEFAULT 0, calls INTEGER DEFAULT 0, updated_at INTEGER DEFAULT 0)`,
    `CREATE TABLE IF NOT EXISTS gemini_usage_daily (date TEXT, type TEXT, tokens_in INTEGER DEFAULT 0, tokens_out INTEGER DEFAULT 0, calls INTEGER DEFAULT 0, PRIMARY KEY(date, type))`,
    `CREATE TABLE IF NOT EXISTS kudos (id TEXT PRIMARY KEY, tag TEXT, source TEXT, content TEXT, added_by TEXT, created_at INTEGER)`,
    `CREATE TABLE IF NOT EXISTS monthly_contests (id TEXT PRIMARY KEY, title TEXT, description TEXT, nominate_start INTEGER, nominate_end INTEGER, vote_start INTEGER, vote_end INTEGER, created_by TEXT, winner TEXT, created_at INTEGER)`,
    `CREATE TABLE IF NOT EXISTS nominations (id TEXT PRIMARY KEY, contest_id TEXT, nominee TEXT, nominated_by TEXT, message TEXT, is_anonymous INTEGER DEFAULT 0, created_at INTEGER, UNIQUE(contest_id, nominated_by))`,
    `CREATE TABLE IF NOT EXISTS nominee_msgs (id TEXT PRIMARY KEY, contest_id TEXT, nominee TEXT, content TEXT, author_display TEXT, created_at INTEGER)`,
    `CREATE TABLE IF NOT EXISTS contest_votes (contest_id TEXT, voter TEXT, nominee TEXT, PRIMARY KEY(contest_id, voter))`,
    `CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT)`,
    `CREATE TABLE IF NOT EXISTS restaurants (id TEXT PRIMARY KEY, name TEXT, address TEXT, category TEXT, walk_min INTEGER DEFAULT 5, note TEXT, added_by TEXT, created_at INTEGER)`,
    `CREATE TABLE IF NOT EXISTS restaurant_reviews (id TEXT PRIMARY KEY, restaurant_id TEXT, author TEXT, content TEXT, created_at INTEGER)`,
    `CREATE TABLE IF NOT EXISTS restaurant_votes (restaurant_id TEXT, user_id TEXT, vote INTEGER, PRIMARY KEY(restaurant_id, user_id))`,
    `CREATE TABLE IF NOT EXISTS login_logs (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id TEXT, ip TEXT, user_agent TEXT, result TEXT, created_at INTEGER)`,
    `CREATE TABLE IF NOT EXISTS moel_usage (id INTEGER PRIMARY KEY, tokens_in INTEGER DEFAULT 0, tokens_out INTEGER DEFAULT 0, calls INTEGER DEFAULT 0, updated_at INTEGER DEFAULT 0)`,
    `CREATE TABLE IF NOT EXISTS quiz_sessions (id TEXT PRIMARY KEY, question TEXT NOT NULL, answer TEXT NOT NULL, status TEXT DEFAULT 'waiting', started_at INTEGER, revealed_at INTEGER, created_by TEXT, created_at INTEGER)`,
    `CREATE TABLE IF NOT EXISTS quiz_answers (quiz_id TEXT, user_id TEXT, answer TEXT NOT NULL, answered_at INTEGER, PRIMARY KEY(quiz_id, user_id))`,
    `CREATE TABLE IF NOT EXISTS quiz_series (id TEXT PRIMARY KEY, total_stages INTEGER DEFAULT 3, current_stage INTEGER DEFAULT 0, status TEXT DEFAULT 'active', created_by TEXT, created_at INTEGER)`,
    `CREATE TABLE IF NOT EXISTS photo_contests (id TEXT PRIMARY KEY, title TEXT NOT NULL, description TEXT DEFAULT '', status TEXT DEFAULT 'draft', created_by TEXT, created_at INTEGER)`,
    `CREATE TABLE IF NOT EXISTS photo_entries (id TEXT PRIMARY KEY, contest_id TEXT NOT NULL, uploader TEXT NOT NULL, img_url TEXT NOT NULL, caption TEXT DEFAULT '', created_at INTEGER)`,
    `CREATE TABLE IF NOT EXISTS photo_votes (contest_id TEXT, voter TEXT, photo_id TEXT, PRIMARY KEY(contest_id, voter))`,
  ];
  await env.DB.batch(tables.map(t => env.DB.prepare(t)));
  // 마이그레이션: name 컬럼이 없는 기존 DB 대응 (INSERT 전에 실행)
  try { await env.DB.exec("ALTER TABLE users ADD COLUMN name TEXT DEFAULT ''"); } catch(e) {}
  try { await env.DB.exec("ALTER TABLE users ADD COLUMN dept TEXT DEFAULT ''"); } catch(e) {}
  try { await env.DB.exec("ALTER TABLE posts ADD COLUMN mode TEXT DEFAULT 'normal'"); } catch(e) {}
  try { await env.DB.exec("ALTER TABLE user_profiles ADD COLUMN show_badge_admin INTEGER DEFAULT 1"); } catch(e) {}
  try { await env.DB.exec("ALTER TABLE user_profiles ADD COLUMN show_badge_top INTEGER DEFAULT 1"); } catch(e) {}
  try { await env.DB.exec("ALTER TABLE user_profiles ADD COLUMN granted_badge_admin INTEGER DEFAULT 0"); } catch(e) {}
  try { await env.DB.exec("ALTER TABLE user_profiles ADD COLUMN granted_badge_top INTEGER DEFAULT 0"); } catch(e) {}
  try { await env.DB.exec("ALTER TABLE kudos ADD COLUMN user_target TEXT DEFAULT ''"); } catch(e) {}
  try { await env.DB.exec("ALTER TABLE events ADD COLUMN tagged_user TEXT DEFAULT ''"); } catch(e) {}
  try { await env.DB.exec("CREATE TABLE IF NOT EXISTS gemini_usage_daily (date TEXT, type TEXT, tokens_in INTEGER DEFAULT 0, tokens_out INTEGER DEFAULT 0, calls INTEGER DEFAULT 0, PRIMARY KEY(date, type))"); } catch(e) {}
  try { await env.DB.exec("ALTER TABLE quiz_sessions ADD COLUMN series_id TEXT"); } catch(e) {}
  try { await env.DB.exec("ALTER TABLE quiz_sessions ADD COLUMN stage_num INTEGER DEFAULT 1"); } catch(e) {}
  try { await env.DB.exec("ALTER TABLE quiz_sessions ADD COLUMN group_target TEXT DEFAULT 'all'"); } catch(e) {}
  try { await env.DB.exec("ALTER TABLE quiz_series ADD COLUMN group_target TEXT DEFAULT 'all'"); } catch(e) {}
  try { await env.DB.exec(`CREATE TABLE IF NOT EXISTS ladder_games (id TEXT PRIMARY KEY, series_id TEXT, participants TEXT NOT NULL, structure TEXT NOT NULL, picks TEXT DEFAULT '{}', winner_id TEXT, status TEXT DEFAULT 'picking', pick_deadline INTEGER, created_at INTEGER, created_by TEXT)`); } catch(e) {}
  try { await env.DB.exec(`CREATE TABLE IF NOT EXISTS quiz_attendees (quiz_id TEXT NOT NULL, user_id TEXT NOT NULL, attended_at INTEGER, PRIMARY KEY(quiz_id, user_id))`); } catch(e) {}
  try { await env.DB.exec("ALTER TABLE photo_contests ADD COLUMN contest_group TEXT DEFAULT 'branch'"); } catch(e) {}
  try { await env.DB.exec("ALTER TABLE photo_contests ADD COLUMN revealed INTEGER DEFAULT 0"); } catch(e) {}
  try { await env.DB.exec("ALTER TABLE photo_contests ADD COLUMN entry_type TEXT DEFAULT 'photo'"); } catch(e) {}
  try { await env.DB.exec("ALTER TABLE photo_entries ADD COLUMN text_content TEXT DEFAULT ''"); } catch(e) {}
  // 건강봇 아바타 시드
  try { await env.DB.prepare("INSERT INTO user_profiles(user_id,avatar_url) VALUES('000000099','💊') ON CONFLICT(user_id) DO UPDATE SET avatar_url=CASE WHEN avatar_url IS NULL OR avatar_url='' THEN '💊' ELSE avatar_url END").run(); } catch(e) {}
  // 관리자 계정 문자열 ID → 숫자 ID 마이그레이션
  try { await env.DB.exec("DELETE FROM sessions WHERE user_id='관리자'"); } catch(e) {}
  try { await env.DB.exec("DELETE FROM user_roles WHERE user_id='관리자'"); } catch(e) {}
  try { await env.DB.exec("DELETE FROM users WHERE id='관리자'"); } catch(e) {}
  await env.DB.batch([
    env.DB.prepare('INSERT INTO user_roles(user_id,role) VALUES(?,?) ON CONFLICT(user_id) DO UPDATE SET role=?').bind('000000001','admin','admin'),
    env.DB.prepare('INSERT INTO users(id,name,password,status,created_at) VALUES(?,?,?,?,?) ON CONFLICT(id) DO NOTHING').bind('000000001','관리자','9999','active',0),
    env.DB.prepare('INSERT INTO user_roles(user_id,role) VALUES(?,?) ON CONFLICT(user_id) DO UPDATE SET role=?').bind('050007557','admin','admin'),
    env.DB.prepare('INSERT INTO users(id,name,password,status,created_at) VALUES(?,?,?,?,?) ON CONFLICT(id) DO NOTHING').bind('050007557','김창민','1234','active',0),
    env.DB.prepare('INSERT INTO user_roles(user_id,role) VALUES(?,?) ON CONFLICT(user_id) DO UPDATE SET role=?').bind('000000099','user','user'),
    env.DB.prepare('INSERT INTO users(id,name,password,status,created_at) VALUES(?,?,?,?,?) ON CONFLICT(id) DO NOTHING').bind('000000099','건강봇','__agent__','active',0),
  ]);
  // 인덱스 (쿼리 속도 최적화)
  const indexes = [
    'CREATE INDEX IF NOT EXISTS idx_posts_created ON posts(created_at DESC)',
    'CREATE INDEX IF NOT EXISTS idx_comments_post ON comments(post_id, created_at ASC)',
    'CREATE INDEX IF NOT EXISTS idx_replies_comment ON comment_replies(comment_id, created_at ASC)',
    'CREATE INDEX IF NOT EXISTS idx_likes_post ON likes(post_id)',
    'CREATE INDEX IF NOT EXISTS idx_likes_user ON likes(user_id)',
    'CREATE INDEX IF NOT EXISTS idx_comment_likes_comment ON comment_likes(comment_id)',
    'CREATE INDEX IF NOT EXISTS idx_comment_likes_user ON comment_likes(user_id)',
    'CREATE INDEX IF NOT EXISTS idx_sessions_user ON sessions(user_id)',
    'CREATE INDEX IF NOT EXISTS idx_sessions_token ON sessions(token)',
    'CREATE INDEX IF NOT EXISTS idx_login_logs_created ON login_logs(created_at DESC)',
    'CREATE INDEX IF NOT EXISTS idx_login_logs_user ON login_logs(user_id, result, created_at)',
    'CREATE INDEX IF NOT EXISTS idx_chat_created ON chat_messages(created_at ASC)',
    'CREATE INDEX IF NOT EXISTS idx_nominations_contest ON nominations(contest_id)',
    'CREATE INDEX IF NOT EXISTS idx_contest_votes_contest ON contest_votes(contest_id)',
    'CREATE INDEX IF NOT EXISTS idx_post_keywords ON post_keywords(post_id)',
    'CREATE INDEX IF NOT EXISTS idx_presence_user ON user_presence(user_id)',
  ];
  await env.DB.batch(indexes.map(s => env.DB.prepare(s)));
  _dbReady = true;
}

export default {
  async fetch(request, env, ctx) {
    if (request.method === 'OPTIONS') return new Response(null, { headers: CORS });

    const url = new URL(request.url);
    const p = url.pathname;
    const m = request.method;

    try {
      await initDB(env);
      // ── 이미지 서빙 ──
      if (p.startsWith('/img/') && m === 'GET') {
        const obj = await env.R2.get(p.slice(1));
        if (!obj) return new Response('Not found', { status: 404 });
        return new Response(obj.body, {
          headers: { 'Content-Type': obj.httpMetadata?.contentType || 'image/jpeg', 'Cache-Control': 'public,max-age=31536000', ...CORS }
        });
      }

      // ── 이미지 업로드 ──
      if (p === '/api/upload' && m === 'POST') {
        const row = await env.DB.prepare('SELECT bytes FROM usage WHERE id=1').first();
        const used = row?.bytes || 0;
        const fd = await request.formData();
        const file = fd.get('file');
        if (!file) return json({ error: '파일 없음' }, 400);
        if (used + file.size > MAX_BYTES) return json({ error: '저장 용량 한도(9GB) 초과' }, 400);
        const ext = file.name.split('.').pop() || 'jpg';
        const key = `img/${Date.now()}-${Math.random().toString(36).slice(2)}.${ext}`;
        await env.R2.put(key, file.stream(), { httpMetadata: { contentType: file.type } });
        await env.DB.prepare('INSERT INTO usage(id,bytes) VALUES(1,?) ON CONFLICT(id) DO UPDATE SET bytes=bytes+?')
          .bind(file.size, file.size).run();
        return json({ url: `${new URL(request.url).origin}/${key}` });
      }

      // ── 뉴스 (Google News RSS + D1 캐시 10분, 요약 없음) ──
      if (p === '/api/news' && m === 'GET') {
        const cat = url.searchParams.get('category') || 'labor';
        const queries = {
          labor: '고용노동부 OR 취업 OR 채용 OR 실업급여 일자리',
          local: '마포구 OR 용산구 OR 서대문구 OR 은평구',
          health: '질병관리청 OR 보건복지부 건강',
          law: '근로기준법 OR 노동법 OR 산업재해 OR 노동부 법률',
        };
        if (!queries[cat]) return json({ error: 'unknown' }, 400);
        const feedUrl = 'https://news.google.com/rss/search?q=' + encodeURIComponent(queries[cat]) + '&hl=ko&gl=KR&ceid=KR:ko';
        const cached = await env.DB.prepare('SELECT data, cached_at FROM news_cache WHERE category=?').bind(cat).first();
        if (cached && (Math.floor(Date.now() / 1000) - cached.cached_at) < 600) {
          return json(JSON.parse(cached.data));
        }
        const resp = await fetch(feedUrl, {
          headers: { 'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36' }
        });
        if (!resp.ok) return json({ error: '뉴스를 불러오지 못했습니다.' }, 502);
        const xml = await resp.text();
        const items = parseRSS(xml).sort((a, b) => new Date(b.pubDate || 0) - new Date(a.pubDate || 0));
        const now = Math.floor(Date.now() / 1000);
        await env.DB.prepare('INSERT INTO news_cache(category,data,cached_at) VALUES(?,?,?) ON CONFLICT(category) DO UPDATE SET data=?,cached_at=?')
          .bind(cat, JSON.stringify(items), now, JSON.stringify(items), now).run();
        return json(items);
      }

      // ── 접속 이력 (관리자) ──
      if (p === '/api/admin/login-logs' && m === 'GET') {
        const authToken = url.searchParams.get('token') || request.headers.get('Authorization')?.replace('Bearer ', '');
        const sess = authToken ? await env.DB.prepare('SELECT user_id FROM sessions WHERE token=?').bind(authToken).first() : null;
        if (!sess) return json({ error: 'unauthorized' }, 401);
        const role = await env.DB.prepare('SELECT role FROM user_roles WHERE user_id=?').bind(sess.user_id).first();
        if (!['admin','sub_admin'].includes(role?.role)) return json({ error: 'forbidden' }, 403);
        const limit = Math.min(parseInt(url.searchParams.get('limit') || '100'), 200);
        const rows = await env.DB.prepare('SELECT l.id, l.user_id, u.name, l.ip, l.user_agent, l.result, l.created_at FROM login_logs l LEFT JOIN users u ON l.user_id=u.id ORDER BY l.created_at DESC LIMIT ?').bind(limit).all();
        return json(rows.results || []);
      }

      // ── 법령 검색 ──
      if (p === '/api/law-search' && m === 'GET') {
        const OC = env.LAW_OC || 'STEP-OPENAPI';
        const q = url.searchParams.get('q') || '근로';
        const target = url.searchParams.get('target') || 'all';
        const nocache = url.searchParams.get('nocache') === '1';
        const cacheKey = `law3_${OC}_${target}_${q.slice(0,30)}`;
        if (!nocache) {
          const cached = await env.DB.prepare('SELECT data, cached_at FROM news_cache WHERE category=?').bind(cacheKey).first();
          if (cached && (Math.floor(Date.now() / 1000) - (cached.cached_at || 0)) < 21600) { // 6시간 캐시
            try {
              const parsed = JSON.parse(cached.data);
              if (parsed.laws?.length || parsed.precs?.length || parsed.expcs?.length) return json(parsed);
            } catch (e) {}
          }
        }
        // 동일 쿼리 동시 요청 중복 제거 (singleflight)
        if (LAW_INFLIGHT.has(cacheKey)) {
          try { return json(await LAW_INFLIGHT.get(cacheKey)); } catch(e) {}
        }
        const fmtDate = d => d && String(d).length === 8 ? `${String(d).slice(0,4)}.${String(d).slice(4,6)}.${String(d).slice(6,8)}` : (d || '');
        const enc = encodeURIComponent(q);
        const LAW_HEADERS = {
          'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
          'Accept': 'application/json, text/plain, */*',
          'Referer': 'https://www.law.go.kr/',
          'Accept-Language': 'ko-KR,ko;q=0.9'
        };
        const lawFetch = (t, params) =>
          fetchTimeout(`https://www.law.go.kr/DRF/lawSearch.do?OC=${OC}&target=${t}&type=JSON&query=${enc}&${params}`, { headers: LAW_HEADERS }, 15000);
        const tasks = [];
        if (target === 'all' || target === 'law')  tasks.push(['law',  lawFetch('law',  'display=10&sort=efYd')]);
        if (target === 'all' || target === 'prec')  tasks.push(['prec', lawFetch('prec', 'display=10&sort=date')]);
        if (target === 'all' || target === 'expc')  tasks.push(['expc', lawFetch('expc', 'display=10')]);

        // 결과 Promise를 Map에 저장 → 동시 요청은 이 Promise를 공유
        const workPromise = (async () => {
        const settled = await Promise.allSettled(tasks.map(([, f]) => f));
        let laws = [], precs = [], expcs = [], apiDebug = [];
        for (let i = 0; i < tasks.length; i++) {
          const [type] = tasks[i]; const res = settled[i];
          if (res.status !== 'fulfilled') { apiDebug.push(`${type}:fetch_fail`); continue; }
          const rawText = await res.value.text().catch(() => '');
          if (!res.value.ok) { apiDebug.push(`${type}:HTTP${res.value.status}`); continue; }
          let d;
          try { d = JSON.parse(rawText); } catch(e) { apiDebug.push(`${type}:json_fail:${rawText.slice(0,50)}`); continue; }
          if (type === 'law') {
            // LawSearch.law 구조 (공공데이터포털 API 가이드 기준)
            // 법령일련번호 = lawService.do?MST= 에 쓰이는 값 (법령상세링크에서도 확인)
            const root = d?.LawSearch || {};
            const arr = root.law || [];
            laws = (Array.isArray(arr) ? arr : arr ? [arr] : []).map(l => {
              const link = l['법령상세링크'] || '';
              // 법령상세링크에서 MST 직접 추출 (가장 신뢰도 높음)
              const mstFromLink = (link.match(/MST=(\d+)/) || [])[1] || '';
              return {
                name: l['법령명한글'] || l['법령명'] || '', dept: l['소관부처명'] || '',
                date: fmtDate(l['시행일자'] || l['공포일자'] || ''),
                id: l['법령ID'] || l['법령일련번호'] || '',
                // 법령일련번호 = MST (API 문서 기준)
                mst: mstFromLink || l['법령일련번호'] || l['MST'] || l['법령MST번호'] || ''
              };
            }).filter(l => l.name);
            if (!laws.length) apiDebug.push(`law:empty:${JSON.stringify(root).slice(0,120)}`);
          } else if (type === 'prec') {
            const arr = d?.PrecSearch?.prec || [];
            precs = (Array.isArray(arr) ? arr : arr ? [arr] : []).map(p => ({
              name: p['사건명'] || '', num: p['사건번호'] || '',
              court: p['법원명'] || '', date: fmtDate(p['선고일자'] || ''), id: p['판례일련번호'] || ''
            })).filter(p => p.name);
          } else if (type === 'expc') {
            const arr = d?.ExpCSearch?.expc || d?.ExpcSearch?.expc || [];
            expcs = (Array.isArray(arr) ? arr : arr ? [arr] : []).map(e => ({
              name: e['해석례명'] || e['제목'] || '', dept: e['소관부처명'] || e['소관부처'] || '',
              date: fmtDate(e['회신일자'] || e['시행일자'] || ''), id: e['해석례일련번호'] || e['일련번호'] || ''
            })).filter(e => e.name);
          }
        }
        const result = { laws, precs, expcs, query: q, oc: OC, debug: apiDebug };
          return result;
        })();
        LAW_INFLIGHT.set(cacheKey, workPromise);
        workPromise.finally(() => LAW_INFLIGHT.delete(cacheKey));
        const result = await workPromise;
        const now = Math.floor(Date.now() / 1000);
        if (result.laws?.length || result.precs?.length || result.expcs?.length) {
          await env.DB.prepare('INSERT INTO news_cache(category,data,cached_at) VALUES(?,?,?) ON CONFLICT(category) DO UPDATE SET data=?,cached_at=?')
            .bind(cacheKey, JSON.stringify(result), now, JSON.stringify(result), now).run();
        }
        return json(result);
      }

      // ── 법령/판례 본문 조회 (모바일 HTML) ──
      if (p === '/api/law-content' && m === 'GET') {
        const OC = env.LAW_OC || 'STEP-OPENAPI';
        const id = url.searchParams.get('id') || '';
        const mst = url.searchParams.get('mst') || '';
        const lawtype = url.searchParams.get('lawtype') || 'law'; // law | prec
        if (!id && !mst) return json({ error: 'id 필요' }, 400);
        const cacheKey = `lawxml1_${lawtype}_${id||mst}`;
        const cached = await env.DB.prepare('SELECT data, cached_at FROM news_cache WHERE category=?').bind(cacheKey).first();
        if (cached) {
          try { const r = JSON.parse(cached.data); if(r.html && r.html.length > 50 && !r.error) return json(r); } catch (e) {}
        }
        const lawGoFallbackUrl = lawtype === 'prec'
          ? `https://www.law.go.kr/precedInfoP.do?mode=0&precSeq=${encodeURIComponent(id)}`
          : mst ? `https://www.law.go.kr/lsEfInfoP.do?lsiSeq=${encodeURIComponent(mst)}`
          : id ? `https://www.law.go.kr/lsInfoP.do?lsiSeq=${encodeURIComponent(id)}` : '';
        const xhdr = {
          'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
          'Accept': 'application/xml,text/xml,*/*',
          'Referer': 'https://www.law.go.kr/',
          'Accept-Language': 'ko-KR,ko;q=0.9'
        };
        // XML 오류/무결과 판별
        const isXmlError = (s) => /일치하는.*없습니다|법령명을 확인|Error code|SSL handshake|cloudflare/i.test(s.slice(0, 3000));
        // 태그 내용 추출 + 태그 제거 유틸
        const stripCdata = (s) => s.replace(/<!\[CDATA\[([\s\S]*?)\]\]>/g, '$1');
        const strip = (s) => stripCdata(s).replace(/<[^>]+>/g,'').replace(/&lt;/g,'<').replace(/&gt;/g,'>').replace(/&amp;/g,'&').replace(/&nbsp;/g,' ').trim();
        const xtag = (xml, t) => { const m = xml.match(new RegExp(`<${t}[^>]*>([\\s\\S]*?)<\\/${t}>`, 'i')); return m ? m[1] : ''; };
        // XML → HTML 변환
        const xmlToHtml = (xml) => {
          // 판례
          if (/<PrecService|<판시사항|<판결요지/i.test(xml)) {
            const fields = [['사건명','사건명'],['사건번호','사건번호'],['선고일자','선고'],['법원명','법원'],
              ['판시사항','판시사항'],['판결요지','판결요지'],['참조조문','참조 조문'],
              ['참조판례','참조 판례'],['판결이유','판결이유'],['주문','주문']];
            let h = '';
            for (const [t, label] of fields) {
              const v = strip(xtag(xml, t));
              if (v) h += `<div style="margin:12px 0 0"><strong style="font-size:13px;color:#555">${label}</strong><div style="margin-top:4px;line-height:1.8;white-space:pre-wrap;font-size:14px">${v}</div></div><hr style="border:none;border-top:1px solid #eee;margin:12px 0 0">`;
            }
            return h;
          }
          // 법령 - 조문단위 (아코디언: 제목만 표시, 탭하면 내용 펼침)
          const units = [...xml.matchAll(/<조문단위[^>]*>([\s\S]*?)<\/조문단위>/gi)];
          if (units.length) {
            return units.map((u, i) => {
              const num   = strip(xtag(u[1], '조문번호'));
              const title = strip(xtag(u[1], '조문제목'));
              const body  = strip(xtag(u[1], '조문내용'));
              const paras = [...u[1].matchAll(/<항[^>]*>([\s\S]*?)<\/항>/gi)].map(a => {
                const pn = strip(xtag(a[1], '항번호'));
                const pc = strip(xtag(a[1], '항내용'));
                return pn||pc ? `<div style="margin:4px 0 0 14px;line-height:1.7;color:#333">${pn} ${pc}</div>` : '';
              }).join('');
              const hasBody = body || paras;
              const bodyHtml = hasBody ? `<div class="la-body" style="display:none;padding:10px 0 4px;border-top:1px solid #e8e8e8;margin-top:8px">${body?`<div style="line-height:1.8;font-size:14px;color:#222">${body}</div>`:''}${paras}</div>` : '';
              return `<div class="la-item" onclick="toggleLawArticle(this)" style="padding:11px 14px;border-left:3px solid var(--green,#4caf50);margin:6px 0;background:#f9fafb;border-radius:0 6px 6px 0;cursor:${hasBody?'pointer':'default'}">
                <div style="display:flex;justify-content:space-between;align-items:center">
                  <strong style="font-size:14px;color:#111">제${num}조${title?` <span style="font-weight:400;color:#555">(${title})</span>`:''}</strong>
                  ${hasBody?`<span class="la-arr" style="font-size:11px;color:#aaa;transition:transform .2s">▼</span>`:''}
                </div>${bodyHtml}
              </div>`;
            }).join('');
          }
          // fallback: <조문> 직접 구조 (조문단위 없이 조문번호/내용이 바로 있는 경우)
          const joNums = [...xml.matchAll(/<조문번호[^>]*>([\s\S]*?)<\/조문번호>/gi)];
          if (joNums.length) {
            // 조문단위가 없으면 전체 XML에서 조문번호/제목/내용 시퀀스로 파싱
            const numRe = /<조문번호[^>]*>([\s\S]*?)<\/조문번호>/gi;
            const blocks = [];
            let m2;
            const xmlNorm = xml;
            // 조문번호 위치를 기준으로 분할
            const positions = [];
            let mm;
            const re2 = /<조문번호[^>]*>/gi;
            while ((mm = re2.exec(xmlNorm)) !== null) positions.push(mm.index);
            positions.forEach((pos, idx) => {
              const chunk = xmlNorm.slice(pos, positions[idx+1] || pos + 3000);
              const num   = strip(xtag(chunk, '조문번호'));
              const title = strip(xtag(chunk, '조문제목'));
              const body  = strip(xtag(chunk, '조문내용'));
              if (num) blocks.push({ num, title, body, paras: '' });
            });
            if (blocks.length) return blocks.map(b => {
              const hasBody = b.body;
              const bodyHtml = hasBody ? `<div class="la-body" style="display:none;padding:10px 0 4px;border-top:1px solid #e8e8e8;margin-top:8px"><div style="line-height:1.8;font-size:14px;color:#222">${b.body}</div></div>` : '';
              return `<div class="la-item" onclick="toggleLawArticle(this)" style="padding:11px 14px;border-left:3px solid var(--green,#4caf50);margin:6px 0;background:#f9fafb;border-radius:0 6px 6px 0;cursor:${hasBody?'pointer':'default'}">
                <div style="display:flex;justify-content:space-between;align-items:center">
                  <strong style="font-size:14px;color:#111">제${b.num}조${b.title?` <span style="font-weight:400;color:#555">(${b.title})</span>`:''}</strong>
                  ${hasBody?`<span class="la-arr" style="font-size:11px;color:#aaa;transition:transform .2s">▼</span>`:''}
                </div>${bodyHtml}
              </div>`;
            }).join('');
          }
          return '';
        };
        // XML 전용 시도 목록 (HTML은 gzip/EUC-KR 문제로 제외)
        const attempts = [];
        if (lawtype === 'prec') {
          if (id) attempts.push(`https://www.law.go.kr/DRF/lawService.do?OC=${OC}&target=prec&ID=${id}&type=XML`);
        } else {
          if (mst) attempts.push(`https://www.law.go.kr/DRF/lawService.do?OC=${OC}&target=law&MST=${mst}&type=XML`);
          if (id)  attempts.push(`https://www.law.go.kr/DRF/lawService.do?OC=${OC}&target=law&ID=${id}&type=XML`);
          if (id)  attempts.push(`https://www.law.go.kr/DRF/lawService.do?OC=${OC}&target=eflaw&ID=${id}&type=XML`);
        }
        let html = '';
        const debug = ['trying_fetch'];
        for (const apiUrl of attempts) {
          try {
            const res = await fetchTimeout(apiUrl, { headers: xhdr }, 12000);
            // 대형 법령 XML 타임아웃 방지: 500KB 이상이면 앞부분만 사용
            const MAX_XML = 1500 * 1024;
            let raw;
            const ct = res.headers.get('content-length');
            if (ct && parseInt(ct) > MAX_XML) {
              const reader = res.body.getReader();
              const chunks = []; let total = 0;
              while (total < MAX_XML) {
                const { done, value } = await reader.read();
                if (done) break;
                chunks.push(value); total += value.length;
              }
              reader.cancel();
              raw = new TextDecoder().decode(await new Blob(chunks).arrayBuffer());
            } else {
              raw = await res.text();
            }
            const tgt = apiUrl.match(/target=(\w+)/)?.[1] || '?';
            debug.push(`${tgt}:${res.status}:${raw.length}chars`);
            if (raw.length < 100 || isXmlError(raw)) { debug.push(`filtered:${raw.slice(0,60)}`); continue; }
            const converted = xmlToHtml(raw);
            if (converted.length > 50) { html = converted; break; }
            debug.push(`parse_fail:${raw.slice(0,120)}`);
          } catch(e) { debug.push(`fail:${e.message}`); }
        }
        if (!html && lawGoFallbackUrl) {
          html = `<div style="padding:24px 16px;text-align:center"><div style="font-size:14px;color:#666;margin-bottom:16px;line-height:1.6">본문을 직접 불러오지 못했습니다.<br>법제처 사이트에서 확인해 주세요.</div><a href="${lawGoFallbackUrl}" target="_blank" rel="noopener" style="display:inline-block;padding:10px 24px;background:#37c272;color:#fff;border-radius:8px;font-size:14px;font-weight:600;text-decoration:none">⚖️ 법제처에서 보기 →</a></div>`;
        }
        const result = { html, id, lawtype, debug };
        const now = Math.floor(Date.now() / 1000);
        if (html.length > 200 && !html.includes('법제처에서 보기')) {
          await env.DB.prepare('INSERT INTO news_cache(category,data,cached_at) VALUES(?,?,?) ON CONFLICT(category) DO UPDATE SET data=?,cached_at=?')
            .bind(cacheKey, JSON.stringify(result), now, JSON.stringify(result), now).run();
        }
        return json(result);
      }

      // ── 법률 AI 질문 ──
      if (p === '/api/law-ask' && m === 'POST') {
        const b = await request.json();
        const question = (b.question || '').trim().slice(0, 500);
        const precContext = (b.precContext || '').trim().slice(0, 4000);
        if (!question) return json({ error: '질문을 입력해주세요.' }, 400);
        const OC = env.LAW_OC || 'STEP-OPENAPI';
        const enc = encodeURIComponent(question.slice(0, 50));
        const fmtDate = d => d && d.length === 8 ? `${d.slice(0,4)}.${d.slice(4,6)}.${d.slice(6,8)}` : (d || '');
        const ASK_HEADERS = {
          'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
          'Accept': 'application/json, text/plain, */*',
          'Referer': 'https://www.law.go.kr/', 'Accept-Language': 'ko-KR,ko;q=0.9'
        };
        const [lawRes, precRes, expcRes] = await Promise.allSettled([
          fetchTimeout(`https://www.law.go.kr/DRF/lawSearch.do?OC=${OC}&target=eflaw&type=JSON&query=${enc}&nw=3&display=5&sort=efYd`, { headers: ASK_HEADERS }, 9000),
          fetchTimeout(`https://www.law.go.kr/DRF/lawSearch.do?OC=${OC}&target=prec&type=JSON&query=${enc}&display=5&sort=date`, { headers: ASK_HEADERS }, 9000),
          fetchTimeout(`https://www.law.go.kr/DRF/lawSearch.do?OC=${OC}&target=expc&type=JSON&query=${enc}&display=3`, { headers: ASK_HEADERS }, 9000)
        ]);
        let sources = [], context = '';
        if (lawRes.status === 'fulfilled' && lawRes.value.ok) {
          try {
            const d = await lawRes.value.json();
            const root = d?.LawSearch || d?.EflawSearch || {};
            const rawArr = root.law || [];
            const arr = (Array.isArray(rawArr) ? rawArr : rawArr ? [rawArr] : []).filter(l => l['법령명한글'] || l['법령명']);
            if (arr.length) {
              context += '【관련 법령】\n' + arr.map(l => `- ${l['법령명한글']||l['법령명']} (시행: ${fmtDate(l['시행일자']||'')})`).join('\n') + '\n\n';
              arr.forEach(l => sources.push({ type: 'law', name: l['법령명한글']||l['법령명'], id: l['법령일련번호'] }));
            }
          } catch (e) {}
        }
        if (precRes.status === 'fulfilled' && precRes.value.ok) {
          try {
            const d = await precRes.value.json();
            const arr = (Array.isArray(d?.PrecSearch?.prec) ? d.PrecSearch.prec : d?.PrecSearch?.prec ? [d.PrecSearch.prec] : []).filter(p => p['사건명']);
            if (arr.length) {
              context += '【관련 판례】\n' + arr.map(p => `- ${p['사건명']} (${p['법원명']||''} ${fmtDate(p['선고일자']||'')})`).join('\n') + '\n\n';
              arr.forEach(p => sources.push({ type: 'prec', name: p['사건명'], num: p['사건번호'], court: p['법원명'], date: fmtDate(p['선고일자']||'') }));
            }
          } catch (e) {}
        }
        if (expcRes.status === 'fulfilled' && expcRes.value.ok) {
          try {
            const d = await expcRes.value.json();
            const arr = (Array.isArray(d?.ExpCSearch?.expc) ? d.ExpCSearch.expc : d?.ExpCSearch?.expc ? [d.ExpCSearch.expc] : []).filter(e => e['해석례명']||e['제목']);
            if (arr.length) {
              context += '【관련 해석례】\n' + arr.map(e => `- ${e['해석례명']||e['제목']||''}`).join('\n') + '\n\n';
              arr.forEach(e => sources.push({ type: 'expc', name: e['해석례명']||e['제목']||'' }));
            }
          } catch (e) {}
        }
        const precSection = precContext ? `【판례·해석례 본문 (직접 제공)】\n${precContext}\n\n` : '';
        const sysPrompt = '당신은 대한민국 노동법 전문가입니다. 법률 용어를 일반인이 이해하기 쉽게 풀어 설명하고, 마크다운 없이 일반 텍스트로 작성하세요. 마지막에 "더 정확한 내용은 전문 노무사·변호사와 상담을 권합니다." 문구를 추가하세요.';
        const userMsg = `아래 자료를 참고하여 질문에 답변하세요.\n\n${precSection}${context || ''}${!precSection&&!context?'(관련 법령 정보를 찾지 못했습니다. 일반 지식으로 답변합니다.)\n\n':''}질문: ${question}\n\n${precContext?'제공된 판례·해석례 내용을 중심으로 쉽게 풀어서 설명하세요.':'관련 법령/판례를 인용하고 법 조항 번호를 명시하세요 (예: 근로기준법 제56조).'}`;
        const aiResult = await callAI(sysPrompt, userMsg, env, { type: 'qa', maxTokens: 8192 });
        if (!aiResult) return json({ error: 'AI 서비스를 사용할 수 없습니다. 잠시 후 다시 시도해주세요.' }, 503);
        return json({ answer: aiResult.text, sources, model: aiResult.model });
      }

      // ── 글 목록 (커서 기반 페이지네이션) ──
      if (p === '/api/posts' && m === 'GET') {
        const limit = Math.min(parseInt(url.searchParams.get('limit') || '20'), 50);
        const before = parseInt(url.searchParams.get('before') || '0');
        const baseSQL = 'SELECT p.*, COALESCE(cc.cnt,0)+COALESCE(rc.rcnt,0) as comment_count, pk.keyword FROM posts p LEFT JOIN (SELECT post_id, COUNT(*) as cnt FROM comments GROUP BY post_id) cc ON p.id=cc.post_id LEFT JOIN (SELECT c.post_id, COUNT(*) as rcnt FROM comment_replies cr JOIN comments c ON cr.comment_id=c.id GROUP BY c.post_id) rc ON p.id=rc.post_id LEFT JOIN post_keywords pk ON p.id=pk.post_id';
        const rows = before > 0
          ? await env.DB.prepare(baseSQL + ' WHERE p.created_at < ? ORDER BY p.created_at DESC LIMIT ?').bind(before, limit + 1).all()
          : await env.DB.prepare(baseSQL + ' ORDER BY p.created_at DESC LIMIT ?').bind(limit + 1).all();
        const items = rows.results || [];
        const has_more = items.length > limit;
        if (has_more) items.pop();
        return json({ posts: items.map(r => ({ ...r, blocks: JSON.parse(r.blocks) })), has_more, next_cursor: has_more ? items[items.length - 1].created_at : null });
      }

      // ── 글 작성 ──
      if (p === '/api/posts' && m === 'POST') {
        const b = await request.json();
        const id = 'post_' + Date.now();
        const now = Math.floor(Date.now() / 1000);
        await env.DB.prepare('INSERT INTO posts(id,author,blocks,created_at,mode) VALUES(?,?,?,?,?)')
          .bind(id, b.author, JSON.stringify(b.blocks), now, b.mode||'normal').run();
        if (b.keyword) {
          await env.DB.prepare('INSERT INTO post_keywords(post_id,keyword) VALUES(?,?) ON CONFLICT(post_id) DO UPDATE SET keyword=?')
            .bind(id, b.keyword, b.keyword).run();
        }
        ctx.waitUntil(addMileageDB(env, b.author, 2));
        return json({ id });
      }

      // ── 글 검색 (전체 DB) ──
      if (p === '/api/posts/search' && m === 'GET') {
        const q = (url.searchParams.get('q') || '').trim();
        if (!q) return json({ posts: [] });
        const limit = Math.min(parseInt(url.searchParams.get('limit') || '30'), 50);
        const like = `%${q}%`;
        const rows = await env.DB.prepare(
          `SELECT p.id, p.author, p.blocks, p.created_at, p.like_count, p.mode, pk.keyword
           FROM posts p
           LEFT JOIN post_keywords pk ON p.id=pk.post_id
           WHERE p.author LIKE ? OR pk.keyword LIKE ? OR p.blocks LIKE ?
           ORDER BY p.created_at DESC LIMIT ?`
        ).bind(like, like, like, limit).all();
        return json({ posts: (rows.results || []).map(r => ({ ...r, blocks: JSON.parse(r.blocks) })) });
      }

      // ── 글 수정 ──
      if (p.match(/^\/api\/posts\/[^/]+$/) && m === 'PUT') {
        const id = p.split('/')[3];
        const b = await request.json();
        await env.DB.prepare('UPDATE posts SET blocks=?,mode=? WHERE id=?').bind(JSON.stringify(b.blocks), b.mode||'normal', id).run();
        if (b.keyword !== undefined) {
          if (b.keyword) {
            await env.DB.prepare('INSERT INTO post_keywords(post_id,keyword) VALUES(?,?) ON CONFLICT(post_id) DO UPDATE SET keyword=?')
              .bind(id, b.keyword, b.keyword).run();
          } else {
            await env.DB.prepare('DELETE FROM post_keywords WHERE post_id=?').bind(id).run();
          }
        }
        return json({ ok: true });
      }

      // ── 글 삭제 ──
      if (p.match(/^\/api\/posts\/[^/]+$/) && m === 'DELETE') {
        const id = p.split('/')[3];
        await env.DB.prepare('DELETE FROM posts WHERE id=?').bind(id).run();
        await env.DB.prepare('DELETE FROM comments WHERE post_id=?').bind(id).run();
        await env.DB.prepare('DELETE FROM likes WHERE post_id=?').bind(id).run();
        return json({ ok: true });
      }

      // ── 좋아요 목록 ──
      if (p === '/api/likes' && m === 'GET') {
        const userId = url.searchParams.get('user_id');
        if (!userId) return json([]);
        const rows = await env.DB.prepare('SELECT post_id FROM likes WHERE user_id=?').bind(userId).all();
        return json(rows.results.map(r => r.post_id));
      }

      // ── 좋아요 토글 ──
      if (p.match(/^\/api\/posts\/[^/]+\/like$/) && m === 'POST') {
        const postId = p.split('/')[3];
        const { user_id } = await request.json();
        const existing = await env.DB.prepare('SELECT 1 FROM likes WHERE post_id=? AND user_id=?').bind(postId, user_id).first();
        if (existing) {
          await env.DB.prepare('DELETE FROM likes WHERE post_id=? AND user_id=?').bind(postId, user_id).run();
          await env.DB.prepare('UPDATE posts SET like_count=MAX(0,like_count-1) WHERE id=?').bind(postId).run();
          return json({ liked: false });
        } else {
          await env.DB.prepare('INSERT INTO likes(post_id,user_id) VALUES(?,?)').bind(postId, user_id).run();
          await env.DB.prepare('UPDATE posts SET like_count=like_count+1 WHERE id=?').bind(postId).run();
          ctx.waitUntil(env.DB.prepare('SELECT author FROM posts WHERE id=?').bind(postId).first()
            .then(post => { if (post && post.author !== user_id) return addMileageDB(env, post.author, 0.5); }));
          return json({ liked: true });
        }
      }

      // ── 댓글 목록 (대댓글 포함) ──
      if (p.match(/^\/api\/posts\/[^/]+\/comments$/) && m === 'GET') {
        const postId = p.split('/')[3];
        const userId = url.searchParams.get('user_id');
        const [rows, likedRows, allReplies] = await Promise.all([
          env.DB.prepare('SELECT c.*, COALESCE(cl.cnt,0) as like_count FROM comments c LEFT JOIN (SELECT comment_id, COUNT(*) as cnt FROM comment_likes GROUP BY comment_id) cl ON c.id=cl.comment_id WHERE c.post_id=? ORDER BY c.created_at ASC').bind(postId).all(),
          userId ? env.DB.prepare('SELECT comment_id FROM comment_likes WHERE user_id=?').bind(userId).all() : Promise.resolve({ results: [] }),
          env.DB.prepare('SELECT * FROM comment_replies WHERE comment_id IN (SELECT id FROM comments WHERE post_id=?) ORDER BY created_at ASC').bind(postId).all(),
        ]);
        const likedSet = new Set(likedRows.results.map(r => r.comment_id));
        const repliesByComment = {};
        for (const r of allReplies.results) {
          if (!repliesByComment[r.comment_id]) repliesByComment[r.comment_id] = [];
          repliesByComment[r.comment_id].push(r);
        }
        return json(rows.results.map(r => ({
          ...r,
          user_liked: likedSet.has(r.id),
          reply_count: (repliesByComment[r.id] || []).length,
          replies: repliesByComment[r.id] || [],
        })));
      }

      // ── 댓글 작성 ──
      if (p.match(/^\/api\/posts\/[^/]+\/comments$/) && m === 'POST') {
        const postId = p.split('/')[3];
        const b = await request.json();
        const id = 'cmt_' + Date.now() + '_' + Math.random().toString(36).slice(2, 6);
        const now = Math.floor(Date.now() / 1000);
        await env.DB.prepare('INSERT INTO comments(id,post_id,author,content,created_at) VALUES(?,?,?,?,?)')
          .bind(id, postId, b.author, b.content, now).run();
        ctx.waitUntil(addMileageDB(env, b.author, 1));
        return json({ id, post_id: postId, author: b.author, content: b.content, created_at: now });
      }

      // ── 대댓글 삭제 ──
      if (p.match(/^\/api\/replies\/[^/]+$/) && m === 'DELETE') {
        const id = p.split('/')[3];
        await env.DB.prepare('DELETE FROM comment_replies WHERE id=?').bind(id).run();
        return json({ ok: true });
      }

      // ── 댓글 삭제 ──
      if (p.match(/^\/api\/comments\/[^/]+$/) && m === 'DELETE') {
        const id = p.split('/')[3];
        await env.DB.prepare('DELETE FROM comments WHERE id=?').bind(id).run();
        await env.DB.prepare('DELETE FROM comment_replies WHERE comment_id=?').bind(id).run();
        await env.DB.prepare('DELETE FROM comment_likes WHERE comment_id=?').bind(id).run();
        return json({ ok: true });
      }

      // ── 댓글 좋아요 ──
      if (p.match(/^\/api\/comments\/[^/]+\/like$/) && m === 'POST') {
        const commentId = p.split('/')[3];
        const { user_id, comment_author } = await request.json();
        const existing = await env.DB.prepare('SELECT 1 FROM comment_likes WHERE comment_id=? AND user_id=?').bind(commentId, user_id).first();
        if (existing) {
          await env.DB.prepare('DELETE FROM comment_likes WHERE comment_id=? AND user_id=?').bind(commentId, user_id).run();
        } else {
          await env.DB.prepare('INSERT INTO comment_likes(comment_id,user_id) VALUES(?,?)').bind(commentId, user_id).run();
          if (comment_author && comment_author !== user_id) ctx.waitUntil(addMileageDB(env, comment_author, 0.5));
        }
        const cnt = await env.DB.prepare('SELECT COUNT(*) as c FROM comment_likes WHERE comment_id=?').bind(commentId).first();
        return json({ liked: !existing, count: cnt?.c || 0 });
      }

      // ── 답글 목록 ──
      if (p.match(/^\/api\/comments\/[^/]+\/replies$/) && m === 'GET') {
        const commentId = p.split('/')[3];
        const rows = await env.DB.prepare('SELECT * FROM comment_replies WHERE comment_id=? ORDER BY created_at ASC').bind(commentId).all();
        return json(rows.results);
      }

      // ── 답글 작성 ──
      if (p.match(/^\/api\/comments\/[^/]+\/replies$/) && m === 'POST') {
        const commentId = p.split('/')[3];
        const { author, content } = await request.json();
        const id = 'rep_' + Date.now() + '_' + Math.random().toString(36).slice(2, 6);
        const now = Math.floor(Date.now() / 1000);
        await env.DB.prepare('INSERT INTO comment_replies(id,comment_id,author,content,created_at) VALUES(?,?,?,?,?)')
          .bind(id, commentId, author, content, now).run();
        ctx.waitUntil(addMileageDB(env, author, 1));
        return json({ id, comment_id: commentId, author, content, created_at: now });
      }

      // ── 서부소식 (경조사) ──
      if (p === '/api/events' && m === 'GET') {
        const rows = await env.DB.prepare('SELECT * FROM events ORDER BY created_at DESC').all();
        return json(rows.results);
      }
      if (p === '/api/events' && m === 'POST') {
        const b = await request.json();
        const id = 'evt_' + Date.now();
        await env.DB.prepare('INSERT INTO events(id,author,type,title,content,tagged_user,created_at) VALUES(?,?,?,?,?,?,?)')
          .bind(id, b.author, b.type || '기타', b.title, b.content || '', b.tagged_user || '', Math.floor(Date.now() / 1000)).run();
        return json({ id });
      }
      if (p.match(/^\/api\/events\/[^/]+$/) && m === 'DELETE') {
        const id = p.split('/')[3];
        const t = url.searchParams.get('token') || request.headers.get('Authorization')?.replace('Bearer ', '');
        const s = t ? await env.DB.prepare('SELECT user_id FROM sessions WHERE token=?').bind(t).first() : null;
        if (!s) return json({ error: 'unauthorized' }, 401);
        const ro = await env.DB.prepare('SELECT role FROM user_roles WHERE user_id=?').bind(s.user_id).first();
        if (ro?.role !== 'admin') {
          const evt = await env.DB.prepare('SELECT author FROM events WHERE id=?').bind(id).first();
          if (!evt || evt.author !== s.user_id) return json({ error: 'forbidden' }, 403);
        }
        await env.DB.prepare('DELETE FROM events WHERE id=?').bind(id).run();
        return json({ ok: true });
      }

      // ── 칭찬합니다 (kudos) ──
      if (p === '/api/kudos' && m === 'GET') {
        const rows = await env.DB.prepare('SELECT * FROM kudos ORDER BY created_at DESC').all();
        return json(rows.results);
      }
      if (p === '/api/kudos' && m === 'POST') {
        const b = await request.json();
        const id = 'kudos_' + Date.now();
        await env.DB.prepare('INSERT INTO kudos(id,tag,source,content,added_by,user_target,created_at) VALUES(?,?,?,?,?,?,?)')
          .bind(id, b.tag, b.source || '', b.content, b.added_by, b.user_target || '', Math.floor(Date.now() / 1000)).run();
        return json({ id });
      }
      if (p.match(/^\/api\/kudos\/[^/]+$/) && m === 'DELETE') {
        const id = p.split('/')[3];
        await env.DB.prepare('DELETE FROM kudos WHERE id=?').bind(id).run();
        return json({ ok: true });
      }

      // ── 이달의 서부인 (monthly contests) ──
      if (p === '/api/contests' && m === 'GET') {
        const rows = await env.DB.prepare('SELECT * FROM monthly_contests ORDER BY created_at DESC').all();
        return json(rows.results);
      }
      if (p === '/api/contests' && m === 'POST') {
        const b = await request.json();
        const id = 'mc_' + Date.now();
        await env.DB.prepare('INSERT INTO monthly_contests(id,title,description,nominate_start,nominate_end,vote_start,vote_end,created_by,created_at) VALUES(?,?,?,?,?,?,?,?,?)')
          .bind(id, b.title, b.description || '', b.nominate_start || 0, b.nominate_end || 0, b.vote_start || 0, b.vote_end || 0, b.created_by, Math.floor(Date.now() / 1000)).run();
        return json({ id });
      }
      if (p.match(/^\/api\/contests\/[^/]+$/) && m === 'DELETE') {
        const id = p.split('/')[3];
        await env.DB.prepare('DELETE FROM monthly_contests WHERE id=?').bind(id).run();
        await env.DB.prepare('DELETE FROM nominations WHERE contest_id=?').bind(id).run();
        await env.DB.prepare('DELETE FROM nominee_msgs WHERE contest_id=?').bind(id).run();
        await env.DB.prepare('DELETE FROM contest_votes WHERE contest_id=?').bind(id).run();
        return json({ ok: true });
      }
      if (p.match(/^\/api\/contests\/[^/]+\/winner$/) && m === 'POST') {
        const id = p.split('/')[3];
        const { winner } = await request.json();
        await env.DB.prepare('UPDATE monthly_contests SET winner=? WHERE id=?').bind(winner, id).run();
        return json({ ok: true });
      }
      if (p.match(/^\/api\/contests\/[^/]+$/) && m === 'PUT') {
        const id = p.split('/')[3];
        const b = await request.json();
        const fields = [];
        const vals = [];
        if (b.title !== undefined) { fields.push('title=?'); vals.push(b.title); }
        if (b.winner !== undefined) { fields.push('winner=?'); vals.push(b.winner || null); }
        if (!fields.length) return json({ error: '변경 항목 없음' }, 400);
        vals.push(id);
        await env.DB.prepare(`UPDATE monthly_contests SET ${fields.join(',')} WHERE id=?`).bind(...vals).run();
        return json({ ok: true });
      }
      if (p.match(/^\/api\/contests\/[^/]+\/nominations$/) && m === 'GET') {
        const contestId = p.split('/')[3];
        const rows = await env.DB.prepare('SELECT * FROM nominations WHERE contest_id=? ORDER BY created_at ASC').bind(contestId).all();
        const msgs = await env.DB.prepare('SELECT * FROM nominee_msgs WHERE contest_id=? ORDER BY created_at ASC').bind(contestId).all();
        return json({ nominations: rows.results, msgs: msgs.results });
      }
      if (p.match(/^\/api\/contests\/[^/]+\/nominations$/) && m === 'POST') {
        const contestId = p.split('/')[3];
        const b = await request.json();
        const contest = await env.DB.prepare('SELECT * FROM monthly_contests WHERE id=?').bind(contestId).first();
        if (!contest) return json({ error: 'not found' }, 404);
        const now = Math.floor(Date.now() / 1000);
        if (now < contest.nominate_start || now > contest.nominate_end)
          return json({ error: '추천 기간이 아닙니다' }, 400);
        const year = new Date().getFullYear();
        const yearStart = Math.floor(new Date(year, 0, 1).getTime() / 1000);
        const yearEnd = Math.floor(new Date(year + 1, 0, 1).getTime() / 1000);
        const prevWin = await env.DB.prepare('SELECT 1 FROM monthly_contests WHERE winner=? AND created_at>=? AND created_at<?').bind(b.nominee, yearStart, yearEnd).first();
        if (prevWin) return json({ error: '올해 이미 수상한 분입니다' }, 400);
        const existing = await env.DB.prepare('SELECT id FROM nominations WHERE contest_id=? AND nominated_by=?').bind(contestId, b.nominated_by).first();
        if (existing) return json({ error: '이미 추천하셨습니다' }, 400);
        const id = 'nom_' + Date.now() + '_' + Math.random().toString(36).slice(2, 5);
        await env.DB.prepare('INSERT INTO nominations(id,contest_id,nominee,nominated_by,message,is_anonymous,created_at) VALUES(?,?,?,?,?,?,?)')
          .bind(id, contestId, b.nominee, b.nominated_by, b.message || '', b.is_anonymous ? 1 : 0, now).run();
        return json({ id });
      }
      if (p.match(/^\/api\/contests\/[^/]+\/msgs$/) && m === 'POST') {
        const contestId = p.split('/')[3];
        const b = await request.json();
        const existing = await env.DB.prepare("SELECT 1 FROM nominee_msgs WHERE contest_id=? AND json_extract(author_display,'$.voter')=?").bind(contestId, b.voter).first();
        if (existing) return json({ error: '이미 댓글을 작성하셨습니다' }, 400);
        const id = 'msg_' + Date.now() + '_' + Math.random().toString(36).slice(2, 5);
        const authorDisplay = b.is_anonymous ? '익명' : b.voter;
        await env.DB.prepare('INSERT INTO nominee_msgs(id,contest_id,nominee,content,author_display,created_at) VALUES(?,?,?,?,?,?)')
          .bind(id, contestId, b.nominee, b.content, JSON.stringify({ display: authorDisplay, voter: b.voter }), Math.floor(Date.now() / 1000)).run();
        return json({ id });
      }
      if (p.match(/^\/api\/contests\/[^/]+\/vote$/) && m === 'POST') {
        const contestId = p.split('/')[3];
        const { voter, nominee } = await request.json();
        const contest = await env.DB.prepare('SELECT * FROM monthly_contests WHERE id=?').bind(contestId).first();
        if (!contest) return json({ error: 'not found' }, 404);
        const now = Math.floor(Date.now() / 1000);
        if (now < contest.vote_start || now > contest.vote_end)
          return json({ error: '투표 기간이 아닙니다' }, 400);
        const existing = await env.DB.prepare('SELECT 1 FROM contest_votes WHERE contest_id=? AND voter=?').bind(contestId, voter).first();
        if (existing) return json({ error: '이미 투표하셨습니다' }, 400);
        await env.DB.prepare('INSERT INTO contest_votes(contest_id,voter,nominee) VALUES(?,?,?)').bind(contestId, voter, nominee).run();
        return json({ ok: true });
      }
      if (p.match(/^\/api\/contests\/[^/]+\/votes$/) && m === 'GET') {
        const contestId = p.split('/')[3];
        const voter = url.searchParams.get('voter');
        const rows = await env.DB.prepare('SELECT nominee, COUNT(*) as count FROM contest_votes WHERE contest_id=? GROUP BY nominee ORDER BY count DESC').bind(contestId).all();
        const myVote = voter ? (await env.DB.prepare('SELECT nominee FROM contest_votes WHERE contest_id=? AND voter=?').bind(contestId, voter).first())?.nominee : null;
        return json({ results: rows.results, my_vote: myVote });
      }

      // ── 사진 투표 행사 ──
      if (p === '/api/photo-contests' && m === 'GET') {
        const t = url.searchParams.get('token') || request.headers.get('Authorization')?.replace('Bearer ', '');
        const sess = t ? await env.DB.prepare('SELECT user_id FROM sessions WHERE token=?').bind(t).first() : null;
        const role = sess ? await env.DB.prepare('SELECT role FROM user_roles WHERE user_id=?').bind(sess.user_id).first() : null;
        const isAdmin = role?.role === 'admin' || role?.role === 'sub_admin';
        const grp = url.searchParams.get('group') || null;
        let rows;
        if (grp) {
          rows = isAdmin
            ? await env.DB.prepare("SELECT * FROM photo_contests WHERE contest_group=? ORDER BY created_at DESC").bind(grp).all()
            : await env.DB.prepare("SELECT * FROM photo_contests WHERE contest_group=? AND status!='draft' ORDER BY created_at DESC").bind(grp).all();
        } else {
          rows = isAdmin
            ? await env.DB.prepare("SELECT * FROM photo_contests ORDER BY created_at DESC").all()
            : await env.DB.prepare("SELECT * FROM photo_contests WHERE status!='draft' ORDER BY created_at DESC").all();
        }
        const list = rows.results || [];
        for (const c of list) {
          const ec = await env.DB.prepare("SELECT COUNT(*) as cnt FROM photo_entries WHERE contest_id=?").bind(c.id).first();
          c.entry_count = ec?.cnt || 0;
          const vc = await env.DB.prepare("SELECT COUNT(*) as cnt FROM photo_votes WHERE contest_id=?").bind(c.id).first();
          c.vote_count = vc?.cnt || 0;
        }
        return json(list);
      }

      if (p === '/api/photo-contests' && m === 'POST') {
        const t = url.searchParams.get('token') || request.headers.get('Authorization')?.replace('Bearer ', '');
        const sess = t ? await env.DB.prepare('SELECT user_id FROM sessions WHERE token=?').bind(t).first() : null;
        if (!sess) return json({ error: 'unauthorized' }, 401);
        const role = await env.DB.prepare('SELECT role FROM user_roles WHERE user_id=?').bind(sess.user_id).first();
        if (!['admin','sub_admin'].includes(role?.role)) return json({ error: 'forbidden' }, 403);
        const { title, description, contest_group } = await request.json();
        if (!title) return json({ error: '제목 필요' }, 400);
        const grpVal = contest_group === 'center' ? 'center' : 'branch';
        const id = 'pc_' + Date.now() + '_' + Math.random().toString(36).slice(2, 5);
        await env.DB.prepare("INSERT INTO photo_contests(id,title,description,status,contest_group,created_by,created_at) VALUES(?,?,?,'draft',?,?,?)").bind(id, title, description || '', grpVal, sess.user_id, Math.floor(Date.now() / 1000)).run();
        return json({ ok: true, id });
      }

      if (p.match(/^\/api\/photo-contests\/[^/]+\/status$/) && m === 'POST') {
        const cid = p.split('/')[3];
        const t = url.searchParams.get('token') || request.headers.get('Authorization')?.replace('Bearer ', '');
        const sess = t ? await env.DB.prepare('SELECT user_id FROM sessions WHERE token=?').bind(t).first() : null;
        if (!sess) return json({ error: 'unauthorized' }, 401);
        const role = await env.DB.prepare('SELECT role FROM user_roles WHERE user_id=?').bind(sess.user_id).first();
        if (!['admin','sub_admin'].includes(role?.role)) return json({ error: 'forbidden' }, 403);
        const { status } = await request.json();
        if (!['draft', 'open', 'closed'].includes(status)) return json({ error: 'invalid' }, 400);
        await env.DB.prepare("UPDATE photo_contests SET status=? WHERE id=?").bind(status, cid).run();
        return json({ ok: true });
      }

      if (p.match(/^\/api\/photo-contests\/[^/]+\/entries\/[^/]+$/) && m === 'DELETE') {
        const parts = p.split('/');
        const cid = parts[3], eid = parts[5];
        const t = url.searchParams.get('token') || request.headers.get('Authorization')?.replace('Bearer ', '');
        const sess = t ? await env.DB.prepare('SELECT user_id FROM sessions WHERE token=?').bind(t).first() : null;
        if (!sess) return json({ error: 'unauthorized' }, 401);
        const entry = await env.DB.prepare("SELECT * FROM photo_entries WHERE id=? AND contest_id=?").bind(eid, cid).first();
        if (!entry) return json({ error: 'not found' }, 404);
        const role = await env.DB.prepare('SELECT role FROM user_roles WHERE user_id=?').bind(sess.user_id).first();
        if (entry.uploader !== sess.user_id && role?.role !== 'admin') return json({ error: 'forbidden' }, 403);
        await env.DB.prepare("DELETE FROM photo_votes WHERE photo_id=?").bind(eid).run();
        await env.DB.prepare("DELETE FROM photo_entries WHERE id=?").bind(eid).run();
        return json({ ok: true });
      }

      if (p.match(/^\/api\/photo-contests\/[^/]+\/entries$/) && m === 'GET') {
        const cid = p.split('/')[3];
        const t = url.searchParams.get('token') || request.headers.get('Authorization')?.replace('Bearer ', '');
        const sess = t ? await env.DB.prepare('SELECT user_id FROM sessions WHERE token=?').bind(t).first() : null;
        const userId = sess?.user_id || null;
        const contest = await env.DB.prepare("SELECT * FROM photo_contests WHERE id=?").bind(cid).first();
        if (!contest) return json({ error: 'not found' }, 404);
        const eRows = await env.DB.prepare(
          `SELECT e.*, u.name as uploader_name, COALESCE(vc.cnt,0) as vote_count
           FROM photo_entries e LEFT JOIN users u ON e.uploader=u.id
           LEFT JOIN (SELECT photo_id, COUNT(*) as cnt FROM photo_votes WHERE contest_id=? GROUP BY photo_id) vc ON e.id=vc.photo_id
           WHERE e.contest_id=? ORDER BY e.created_at ASC`
        ).bind(cid, cid).all();
        const myVote = userId ? (await env.DB.prepare("SELECT photo_id FROM photo_votes WHERE contest_id=? AND voter=?").bind(cid, userId).first())?.photo_id : null;
        const revealed = contest.revealed === 1;
        const entries = (eRows.results || []).map(e => ({
          ...e,
          uploader_name: revealed ? e.uploader_name : '익명',
          is_mine: userId ? e.uploader === userId : false,
        }));
        return json({ contest, entries, my_vote: myVote });
      }

      if (p.match(/^\/api\/photo-contests\/[^/]+\/entries$/) && m === 'POST') {
        const cid = p.split('/')[3];
        const t = url.searchParams.get('token') || request.headers.get('Authorization')?.replace('Bearer ', '');
        const sess = t ? await env.DB.prepare('SELECT user_id FROM sessions WHERE token=?').bind(t).first() : null;
        if (!sess) return json({ error: 'unauthorized' }, 401);
        const contest = await env.DB.prepare("SELECT * FROM photo_contests WHERE id=?").bind(cid).first();
        if (!contest || contest.status !== 'open') return json({ error: '참여할 수 없는 행사입니다' }, 400);
        const { img_url, caption } = await request.json();
        if (!img_url) return json({ error: '이미지 필요' }, 400);
        const id = 'pe_' + Date.now() + '_' + Math.random().toString(36).slice(2, 5);
        await env.DB.prepare("INSERT INTO photo_entries(id,contest_id,uploader,img_url,caption,created_at) VALUES(?,?,?,?,?,?)").bind(id, cid, sess.user_id, img_url, caption || '', Math.floor(Date.now() / 1000)).run();
        return json({ ok: true, id });
      }

      if (p.match(/^\/api\/photo-contests\/[^/]+\/vote$/) && m === 'POST') {
        const cid = p.split('/')[3];
        const t = url.searchParams.get('token') || request.headers.get('Authorization')?.replace('Bearer ', '');
        const sess = t ? await env.DB.prepare('SELECT user_id FROM sessions WHERE token=?').bind(t).first() : null;
        if (!sess) return json({ error: 'unauthorized' }, 401);
        const contest = await env.DB.prepare("SELECT * FROM photo_contests WHERE id=?").bind(cid).first();
        if (!contest || contest.status !== 'open') return json({ error: '투표할 수 없는 행사입니다' }, 400);
        const { photo_id } = await request.json();
        const entry = await env.DB.prepare("SELECT uploader FROM photo_entries WHERE id=? AND contest_id=?").bind(photo_id, cid).first();
        if (!entry) return json({ error: 'not found' }, 404);
        if (entry.uploader === sess.user_id) return json({ error: '본인 사진에는 투표할 수 없습니다' }, 400);
        await env.DB.prepare("INSERT INTO photo_votes(contest_id,voter,photo_id) VALUES(?,?,?) ON CONFLICT(contest_id,voter) DO UPDATE SET photo_id=?").bind(cid, sess.user_id, photo_id, photo_id).run();
        return json({ ok: true });
      }

      if (p.match(/^\/api\/photo-contests\/[^/]+\/reveal$/) && m === 'POST') {
        const cid = p.split('/')[3];
        const t = url.searchParams.get('token') || request.headers.get('Authorization')?.replace('Bearer ', '');
        const sess = t ? await env.DB.prepare('SELECT user_id FROM sessions WHERE token=?').bind(t).first() : null;
        if (!sess) return json({ error: 'unauthorized' }, 401);
        const role = await env.DB.prepare('SELECT role FROM user_roles WHERE user_id=?').bind(sess.user_id).first();
        if (!['admin','sub_admin'].includes(role?.role)) return json({ error: 'forbidden' }, 403);
        await env.DB.prepare("UPDATE photo_contests SET revealed=1 WHERE id=?").bind(cid).run();
        return json({ ok: true });
      }

      if (p.match(/^\/api\/photo-contests\/[^/]+$/) && m === 'DELETE') {
        const cid = p.split('/')[3];
        const t = url.searchParams.get('token') || request.headers.get('Authorization')?.replace('Bearer ', '');
        const sess = t ? await env.DB.prepare('SELECT user_id FROM sessions WHERE token=?').bind(t).first() : null;
        if (!sess) return json({ error: 'unauthorized' }, 401);
        const role = await env.DB.prepare('SELECT role FROM user_roles WHERE user_id=?').bind(sess.user_id).first();
        if (!['admin','sub_admin'].includes(role?.role)) return json({ error: 'forbidden' }, 403);
        await env.DB.prepare("DELETE FROM photo_votes WHERE contest_id=?").bind(cid).run();
        await env.DB.prepare("DELETE FROM photo_entries WHERE contest_id=?").bind(cid).run();
        await env.DB.prepare("DELETE FROM photo_contests WHERE id=?").bind(cid).run();
        return json({ ok: true });
      }

      // ── 역할 관리 ──
      if (p === '/api/roles' && m === 'GET') {
        const rows = await env.DB.prepare('SELECT * FROM user_roles').all();
        return json(rows.results);
      }
      if (p.match(/^\/api\/roles\/[^/]+$/) && m === 'PUT') {
        const userId = decodeURIComponent(p.split('/')[3]);
        const { role } = await request.json();
        await env.DB.prepare('INSERT INTO user_roles(user_id,role) VALUES(?,?) ON CONFLICT(user_id) DO UPDATE SET role=?')
          .bind(userId, role, role).run();
        return json({ ok: true });
      }

      // ── 사용자 관리 ──
      if (p === '/api/users' && m === 'GET') {
        const rows = await env.DB.prepare('SELECT u.id, u.name, u.dept, u.status, u.created_at, up.last_seen FROM users u LEFT JOIN user_presence up ON u.id=up.user_id ORDER BY u.created_at ASC').all();
        return json(rows.results);
      }
      if (p === '/api/users' && m === 'POST') {
        const { id, name, password, dept } = await request.json();
        if (!id || !/^\d{9}$/.test(id)) return json({ error: '온나라 사번은 9자리 숫자입니다.' }, 400);
        if (!name || !name.trim()) return json({ error: '이름을 입력해주세요.' }, 400);
        const exists = await env.DB.prepare('SELECT 1 FROM users WHERE id=?').bind(id).first();
        if (exists) return json({ error: '이미 등록된 사번입니다.' }, 409);
        await env.DB.prepare('INSERT INTO users(id,name,dept,password,status,created_at) VALUES(?,?,?,?,?,?)')
          .bind(id, name.trim(), (dept||'').trim(), password || '1234', 'active', Math.floor(Date.now() / 1000)).run();
        await env.DB.prepare('INSERT INTO user_roles(user_id,role) VALUES(?,?) ON CONFLICT(user_id) DO NOTHING').bind(id, 'user').run();
        return json({ ok: true });
      }
      if (p === '/api/users/bulk' && m === 'POST') {
        const { users: list } = await request.json();
        if (!Array.isArray(list)) return json({ error: 'invalid' }, 400);
        const now = Math.floor(Date.now() / 1000);
        const valid = list.map(u => ({
          uid: (u.id || '').trim(), uname: (u.name || '').trim(),
          upw: (u.password || '1234').trim(), udept: (u.dept || '').trim()
        })).filter(u => u.uid && /^\d{9}$/.test(u.uid) && u.uname);
        if (!valid.length) return json({ ok: true, created: 0, skipped: list.length });
        // D1 파라미터 100개 제한 → 99개씩 청크
        const CHUNK = 99;
        const existingSet = new Set();
        for (let i = 0; i < valid.length; i += CHUNK) {
          const chunk = valid.slice(i, i + CHUNK);
          const ph = chunk.map(() => '?').join(',');
          const rows = await env.DB.prepare(`SELECT id FROM users WHERE id IN (${ph})`).bind(...chunk.map(u => u.uid)).all();
          (rows.results || []).forEach(r => existingSet.add(r.id));
        }
        const toInsert = valid.filter(u => !existingSet.has(u.uid));
        // batch도 50명(=100 statements)씩 나눠 처리
        for (let i = 0; i < toInsert.length; i += 50) {
          const chunk = toInsert.slice(i, i + 50);
          await env.DB.batch([
            ...chunk.map(u => env.DB.prepare('INSERT INTO users(id,name,dept,password,status,created_at) VALUES(?,?,?,?,?,?) ON CONFLICT(id) DO NOTHING').bind(u.uid, u.uname, u.udept, u.upw, 'active', now)),
            ...chunk.map(u => env.DB.prepare('INSERT INTO user_roles(user_id,role) VALUES(?,?) ON CONFLICT(user_id) DO NOTHING').bind(u.uid, 'user')),
          ]);
        }
        return json({ ok: true, created: toInsert.length, skipped: list.length - toInsert.length });
      }
      if (p.match(/^\/api\/users\/[^/]+\/reset-password$/) && m === 'PUT') {
        const userId = decodeURIComponent(p.split('/')[3]);
        await env.DB.prepare('UPDATE users SET password=? WHERE id=?').bind('1234', userId).run();
        await env.DB.prepare('DELETE FROM sessions WHERE user_id=?').bind(userId).run();
        return json({ ok: true });
      }
      if (p.match(/^\/api\/users\/[^/]+\/change-password$/) && m === 'PUT') {
        const userId = decodeURIComponent(p.split('/')[3]);
        const { token, old_password, new_password } = await request.json();
        const user = await env.DB.prepare('SELECT * FROM users WHERE id=?').bind(userId).first();
        if (!user) return json({ error: '사용자를 찾을 수 없습니다.' }, 404);
        if (token) {
          const sess = await env.DB.prepare('SELECT 1 FROM sessions WHERE token=? AND user_id=?').bind(token, userId).first();
          if (!sess) return json({ error: '인증 오류' }, 401);
        } else if (user.password !== old_password) {
          return json({ error: '현재 비밀번호가 올바르지 않습니다.' }, 400);
        }
        if (!new_password || new_password.length < 4) return json({ error: '새 비밀번호는 4자 이상이어야 합니다.' }, 400);
        if (new_password === '1234') return json({ error: '초기 비밀번호는 사용할 수 없습니다.' }, 400);
        await env.DB.prepare('UPDATE users SET password=? WHERE id=?').bind(new_password, userId).run();
        return json({ ok: true });
      }
      if (p.match(/^\/api\/users\/[^/]+\/dept$/) && m === 'PUT') {
        const userId = decodeURIComponent(p.split('/')[3]);
        const { dept } = await request.json();
        await env.DB.prepare('UPDATE users SET dept=? WHERE id=?').bind((dept||'').trim(), userId).run();
        return json({ ok: true });
      }
      if (p.match(/^\/api\/users\/[^/]+$/) && m === 'DELETE') {
        const userId = decodeURIComponent(p.split('/')[3]);
        if (userId === '관리자') return json({ error: '관리자는 삭제할 수 없습니다.' }, 400);
        await env.DB.batch([
          env.DB.prepare('DELETE FROM users WHERE id=?').bind(userId),
          env.DB.prepare('DELETE FROM sessions WHERE user_id=?').bind(userId),
          env.DB.prepare('DELETE FROM user_roles WHERE user_id=?').bind(userId),
          env.DB.prepare('DELETE FROM user_profiles WHERE user_id=?').bind(userId),
          env.DB.prepare('DELETE FROM user_mileage WHERE user_id=?').bind(userId),
          env.DB.prepare('DELETE FROM user_presence WHERE user_id=?').bind(userId),
        ]);
        return json({ ok: true });
      }

      // ── 로그인 / 세션 ──
      if (p === '/api/login' && m === 'POST') {
        const { id, password } = await request.json();
        const ip = request.headers.get('CF-Connecting-IP') || request.headers.get('X-Forwarded-For') || 'unknown';
        const ua = (request.headers.get('User-Agent') || '').slice(0, 200);
        const now = Math.floor(Date.now() / 1000);
        const logResult = async (uid, result) => {
          try {
            await env.DB.prepare('INSERT INTO login_logs(user_id,ip,user_agent,result,created_at) VALUES(?,?,?,?,?)').bind(uid || 'unknown', ip, ua, result, now).run();
            await env.DB.prepare('DELETE FROM login_logs WHERE created_at < (SELECT created_at FROM login_logs ORDER BY created_at DESC LIMIT 1 OFFSET 999)').run();
          } catch(e) {}
        };
        const user = await env.DB.prepare('SELECT * FROM users WHERE id=?').bind(id).first();
        if (!user || user.password !== password) { await logResult(id, 'fail'); return json({ error: '사번 또는 비밀번호가 올바르지 않습니다.' }, 401); }
        if (user.status === 'pending') { await logResult(id, 'pending'); return json({ error: '관리자 승인 대기 중입니다.' }, 403); }
        const token = crypto.randomUUID();
        await env.DB.prepare('INSERT INTO sessions(token,user_id,created_at) VALUES(?,?,?)').bind(token, id, now).run();
        ctx.waitUntil(Promise.all([
          env.DB.prepare('DELETE FROM sessions WHERE user_id=? AND token NOT IN (SELECT token FROM sessions WHERE user_id=? ORDER BY created_at DESC LIMIT 5)').bind(id, id).run(),
          logResult(id, 'ok'),
        ]));
        return json({ ok: true, id: user.id, name: user.name || user.id, dept: user.dept || '', token, must_change_password: user.password === '1234' });
      }
      if (p === '/api/verify-session' && m === 'POST') {
        const { token } = await request.json();
        if (!token) return json({ error: 'no token' }, 401);
        // sessions + users 단일 JOIN 쿼리
        const row = await env.DB.prepare(
          'SELECT s.created_at as sess_created, u.id, u.name, u.status, u.password, u.dept FROM sessions s JOIN users u ON s.user_id=u.id WHERE s.token=?'
        ).bind(token).first();
        if (!row) return json({ error: 'invalid' }, 401);
        const now = Math.floor(Date.now() / 1000);
        if (now - (row.sess_created || 0) > 3600) {
          ctx.waitUntil(env.DB.prepare('DELETE FROM sessions WHERE token=?').bind(token).run());
          return json({ error: 'expired' }, 401);
        }
        if (row.status !== 'active') return json({ error: 'user not found' }, 401);
        // 접속이력 기록 (하루 1회 throttle, 응답 블로킹 없음)
        const todayStart = now - (now % 86400);
        const ip = request.headers.get('CF-Connecting-IP') || request.headers.get('X-Forwarded-For') || 'unknown';
        const ua = request.headers.get('User-Agent') || '';
        ctx.waitUntil(
          env.DB.prepare('SELECT id FROM login_logs WHERE user_id=? AND result=? AND created_at>=?').bind(row.id, 'session', todayStart).first()
            .then(recentLog => { if (!recentLog) return env.DB.prepare('INSERT INTO login_logs(user_id,ip,user_agent,result,created_at) VALUES(?,?,?,?,?)').bind(row.id, ip, ua, 'session', now).run(); })
        );
        return json({ ok: true, id: row.id, name: row.name || row.id, dept: row.dept || '', must_change_password: row.password === '1234' });
      }
      if (p === '/api/sessions' && m === 'DELETE') {
        const { token } = await request.json();
        if (token) await env.DB.prepare('DELETE FROM sessions WHERE token=?').bind(token).run();
        return json({ ok: true });
      }

      // ── 알림 ──
      if (p === '/api/notifications' && m === 'GET') {
        const userId = url.searchParams.get('user_id');
        if (!userId) return json([]);
        const [comments, replies] = await Promise.all([
          env.DB.prepare('SELECT "comment" as type, c.author, substr(c.content,1,60) as excerpt, c.post_id as ref_id, c.created_at FROM comments c JOIN posts p ON c.post_id=p.id WHERE p.author=? AND c.author!=? ORDER BY c.created_at DESC LIMIT 10').bind(userId, userId).all(),
          env.DB.prepare('SELECT "reply" as type, r.author, substr(r.content,1,60) as excerpt, r.comment_id as ref_id, r.created_at FROM comment_replies r JOIN comments c ON r.comment_id=c.id WHERE c.author=? AND r.author!=? ORDER BY r.created_at DESC LIMIT 10').bind(userId, userId).all(),
        ]);
        const items = [...comments.results, ...replies.results].sort((a,b)=>(b.created_at||0)-(a.created_at||0)).slice(0,20);
        return json(items);
      }

      // ── 마일리지 ──
      if (p === '/api/mileage' && m === 'GET') {
        const rows = await env.DB.prepare('SELECT um.* FROM user_mileage um JOIN users u ON um.user_id=u.id ORDER BY um.points DESC').all();
        return json(rows.results);
      }

      // ── 프로필 ──
      if (p === '/api/profiles' && m === 'GET') {
        const rows = await env.DB.prepare('SELECT * FROM user_profiles').all();
        return json(rows.results);
      }
      if (p.match(/^\/api\/profile\/[^/]+\/activity$/) && m === 'GET') {
        const userId = decodeURIComponent(p.split('/')[3]);
        const [recentPosts, recentComments] = await Promise.all([
          env.DB.prepare('SELECT id, blocks, created_at, like_count, mode FROM posts WHERE author=? ORDER BY created_at DESC LIMIT 5').bind(userId).all(),
          env.DB.prepare('SELECT id, content, created_at, post_id FROM comments WHERE author=? ORDER BY created_at DESC LIMIT 5').bind(userId).all(),
        ]);
        return json({
          posts: recentPosts.results.map(r => ({ ...r, blocks: JSON.parse(r.blocks) })),
          comments: recentComments.results,
        });
      }
      if (p.match(/^\/api\/profile\/[^/]+$/) && m === 'GET') {
        const userId = decodeURIComponent(p.split('/')[3]);
        const [profile, postCount, commentCount, mileage] = await Promise.all([
          env.DB.prepare('SELECT * FROM user_profiles WHERE user_id=?').bind(userId).first(),
          env.DB.prepare('SELECT COUNT(*) as c FROM posts WHERE author=?').bind(userId).first(),
          env.DB.prepare('SELECT COUNT(*) as c FROM comments WHERE author=?').bind(userId).first(),
          env.DB.prepare('SELECT points FROM user_mileage WHERE user_id=?').bind(userId).first(),
        ]);
        return json({
          user_id: userId,
          avatar_url: profile?.avatar_url || null,
          show_badge_admin: profile?.show_badge_admin ?? 1,
          show_badge_top: profile?.show_badge_top ?? 1,
          granted_badge_admin: profile?.granted_badge_admin ?? 0,
          granted_badge_top: profile?.granted_badge_top ?? 0,
          post_count: postCount?.c || 0,
          comment_count: commentCount?.c || 0,
          mileage: mileage?.points || 0,
        });
      }
      if (p.match(/^\/api\/profile\/[^/]+$/) && m === 'PUT') {
        const userId = decodeURIComponent(p.split('/')[3]);
        const body = await request.json();
        const existing = await env.DB.prepare('SELECT * FROM user_profiles WHERE user_id=?').bind(userId).first();
        const avatar_url = body.avatar_url !== undefined ? body.avatar_url : (existing?.avatar_url ?? null);
        const show_badge_admin = body.show_badge_admin !== undefined ? (body.show_badge_admin ? 1 : 0) : (existing?.show_badge_admin ?? 1);
        const show_badge_top = body.show_badge_top !== undefined ? (body.show_badge_top ? 1 : 0) : (existing?.show_badge_top ?? 1);
        const granted_badge_admin = body.granted_badge_admin !== undefined ? (body.granted_badge_admin ? 1 : 0) : (existing?.granted_badge_admin ?? 0);
        const granted_badge_top = body.granted_badge_top !== undefined ? (body.granted_badge_top ? 1 : 0) : (existing?.granted_badge_top ?? 0);
        await env.DB.prepare('INSERT INTO user_profiles(user_id,avatar_url,show_badge_admin,show_badge_top,granted_badge_admin,granted_badge_top) VALUES(?,?,?,?,?,?) ON CONFLICT(user_id) DO UPDATE SET avatar_url=?,show_badge_admin=?,show_badge_top=?,granted_badge_admin=?,granted_badge_top=?')
          .bind(userId, avatar_url, show_badge_admin, show_badge_top, granted_badge_admin, granted_badge_top, avatar_url, show_badge_admin, show_badge_top, granted_badge_admin, granted_badge_top).run();
        return json({ ok: true });
      }

      // ── Gemini API 사용량 ──
      if (p === '/api/gemini-usage' && m === 'GET') {
        const [total, daily, byType] = await Promise.all([
          env.DB.prepare('SELECT * FROM gemini_usage WHERE id=1').first(),
          env.DB.prepare('SELECT date, type, tokens_in, tokens_out, calls FROM gemini_usage_daily ORDER BY date DESC, type ASC LIMIT 90').all(),
          env.DB.prepare('SELECT type, SUM(tokens_in) as tokens_in, SUM(tokens_out) as tokens_out, SUM(calls) as calls FROM gemini_usage_daily GROUP BY type ORDER BY type ASC').all(),
        ]);
        return json({
          total: total || { tokens_in: 0, tokens_out: 0, calls: 0 },
          daily: daily.results || [],
          by_type: byType.results || [],
        });
      }

      // ── Claude API 사용량 ──
      if (p === '/api/claude-usage' && m === 'GET') {
        const row = await env.DB.prepare('SELECT * FROM claude_usage WHERE id=1').first();
        return json(row || { tokens_in: 0, tokens_out: 0, calls: 0 });
      }
      if (p === '/api/claude-usage' && m === 'POST') {
        const { tokens_in = 0, tokens_out = 0 } = await request.json();
        await env.DB.prepare(
          'INSERT INTO claude_usage(id,tokens_in,tokens_out,calls,updated_at) VALUES(1,?,?,1,?) ON CONFLICT(id) DO UPDATE SET tokens_in=tokens_in+?,tokens_out=tokens_out+?,calls=calls+1,updated_at=?'
        ).bind(tokens_in, tokens_out, Math.floor(Date.now()/1000), tokens_in, tokens_out, Math.floor(Date.now()/1000)).run();
        return json({ ok: true });
      }
      if (p === '/api/admin/law-cache' && m === 'POST') {
        const tok = request.headers.get('X-Cache-Token');
        if (!env.LAW_CACHE_TOKEN || tok !== env.LAW_CACHE_TOKEN) return json({ error: 'unauthorized' }, 401);
        const items = await request.json();
        if (!Array.isArray(items) || !items.length) return json({ error: 'items[] 필요' }, 400);
        const now = Math.floor(Date.now() / 1000);
        await env.DB.batch(items.map(({ key, data }) =>
          env.DB.prepare('INSERT INTO news_cache(category,data,cached_at) VALUES(?,?,?) ON CONFLICT(category) DO UPDATE SET data=?,cached_at=?')
            .bind(key, JSON.stringify(data), now, JSON.stringify(data), now)
        ));
        return json({ ok: true, count: items.length });
      }
      if (p === '/api/moel-usage' && m === 'GET') {
        const row = await env.DB.prepare('SELECT * FROM moel_usage WHERE id=1').first();
        return json(row || { tokens_in: 0, tokens_out: 0, calls: 0 });
      }
      if (p === '/api/test-ai' && m === 'GET') {
        const result = { binding: !!env.AI };
        if (!env.AI) return json({ ...result, error: 'AI 바인딩 없음 (wrangler.toml [ai] binding 확인 또는 재배포 필요)' });
        try {
          const wRes = await env.AI.run('@cf/meta/llama-3.3-70b-instruct-fp8-fast', {
            messages: [{ role: 'user', content: '안녕하세요, 한 문장으로 답하세요.' }],
            max_tokens: 80,
          });
          return json({ ...result, ok: true, response: wRes?.response || '', raw: wRes });
        } catch (e) {
          return json({ ...result, ok: false, error: e.message, stack: e.stack?.slice(0, 300) });
        }
      }

      if (p === '/api/test-moel' && m === 'GET') {
        if (!env.MOEL_LLM_TOKEN) return json({ error: 'MOEL_LLM_TOKEN 미설정' }, 400);
        try {
          const mRes = await fetchTimeout('https://ai.moel.go.kr/gpt/api/llm', {
            method: 'POST',
            headers: {
              'Authorization': `Bearer ${env.MOEL_LLM_TOKEN}`,
              'Content-Type': 'application/json',
              ...(env.MOEL_ORG_CODE ? { 'OrgCode': env.MOEL_ORG_CODE } : {}),
            },
            body: JSON.stringify({
              model: '빠른 모델 플러스',
              messages: [{ role: 'user', content: '안녕' }],
              stream: false, max_tokens: 50,
            }),
          }, 25000);
          const rawText = await mRes.text();
          return json({ status: mRes.status, ok: mRes.ok, body: rawText.slice(0, 500) });
        } catch (e) {
          return json({ error: e.message });
        }
      }

      // ── 프레즌스 ──
      if (p === '/api/presence' && m === 'POST') {
        const { user_id } = await request.json();
        if (!user_id) return json({ ok: false }, 400);
        await env.DB.prepare('INSERT INTO user_presence(user_id,last_seen) VALUES(?,?) ON CONFLICT(user_id) DO UPDATE SET last_seen=?')
          .bind(user_id, Math.floor(Date.now() / 1000), Math.floor(Date.now() / 1000)).run();
        return json({ ok: true });
      }

      // ── 채팅 ──
      if (p === '/api/chat' && m === 'GET') {
        const since = parseInt(url.searchParams.get('since') || '0');
        const rows = await env.DB.prepare('SELECT * FROM chat_messages WHERE created_at > ? ORDER BY created_at ASC LIMIT 100').bind(since).all();
        return json(rows.results);
      }
      if (p === '/api/chat' && m === 'POST') {
        const { author, content } = await request.json();
        if (!author || !content?.trim()) return json({ error: '내용을 입력하세요' }, 400);
        const id = 'msg_' + Date.now() + '_' + Math.random().toString(36).slice(2, 6);
        const now = Math.floor(Date.now() / 1000);
        await env.DB.prepare('INSERT INTO chat_messages(id,author,content,created_at) VALUES(?,?,?,?)').bind(id, author, content.trim(), now).run();
        return json({ id, author, content: content.trim(), created_at: now });
      }
      if (p.match(/^\/api\/chat\/[^/]+$/) && m === 'DELETE') {
        const id = p.split('/')[3];
        await env.DB.prepare('DELETE FROM chat_messages WHERE id=?').bind(id).run();
        return json({ ok: true });
      }

      // ── 건강봇 에이전트 ──
      const AGENT_ID = '000000099';
      if (p === '/api/agent/health/post' && m === 'POST') {
        // KDCA API는 행정망 전용이라 외부에서 접근 불가 → AI로 직접 생성
        const KDCA_CONTENTS = [
          // 감염·호흡기
          { sn: 5423, name: '감기' }, { sn: 5232, name: '인플루엔자(독감)' }, { sn: 5249, name: '폐렴' },
          { sn: 5466, name: '천식' }, { sn: 6253, name: '기침' }, { sn: 5239, name: '식중독' },
          { sn: 6551, name: '탈수' }, { sn: 5806, name: '알레르기' }, { sn: 6581, name: '두드러기' },
          { sn: 5284, name: '뇌수막염' }, { sn: 6561, name: '결핵' }, { sn: 6677, name: '코로나19' },
          { sn: 5307, name: 'A형간염' }, { sn: 6672, name: 'B형간염' }, { sn: 5309, name: 'C형간염' },
          { sn: 5703, name: '만성비염' }, { sn: 1101, name: '비부비동염' }, { sn: 5707, name: '편도염' },
          { sn: 5801, name: '기관지확장증' }, { sn: 6536, name: '만성폐쇄성폐질환' },
          // 심장·혈관
          { sn: 5300, name: '고혈압' }, { sn: 5243, name: '급성 심근경색증' }, { sn: 6566, name: '협심증' },
          { sn: 1102, name: '부정맥' }, { sn: 3828, name: '심부전' }, { sn: 5337, name: '흉통' },
          { sn: 5260, name: '죽상경화증' }, { sn: 5854, name: '심부 정맥 혈전증' }, { sn: 6540, name: '폐 색전증' },
          // 대사·내분비
          { sn: 5305, name: '당뇨병' }, { sn: 6694, name: '비만' }, { sn: 5304, name: '고혈당' },
          { sn: 5427, name: '대사증후군' }, { sn: 6715, name: '이상지질혈증' }, { sn: 5242, name: '고칼슘혈증' },
          { sn: 5810, name: '갑상선기능저하증' }, { sn: 1831, name: '갑상선기능항진증' },
          // 소화기
          { sn: 6263, name: '소화불량' }, { sn: 5827, name: '변비' }, { sn: 1667, name: '설사' },
          { sn: 1081, name: '복통' }, { sn: 6777, name: '위염' }, { sn: 5359, name: '위십이지장 궤양' },
          { sn: 2057, name: '위식도역류질환' }, { sn: 5248, name: '췌장염' }, { sn: 5818, name: '치핵' },
          { sn: 5297, name: '음주와 건강' }, { sn: 5310, name: '알코올 간질환' }, { sn: 6735, name: '담석증' },
          { sn: 5820, name: '담낭염' }, { sn: 5804, name: '구역질과 구토' }, { sn: 5858, name: '복부 팽만' },
          // 근골격·통증
          { sn: 3796, name: '요통' }, { sn: 5830, name: '두통' }, { sn: 6557, name: '편두통' },
          { sn: 5441, name: '염좌' }, { sn: 5463, name: '골절' }, { sn: 1988, name: '골관절염' },
          { sn: 5833, name: '골다공증' }, { sn: 5972, name: '일자목(거북목)증후군' },
          { sn: 1567, name: '오십견' }, { sn: 6292, name: '수근관 증후군' },
          { sn: 3348, name: '추간판탈출증(디스크)' }, { sn: 4047, name: '척추관 협착증' },
          { sn: 3512, name: '좌골신경통' }, { sn: 5975, name: '족저근막염' },
          { sn: 5687, name: '하지정맥류' }, { sn: 6732, name: '통풍' },
          { sn: 5826, name: '섬유근육통' }, { sn: 5536, name: '반월상 연골판 손상' },
          // 눈
          { sn: 6306, name: '안구건조증' }, { sn: 5226, name: '노안' }, { sn: 6689, name: '백내장' },
          { sn: 6690, name: '녹내장' }, { sn: 5846, name: '눈 충혈' }, { sn: 5269, name: '황반변성' },
          { sn: 5223, name: '비문증(날파리증)' },
          // 귀·코·입
          { sn: 5706, name: '이명' }, { sn: 5705, name: '목쉼' }, { sn: 3568, name: '중이염' },
          { sn: 5362, name: '코골이' }, { sn: 6308, name: '수면무호흡증' }, { sn: 3768, name: '코피' },
          { sn: 5841, name: '구취' }, { sn: 6288, name: '충치' }, { sn: 5716, name: '잇몸병(치주질환)' },
          { sn: 5704, name: '구강건조증' }, { sn: 5485, name: '구내염' }, { sn: 6550, name: '어지럼증' },
          // 피부
          { sn: 3947, name: '여드름' }, { sn: 5694, name: '습진' }, { sn: 6289, name: '지루 피부염' },
          { sn: 5695, name: '가려움증' }, { sn: 5690, name: '원형탈모' }, { sn: 2067, name: '남성형 탈모' },
          { sn: 5500, name: '일광화상' }, { sn: 6670, name: '동상' }, { sn: 5693, name: '발 백선(무좀)' },
          // 정신·신경
          { sn: 5294, name: '우울감' }, { sn: 6549, name: '만성피로증후군' },
          { sn: 5495, name: '뇌졸중' }, { sn: 5853, name: '실신' },
          { sn: 5860, name: '다한증' }, { sn: 6487, name: '손떨림(수전증)' },
          // 비뇨기
          { sn: 5968, name: '방광염' }, { sn: 6674, name: '요로감염' }, { sn: 5433, name: '신장결석' },
          { sn: 3193, name: '전립선비대증' }, { sn: 1104, name: '빈혈' },
          // 생활·예방
          { sn: 5293, name: '운동과 건강' }, { sn: 5299, name: '흡연과 건강' },
          { sn: 5482, name: '황사와 미세먼지' }, { sn: 6545, name: '올바른 손씻기' },
          { sn: 6548, name: '건강기능식품' }, { sn: 6671, name: '국가건강검진' },
          { sn: 6251, name: '신체활동' }, { sn: 6547, name: '건강한 체중조절' },
          { sn: 5298, name: '식이영양' }, { sn: 5353, name: '영양제 올바른 복용' },
          { sn: 3848, name: '폭염 건강수칙' }, { sn: 2048, name: '겨울철 한파 건강수칙' },
          { sn: 6529, name: '직업성 호흡기질환' }, { sn: 6309, name: '스포츠 손상 예방' },
          { sn: 6226, name: '심폐소생술(CPR)' }, { sn: 6264, name: '소음과 건강' },
        ];
        // 최근 봇 게시물에서 사용한 주제 추출 → 중복 방지
        const recentBot = await env.DB.prepare('SELECT blocks FROM posts WHERE author=? ORDER BY created_at DESC LIMIT 15').bind(AGENT_ID).all();
        const usedTopics = new Set();
        for (const rp of (recentBot.results || [])) {
          try {
            const bl = JSON.parse(rp.blocks);
            const txt = bl[0]?.content || '';
            const tm = txt.match(/오늘의 건강 정보[:\s]+([^\n]+)/);
            if (tm) usedTopics.add(tm[1].trim());
          } catch(e) {}
        }
        const available = KDCA_CONTENTS.filter(c => !usedTopics.has(c.name));
        const candidates = available.length ? available : KDCA_CONTENTS;
        const chosen = candidates[Math.floor(Math.random() * candidates.length)];

        // AI로 건강 정보 게시글 직접 생성 (Gemini→Claude 폴백)
        const aiResult = await callAI(
          '서울서부고용노동지청 내부 커뮤니티에 올릴 건강 정보 게시글 작성 봇입니다. 직장인에게 실용적인 건강 팁 위주로, 마크다운 없이 일반 텍스트로 작성하세요.',
          `주제: "${chosen.name}"\n\n요구사항:\n- 원인·증상·예방법/관리법 포함\n- 4~6문장, 300자 이내\n- 친근하고 읽기 쉬운 톤`,
          env, { type: 'health', maxTokens: 1000 }
        );
        if (!aiResult) return json({ error: 'AI 서비스를 사용할 수 없습니다. 잠시 후 다시 시도해주세요.' }, 502);
        const content = `🏥 오늘의 건강 정보: ${chosen.name}\n\n${aiResult.text}\n\n─\n📋 더 궁금한 점은 국가건강정보포털(☎1339)에 문의하세요.`;
        const blocks = [{ type: 'text', content }];
        const postId = 'post_' + Date.now();
        await env.DB.prepare('INSERT INTO posts(id,author,blocks,created_at) VALUES(?,?,?,?)')
          .bind(postId, AGENT_ID, JSON.stringify(blocks), Math.floor(Date.now() / 1000)).run();
        await env.DB.prepare('INSERT INTO post_keywords(post_id,keyword) VALUES(?,?) ON CONFLICT(post_id) DO UPDATE SET keyword=?')
          .bind(postId, '건강', '건강').run();
        return json({ ok: true, id: postId, title: chosen.name, via: aiResult.model });
      }
      if (p === '/api/agent/health/reply' && m === 'POST') {
        const replyBody = await request.json().catch(() => ({}));
        const isDebugMode = replyBody.debug === true; // 관리자 수동 실행 시 Claude 사용 금지
        // 에이전트 게시글에 달린 미답변 댓글 찾기
        const agentPostCheck = await env.DB.prepare('SELECT 1 FROM posts WHERE author=? LIMIT 1').bind(AGENT_ID).first();
        if (!agentPostCheck) return json({ error: '에이전트 게시글 없음' }, 404);
        const targetRow = await env.DB.prepare(
          'SELECT c.id, c.content FROM comments c JOIN posts p ON c.post_id=p.id WHERE p.author=? AND NOT EXISTS (SELECT 1 FROM comment_replies cr WHERE cr.comment_id=c.id AND cr.author=?) ORDER BY c.created_at ASC LIMIT 1'
        ).bind(AGENT_ID, AGENT_ID).first();
        const targetComment = targetRow || null;
        if (!targetComment) return json({ error: '답변할 댓글이 없습니다' });
        const FOOTER = '\n\n더 궁금한 점은 질병관리청(☎1339) 또는 보건복지부 콜센터(☎129)에서 전문 상담을 받으실 수 있습니다.';
        const replyAI = !isDebugMode
          ? await callAI(
              '당신은 서울서부고용노동지청 내부 커뮤니티의 건강 정보 봇입니다. 직원의 댓글에 공신력 있는 건강 정보를 바탕으로 친절하고 실용적으로 2~3문장 내외로 답변하세요. 인사말 없이 바로 내용으로 시작하세요.',
              (targetComment.content || '').slice(0, 1000),
              env, { type: 'reply', maxTokens: 500 }
            )
          : null;
        const replyContent = replyAI
          ? replyAI.text + FOOTER
          : '관련하여 더 궁금한 점은 질병관리청(☎1339) 또는 보건복지부 콜센터(☎129)에서 전문 상담을 받으실 수 있습니다.';
        const replyId = 'rep_' + Date.now() + '_' + Math.random().toString(36).slice(2, 5);
        await env.DB.prepare('INSERT INTO comment_replies(id,comment_id,author,content,created_at) VALUES(?,?,?,?,?)')
          .bind(replyId, targetComment.id, AGENT_ID, replyContent, Math.floor(Date.now() / 1000)).run();
        return json({ ok: true, comment_id: targetComment.id, api_used: !!replyAI, used_model: replyAI?.model || null });
      }

      // ── 설정 조회/변경 ──
      if (p === '/api/settings' && m === 'GET') {
        const rows = await env.DB.prepare('SELECT * FROM settings').all();
        const obj = {};
        rows.results.forEach(r => { obj[r.key] = r.value; });
        return json(obj);
      }
      if (p.match(/^\/api\/settings\/[^/]+$/) && m === 'POST') {
        const key = p.split('/')[3];
        const { value } = await request.json();
        await env.DB.prepare('INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=?').bind(key, value, value).run();
        return json({ ok: true });
      }

      // ── 맛집 ──
      if (p === '/api/restaurants' && m === 'GET') {
        const userId = url.searchParams.get('user_id') || '';
        const rows = await env.DB.prepare('SELECT r.*, COALESCE(up.up,0) as up_count, COALESCE(dn.dn,0) as down_count, COALESCE(rc.cnt,0) as review_count FROM restaurants r LEFT JOIN (SELECT restaurant_id, COUNT(*) as up FROM restaurant_votes WHERE vote=1 GROUP BY restaurant_id) up ON r.id=up.restaurant_id LEFT JOIN (SELECT restaurant_id, COUNT(*) as dn FROM restaurant_votes WHERE vote=-1 GROUP BY restaurant_id) dn ON r.id=dn.restaurant_id LEFT JOIN (SELECT restaurant_id, COUNT(*) as cnt FROM restaurant_reviews GROUP BY restaurant_id) rc ON r.id=rc.restaurant_id ORDER BY r.created_at DESC').all();
        let myVotes = {};
        if (userId) {
          const vr = await env.DB.prepare('SELECT restaurant_id, vote FROM restaurant_votes WHERE user_id=?').bind(userId).all();
          vr.results.forEach(v => { myVotes[v.restaurant_id] = v.vote; });
        }
        return json(rows.results.map(r => ({ ...r, my_vote: myVotes[r.id] || 0 })));
      }
      if (p === '/api/restaurants' && m === 'POST') {
        const b = await request.json();
        if (!b.name) return json({ error: '이름 필수' }, 400);
        const id = 'rst_' + Date.now();
        await env.DB.prepare('INSERT INTO restaurants(id,name,address,category,walk_min,note,added_by,created_at) VALUES(?,?,?,?,?,?,?,?)')
          .bind(id, b.name, b.address||'', b.category||'기타', b.walk_min||5, b.note||'', b.added_by||'', Math.floor(Date.now()/1000)).run();
        return json({ id });
      }
      if (p.match(/^\/api\/restaurants\/[^/]+$/) && m === 'DELETE') {
        const id = p.split('/')[3];
        await env.DB.prepare('DELETE FROM restaurants WHERE id=?').bind(id).run();
        await env.DB.prepare('DELETE FROM restaurant_reviews WHERE restaurant_id=?').bind(id).run();
        await env.DB.prepare('DELETE FROM restaurant_votes WHERE restaurant_id=?').bind(id).run();
        return json({ ok: true });
      }
      if (p.match(/^\/api\/restaurants\/[^/]+\/vote$/) && m === 'POST') {
        const restaurantId = p.split('/')[3];
        const { user_id, vote } = await request.json(); // vote: 1 or -1
        const existing = await env.DB.prepare('SELECT vote FROM restaurant_votes WHERE restaurant_id=? AND user_id=?').bind(restaurantId, user_id).first();
        if (existing && existing.vote === vote) {
          await env.DB.prepare('DELETE FROM restaurant_votes WHERE restaurant_id=? AND user_id=?').bind(restaurantId, user_id).run();
          return json({ vote: 0 });
        }
        await env.DB.prepare('INSERT INTO restaurant_votes(restaurant_id,user_id,vote) VALUES(?,?,?) ON CONFLICT(restaurant_id,user_id) DO UPDATE SET vote=?').bind(restaurantId, user_id, vote, vote).run();
        return json({ vote });
      }
      if (p.match(/^\/api\/restaurants\/[^/]+\/reviews$/) && m === 'GET') {
        const restaurantId = p.split('/')[3];
        const rows = await env.DB.prepare('SELECT * FROM restaurant_reviews WHERE restaurant_id=? ORDER BY created_at DESC').bind(restaurantId).all();
        return json(rows.results);
      }
      if (p.match(/^\/api\/restaurants\/[^/]+\/reviews$/) && m === 'POST') {
        const restaurantId = p.split('/')[3];
        const { author, content } = await request.json();
        if (!content?.trim()) return json({ error: '내용 필수' }, 400);
        const id = 'rr_' + Date.now() + '_' + Math.random().toString(36).slice(2,5);
        await env.DB.prepare('INSERT INTO restaurant_reviews(id,restaurant_id,author,content,created_at) VALUES(?,?,?,?,?)')
          .bind(id, restaurantId, author, content.trim(), Math.floor(Date.now()/1000)).run();
        return json({ id });
      }
      if (p.match(/^\/api\/restaurant-reviews\/[^/]+$/) && m === 'DELETE') {
        const id = p.split('/')[3];
        await env.DB.prepare('DELETE FROM restaurant_reviews WHERE id=?').bind(id).run();
        return json({ ok: true });
      }

      // ── 용량 확인 ──
      if (p === '/api/usage' && m === 'GET') {
        const row = await env.DB.prepare('SELECT bytes FROM usage WHERE id=1').first();
        return json({ bytes: row?.bytes || 0, max: MAX_BYTES });
      }

      // ── OX 퀴즈 ──
      // 관리자 인증 헬퍼
      const quizAdminAuth = async () => {
        const t = url.searchParams.get('token') || request.headers.get('Authorization')?.replace('Bearer ', '');
        const s = t ? await env.DB.prepare('SELECT user_id FROM sessions WHERE token=?').bind(t).first() : null;
        if (!s) return null;
        const r = await env.DB.prepare('SELECT role FROM user_roles WHERE user_id=?').bind(s.user_id).first();
        return (r?.role === 'admin' || r?.role === 'sub_admin') ? s : null;
      };

      if (p === '/api/quiz/current' && m === 'GET') {
        const reqGroup = url.searchParams.get('group') || 'all'; // 'center','branch','all','admin'
        const isAdminReq = reqGroup === 'admin';
        // group filter: 'all' 퀴즈는 모두에게, 그룹 퀴즈는 해당 그룹에만
        const grpFilter = isAdminReq ? '' : ` AND (group_target='all' OR group_target=?)`;
        const grpBind = isAdminReq ? [] : [reqGroup];

        const series = isAdminReq
          ? await env.DB.prepare("SELECT * FROM quiz_series WHERE status IN ('active','finished') ORDER BY created_at DESC LIMIT 1").first()
          : await env.DB.prepare(`SELECT * FROM quiz_series WHERE status IN ('active','finished')${grpFilter} ORDER BY created_at DESC LIMIT 1`).bind(...grpBind).first();
        let session = null, stats = { O: 0, X: 0, total: 0 }, answers = [], survivors = [];

        if (series) {
          session = await env.DB.prepare("SELECT * FROM quiz_sessions WHERE series_id=? AND status IN ('waiting','active','revealed') ORDER BY stage_num DESC LIMIT 1").bind(series.id).first();
          const revRow = await env.DB.prepare("SELECT COUNT(*) as cnt FROM quiz_sessions WHERE series_id=? AND status='revealed'").bind(series.id).first();
          const revCount = revRow?.cnt || 0;
          if (revCount > 0) {
            const survRows = await env.DB.prepare(`SELECT u.id as user_id, u.name FROM users u WHERE u.id IN (SELECT qa.user_id FROM quiz_answers qa JOIN quiz_sessions qs ON qa.quiz_id=qs.id WHERE qs.series_id=? AND qs.status='revealed' AND qa.answer=qs.answer GROUP BY qa.user_id HAVING COUNT(*)=?)`).bind(series.id, revCount).all();
            survivors = (survRows.results || []).map(r => ({ user_id: r.user_id, name: r.name }));
          }
        } else {
          session = isAdminReq
            ? await env.DB.prepare("SELECT * FROM quiz_sessions WHERE status IN ('waiting','active','revealed') AND (series_id IS NULL OR series_id='') ORDER BY created_at DESC LIMIT 1").first()
            : await env.DB.prepare(`SELECT * FROM quiz_sessions WHERE status IN ('waiting','active','revealed') AND (series_id IS NULL OR series_id='')${grpFilter} ORDER BY created_at DESC LIMIT 1`).bind(...grpBind).first();
        }

        let attendees_count = 0, my_attendance = false;
        if (session) {
          const sr = await env.DB.prepare("SELECT answer, COUNT(*) as cnt FROM quiz_answers WHERE quiz_id=? GROUP BY answer").bind(session.id).all();
          for (const r of (sr.results || [])) { stats[r.answer] = r.cnt; stats.total += r.cnt; }
          const ar = await env.DB.prepare('SELECT qa.answer, u.name FROM quiz_answers qa LEFT JOIN users u ON qa.user_id=u.id WHERE qa.quiz_id=?').bind(session.id).all();
          answers = (ar.results || []).map(r => ({ name: r.name || '?', answer: r.answer }));
          const atRow = await env.DB.prepare("SELECT COUNT(*) as cnt FROM quiz_attendees WHERE quiz_id=?").bind(session.id).first();
          attendees_count = atRow?.cnt || 0;
          const tokParam = url.searchParams.get('token');
          if (tokParam) {
            const usess = await env.DB.prepare('SELECT user_id FROM sessions WHERE token=?').bind(tokParam).first();
            if (usess) {
              const myAt = await env.DB.prepare("SELECT 1 FROM quiz_attendees WHERE quiz_id=? AND user_id=?").bind(session.id, usess.user_id).first();
              my_attendance = !!myAt;
            }
          }
          const safe = { ...session };
          if (session.status !== 'revealed') delete safe.answer;
          session = safe;
        }
        return json({ session, series: series ? { ...series, finished: series.status === 'finished' } : null, stats, answers, survivors, attendees_count, my_attendance });
      }

      // 스테이지전 생성
      if (p === '/api/quiz/series' && m === 'POST') {
        const adm = await quizAdminAuth(); if (!adm) return json({ error: 'unauthorized' }, 401);
        const { total_stages, group_target } = await request.json();
        if (!total_stages || total_stages < 2 || total_stages > 20) return json({ error: 'invalid' }, 400);
        const grp = ['all','center','branch'].includes(group_target) ? group_target : 'all';
        await env.DB.prepare("UPDATE quiz_sessions SET status='closed' WHERE status IN ('waiting','active','revealed')").run();
        await env.DB.prepare("UPDATE quiz_series SET status='finished' WHERE status='active'").run();
        await env.DB.prepare("UPDATE quiz_series SET status='closed' WHERE status='finished'").run();
        const id = `series_${Date.now()}_${Math.random().toString(36).slice(2, 7)}`;
        const now = Math.floor(Date.now() / 1000);
        await env.DB.prepare('INSERT INTO quiz_series(id,total_stages,current_stage,status,created_by,created_at,group_target) VALUES(?,?,0,?,?,?,?)').bind(id, total_stages, 'active', adm.user_id, now, grp).run();
        return json({ ok: true, series_id: id, total_stages });
      }

      // 스테이지전 종료
      if (p.match(/^\/api\/quiz\/series\/[^/]+\/finish$/) && m === 'POST') {
        const sid = p.split('/')[4];
        const adm = await quizAdminAuth(); if (!adm) return json({ error: 'unauthorized' }, 401);
        await env.DB.prepare("UPDATE quiz_sessions SET status='closed' WHERE series_id=? AND status IN ('waiting','active')").bind(sid).run();
        await env.DB.prepare("UPDATE quiz_series SET status='finished' WHERE id=?").bind(sid).run();
        return json({ ok: true });
      }

      // 스테이지전 완전 종료 (기록 유지, 화면에서 제거)
      if (p.match(/^\/api\/quiz\/series\/[^/]+\/close$/) && m === 'POST') {
        const sid = p.split('/')[4];
        const adm = await quizAdminAuth(); if (!adm) return json({ error: 'unauthorized' }, 401);
        await env.DB.prepare("UPDATE quiz_series SET status='closed' WHERE id=?").bind(sid).run();
        return json({ ok: true });
      }

      // 스테이지전 기록 삭제
      if (p.match(/^\/api\/quiz\/series\/[^/]+$/) && m === 'DELETE') {
        const sid = p.split('/')[4];
        const adm = await quizAdminAuth(); if (!adm) return json({ error: 'unauthorized' }, 401);
        await env.DB.prepare("DELETE FROM quiz_answers WHERE quiz_id IN (SELECT id FROM quiz_sessions WHERE series_id=?)").bind(sid).run();
        await env.DB.prepare("DELETE FROM quiz_sessions WHERE series_id=?").bind(sid).run();
        await env.DB.prepare("DELETE FROM quiz_series WHERE id=?").bind(sid).run();
        return json({ ok: true });
      }

      // 퀴즈 문제 생성
      if (p === '/api/quiz' && m === 'POST') {
        const adm = await quizAdminAuth(); if (!adm) return json({ error: 'unauthorized' }, 401);
        const { question, answer, series_id, stage_num, group_target } = await request.json();
        if (!question || !['O', 'X'].includes(answer)) return json({ error: 'invalid' }, 400);
        const grp = ['all','center','branch'].includes(group_target) ? group_target : 'all';
        const now = Math.floor(Date.now() / 1000);
        const id = `quiz_${Date.now()}_${Math.random().toString(36).slice(2, 7)}`;
        if (series_id) {
          const serRow = await env.DB.prepare('SELECT total_stages FROM quiz_series WHERE id=? AND status=\'active\'').bind(series_id).first();
          if (!serRow) return json({ error: '시리즈를 찾을 수 없습니다' }, 400);
          const sNum = stage_num || 1;
          if (sNum > serRow.total_stages) return json({ error: `최대 ${serRow.total_stages}라운드까지 가능합니다` }, 400);
          await env.DB.prepare("UPDATE quiz_sessions SET status='closed' WHERE series_id=? AND status='waiting'").bind(series_id).run();
          await env.DB.prepare("UPDATE quiz_series SET current_stage=? WHERE id=?").bind(sNum, series_id).run();
          await env.DB.prepare('INSERT INTO quiz_sessions(id,question,answer,status,created_by,created_at,series_id,stage_num,group_target) VALUES(?,?,?,?,?,?,?,?,?)').bind(id, question, answer, 'waiting', adm.user_id, now, series_id, sNum, grp).run();
        } else {
          await env.DB.prepare("UPDATE quiz_sessions SET status='closed' WHERE status IN ('waiting','active','revealed')").run();
          await env.DB.prepare("UPDATE quiz_series SET status='finished' WHERE status='active'").run();
          await env.DB.prepare("UPDATE quiz_series SET status='closed' WHERE status='finished'").run();
          await env.DB.prepare('INSERT INTO quiz_sessions(id,question,answer,status,created_by,created_at,group_target) VALUES(?,?,?,?,?,?,?)').bind(id, question, answer, 'waiting', adm.user_id, now, grp).run();
        }
        return json({ ok: true, id });
      }

      if (p.match(/^\/api\/quiz\/[^/]+\/start$/) && m === 'POST') {
        const adm = await quizAdminAuth(); if (!adm) return json({ error: 'unauthorized' }, 401);
        const qid = p.split('/')[3];
        const now = Math.floor(Date.now() / 1000);
        await env.DB.prepare("UPDATE quiz_sessions SET status='active', started_at=? WHERE id=? AND status='waiting'").bind(now, qid).run();
        return json({ ok: true, started_at: now });
      }

      if (p.match(/^\/api\/quiz\/[^/]+\/reveal$/) && m === 'POST') {
        const adm = await quizAdminAuth(); if (!adm) return json({ error: 'unauthorized' }, 401);
        const qid = p.split('/')[3];
        const now = Math.floor(Date.now() / 1000);
        await env.DB.prepare("UPDATE quiz_sessions SET status='revealed', revealed_at=? WHERE id=?").bind(now, qid).run();
        // 스테이지전인 경우: 생존자 수 체크
        const sess = await env.DB.prepare("SELECT * FROM quiz_sessions WHERE id=?").bind(qid).first();
        let autoResult = {};
        if (sess?.series_id) {
          const revCount = (await env.DB.prepare("SELECT COUNT(*) as cnt FROM quiz_sessions WHERE series_id=? AND status='revealed'").bind(sess.series_id).first())?.cnt || 0;
          const survivors = (await env.DB.prepare(`SELECT u.id as user_id, u.name FROM users u WHERE u.id IN (SELECT qa.user_id FROM quiz_answers qa JOIN quiz_sessions qs ON qa.quiz_id=qs.id WHERE qs.series_id=? AND qs.status='revealed' AND qa.answer=qs.answer GROUP BY qa.user_id HAVING COUNT(*)=?)`).bind(sess.series_id, revCount).all()).results || [];
          // 정답자 수 (이 스테이지)
          const stageCorrect = (await env.DB.prepare("SELECT COUNT(*) as cnt FROM quiz_answers WHERE quiz_id=? AND answer=?").bind(qid, sess.answer).first())?.cnt || 0;
          if (stageCorrect === 0) {
            // 전원 탈락 — 이 스테이지 번호를 재진행 가능하도록 표시
            autoResult = { all_eliminated: true, stage_num: sess.stage_num };
          } else if (survivors.length === 1) {
            // 최후의 1인 — 시리즈 자동 완료
            await env.DB.prepare("UPDATE quiz_series SET status='finished' WHERE id=?").bind(sess.series_id).run();
            autoResult = { auto_finished: true, winner: survivors[0] };
          }
        }
        return json({ ok: true, ...autoResult });
      }

      if (p.match(/^\/api\/quiz\/[^/]+\/close$/) && m === 'POST') {
        const adm = await quizAdminAuth(); if (!adm) return json({ error: 'unauthorized' }, 401);
        const qid = p.split('/')[3];
        await env.DB.prepare("UPDATE quiz_sessions SET status='closed' WHERE id=?").bind(qid).run();
        return json({ ok: true });
      }

      // ── 사다리 게임 ──
      if (p === '/api/ladder' && m === 'POST') {
        const adm = await quizAdminAuth(); if (!adm) return json({ error: 'unauthorized' }, 401);
        const { series_id, participants } = await request.json(); // participants: [{user_id,name}]
        if (!participants || participants.length < 2) return json({ error: 'need 2+ participants' }, 400);
        const n = participants.length;
        // 사다리 구조 생성
        const rows = Math.max(12, n * 4);
        const rungs = []; // {row, col} → col과 col+1 사이 가로선
        for (let r = 1; r < rows; r++) {
          const used = new Set();
          for (let c = 0; c < n - 1; c++) {
            if (!used.has(c) && !used.has(c+1) && Math.random() < 0.35) {
              rungs.push({ row: r, col: c });
              used.add(c); used.add(c+1);
            }
          }
        }
        // 각 시작 열이 어느 끝 열로 이동하는지 계산
        const trace = pos => {
          let c = pos;
          for (let r = 1; r < rows; r++) {
            const rung = rungs.find(rg => rg.row === r && (rg.col === c || rg.col === c - 1));
            if (rung) c = rung.col === c ? c + 1 : c - 1;
          }
          return c;
        };
        const endPositions = Array.from({ length: n }, (_, i) => trace(i));
        const winnerEnd = Math.floor(Math.random() * n);
        const winnerStart = endPositions.indexOf(winnerEnd);
        const now = Math.floor(Date.now() / 1000);
        const id = `ladder_${Date.now()}_${Math.random().toString(36).slice(2,6)}`;
        const structure = JSON.stringify({ rows, rungs, winner_start: winnerStart, end_positions: endPositions });
        await env.DB.prepare("UPDATE ladder_games SET status='closed' WHERE status IN ('picking','assigned')").run();
        await env.DB.prepare('INSERT INTO ladder_games(id,series_id,participants,structure,picks,status,pick_deadline,created_at,created_by) VALUES(?,?,?,?,?,?,?,?,?)')
          .bind(id, series_id||null, JSON.stringify(participants), structure, '{}', 'picking', now + 8, now, adm.user_id).run();
        return json({ ok: true, id });
      }

      if (p === '/api/ladder/current' && m === 'GET') {
        const game = await env.DB.prepare("SELECT * FROM ladder_games WHERE status IN ('picking','assigned','revealed') ORDER BY created_at DESC LIMIT 1").first();
        if (!game) return json({ game: null });
        const now = Math.floor(Date.now() / 1000);
        const participants = JSON.parse(game.participants);
        const picks = JSON.parse(game.picks || '{}');
        // 마감 지나면 미선택자 랜덤 배정
        if (game.status === 'picking' && now >= game.pick_deadline) {
          const taken = new Set(Object.values(picks));
          const available = Array.from({ length: participants.length }, (_, i) => i).filter(i => !taken.has(i));
          // shuffle available
          for (let i = available.length - 1; i > 0; i--) { const j = Math.floor(Math.random() * (i+1)); [available[i],available[j]]=[available[j],available[i]]; }
          let ai = 0;
          for (const p2 of participants) { if (picks[p2.user_id] === undefined) picks[p2.user_id] = available[ai++]; }
          await env.DB.prepare("UPDATE ladder_games SET picks=?, status='assigned' WHERE id=?").bind(JSON.stringify(picks), game.id).run();
          game.status = 'assigned'; game.picks = JSON.stringify(picks);
        }
        const safe = { id: game.id, series_id: game.series_id, status: game.status, participants, picks,
          pick_deadline: game.pick_deadline, winner_id: game.winner_id,
          structure: game.status === 'revealed' ? JSON.parse(game.structure) : { rows: JSON.parse(game.structure).rows, n: participants.length }
        };
        return json({ game: safe });
      }

      if (p.match(/^\/api\/ladder\/[^/]+\/pick$/) && m === 'POST') {
        const lid = p.split('/')[3];
        const t = url.searchParams.get('token') || request.headers.get('Authorization')?.replace('Bearer ','');
        const s = t ? await env.DB.prepare('SELECT user_id FROM sessions WHERE token=?').bind(t).first() : null;
        if (!s) return json({ error: 'unauthorized' }, 401);
        const { position } = await request.json(); // 0-indexed
        const game = await env.DB.prepare("SELECT * FROM ladder_games WHERE id=? AND status='picking'").bind(lid).first();
        if (!game) return json({ error: '선택 시간이 지났습니다' }, 400);
        const now = Math.floor(Date.now() / 1000);
        if (now >= game.pick_deadline) return json({ error: '시간이 지났습니다' }, 400);
        const participants = JSON.parse(game.participants);
        const n = participants.length;
        if (position < 0 || position >= n) return json({ error: 'invalid position' }, 400);
        const picks = JSON.parse(game.picks || '{}');
        if (Object.values(picks).includes(position)) return json({ error: '이미 선택된 자리입니다' }, 409);
        picks[s.user_id] = position;
        await env.DB.prepare("UPDATE ladder_games SET picks=? WHERE id=?").bind(JSON.stringify(picks), lid).run();
        return json({ ok: true });
      }

      if (p.match(/^\/api\/ladder\/[^/]+\/reveal$/) && m === 'POST') {
        const lid = p.split('/')[3];
        const adm = await quizAdminAuth(); if (!adm) return json({ error: 'unauthorized' }, 401);
        const game = await env.DB.prepare("SELECT * FROM ladder_games WHERE id=?").bind(lid).first();
        if (!game) return json({ error: 'not found' }, 404);
        const picks = JSON.parse(game.picks || '{}');
        const structure = JSON.parse(game.structure);
        // 미선택자 랜덤 배정
        const participants = JSON.parse(game.participants);
        const taken = new Set(Object.values(picks));
        const available = Array.from({length: participants.length},(_,i)=>i).filter(i=>!taken.has(i));
        for (let i = available.length-1; i>0; i--){const j=Math.floor(Math.random()*(i+1));[available[i],available[j]]=[available[j],available[i]];}
        let ai=0; for(const p2 of participants){if(picks[p2.user_id]===undefined)picks[p2.user_id]=available[ai++];}
        const winner = participants.find(p2 => picks[p2.user_id] === structure.winner_start);
        await env.DB.prepare("UPDATE ladder_games SET picks=?,status='revealed',winner_id=? WHERE id=?").bind(JSON.stringify(picks), winner?.user_id||null, lid).run();
        if (winner && game.series_id) {
          await env.DB.prepare("UPDATE quiz_series SET status='finished' WHERE id=?").bind(game.series_id).run();
        }
        return json({ ok: true, winner });
      }

      if (p.match(/^\/api\/quiz\/[^/]+\/attend$/) && m === 'POST') {
        const qid = p.split('/')[3];
        const t = url.searchParams.get('token') || request.headers.get('Authorization')?.replace('Bearer ', '');
        const s = t ? await env.DB.prepare('SELECT user_id FROM sessions WHERE token=?').bind(t).first() : null;
        if (!s) return json({ error: 'unauthorized' }, 401);
        const quiz = await env.DB.prepare('SELECT status FROM quiz_sessions WHERE id=?').bind(qid).first();
        if (!quiz) return json({ error: 'not found' }, 404);
        if (quiz.status !== 'waiting') return json({ error: 'not_waiting' }, 400);
        const now = Math.floor(Date.now() / 1000);
        await env.DB.prepare('INSERT INTO quiz_attendees(quiz_id,user_id,attended_at) VALUES(?,?,?) ON CONFLICT(quiz_id,user_id) DO NOTHING').bind(qid, s.user_id, now).run();
        const cnt = await env.DB.prepare('SELECT COUNT(*) as cnt FROM quiz_attendees WHERE quiz_id=?').bind(qid).first();
        return json({ ok: true, count: cnt?.cnt || 0 });
      }

      if (p.match(/^\/api\/quiz\/[^/]+\/answer$/) && m === 'POST') {
        const qid = p.split('/')[3];
        const t = url.searchParams.get('token') || request.headers.get('Authorization')?.replace('Bearer ', '');
        const s = t ? await env.DB.prepare('SELECT user_id FROM sessions WHERE token=?').bind(t).first() : null;
        if (!s) return json({ error: 'unauthorized' }, 401);
        const { answer } = await request.json();
        if (!['O', 'X'].includes(answer)) return json({ error: 'invalid' }, 400);
        const quiz = await env.DB.prepare('SELECT status FROM quiz_sessions WHERE id=?').bind(qid).first();
        if (!quiz || quiz.status !== 'active') return json({ error: 'not active' }, 400);
        const now = Math.floor(Date.now() / 1000);
        await env.DB.prepare('INSERT INTO quiz_answers(quiz_id,user_id,answer,answered_at) VALUES(?,?,?,?) ON CONFLICT(quiz_id,user_id) DO UPDATE SET answer=?,answered_at=?').bind(qid, s.user_id, answer, now, answer, now).run();
        return json({ ok: true });
      }

      return new Response('Not found', { status: 404, headers: CORS });
    } catch (e) {
      return json({ error: e.message }, 500);
    }
  },

  // Cloudflare Cron 트리거
  // "0 1 * * *"  → 10:00 KST  건강 글 게시
  // "0 7 * * *"  → 16:00 KST  건강 글 게시
  // "*/10 * * * *" → 10분마다 댓글 자동 답변
  async scheduled(event, env, ctx) {
    await initDB(env);
    const cron = event.cron;
    // 하루 2회 건강 정보 글 자동 게시 (10:00 / 16:00 KST)
    if (cron === '0 1 * * *' || cron === '0 7 * * *') {
      ctx.waitUntil(
        fetch('https://band-archive-api.cm99i.workers.dev/api/agent/health/post', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: '{}',
        }).catch(() => {})
      );
    }
    // 10분마다 댓글 자동 답변
    ctx.waitUntil(
      fetch('https://band-archive-api.cm99i.workers.dev/api/agent/health/reply', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: '{}',
      }).catch(() => {})
    );
  },
};
