const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET,POST,PUT,DELETE,OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type',
};
const MAX_BYTES = 9 * 1024 * 1024 * 1024;

function json(data, status = 200) {
  return new Response(JSON.stringify(data), {
    status, headers: { ...CORS, 'Content-Type': 'application/json' }
  });
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
    `CREATE TABLE IF NOT EXISTS kudos (id TEXT PRIMARY KEY, tag TEXT, source TEXT, content TEXT, added_by TEXT, created_at INTEGER)`,
    `CREATE TABLE IF NOT EXISTS monthly_contests (id TEXT PRIMARY KEY, title TEXT, description TEXT, nominate_start INTEGER, nominate_end INTEGER, vote_start INTEGER, vote_end INTEGER, created_by TEXT, winner TEXT, created_at INTEGER)`,
    `CREATE TABLE IF NOT EXISTS nominations (id TEXT PRIMARY KEY, contest_id TEXT, nominee TEXT, nominated_by TEXT, message TEXT, is_anonymous INTEGER DEFAULT 0, created_at INTEGER, UNIQUE(contest_id, nominated_by))`,
    `CREATE TABLE IF NOT EXISTS nominee_msgs (id TEXT PRIMARY KEY, contest_id TEXT, nominee TEXT, content TEXT, author_display TEXT, created_at INTEGER)`,
    `CREATE TABLE IF NOT EXISTS contest_votes (contest_id TEXT, voter TEXT, nominee TEXT, PRIMARY KEY(contest_id, voter))`,
    `CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT)`,
    `CREATE TABLE IF NOT EXISTS restaurants (id TEXT PRIMARY KEY, name TEXT, address TEXT, category TEXT, walk_min INTEGER DEFAULT 5, note TEXT, added_by TEXT, created_at INTEGER)`,
    `CREATE TABLE IF NOT EXISTS restaurant_reviews (id TEXT PRIMARY KEY, restaurant_id TEXT, author TEXT, content TEXT, created_at INTEGER)`,
    `CREATE TABLE IF NOT EXISTS restaurant_votes (restaurant_id TEXT, user_id TEXT, vote INTEGER, PRIMARY KEY(restaurant_id, user_id))`,
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
  _dbReady = true;
}

export default {
  async fetch(request, env) {
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
        return json({ url: '/' + key });
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

      // ── 글 목록 ──
      if (p === '/api/posts' && m === 'GET') {
        const rows = await env.DB.prepare(
          'SELECT p.*, COALESCE(cc.cnt,0)+COALESCE(rc.rcnt,0) as comment_count, pk.keyword FROM posts p LEFT JOIN (SELECT post_id, COUNT(*) as cnt FROM comments GROUP BY post_id) cc ON p.id=cc.post_id LEFT JOIN (SELECT c.post_id, COUNT(*) as rcnt FROM comment_replies cr JOIN comments c ON cr.comment_id=c.id GROUP BY c.post_id) rc ON p.id=rc.post_id LEFT JOIN post_keywords pk ON p.id=pk.post_id ORDER BY p.created_at DESC'
        ).all();
        return json(rows.results.map(r => ({ ...r, blocks: JSON.parse(r.blocks) })));
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
        await addMileageDB(env, b.author, 2);
        return json({ id });
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
          const post = await env.DB.prepare('SELECT author FROM posts WHERE id=?').bind(postId).first();
          if (post && post.author !== user_id) await addMileageDB(env, post.author, 0.5);
          return json({ liked: true });
        }
      }

      // ── 댓글 목록 (대댓글 포함) ──
      if (p.match(/^\/api\/posts\/[^/]+\/comments$/) && m === 'GET') {
        const postId = p.split('/')[3];
        const userId = url.searchParams.get('user_id');
        const rows = await env.DB.prepare(
          'SELECT c.*, COALESCE(cl.cnt,0) as like_count FROM comments c LEFT JOIN (SELECT comment_id, COUNT(*) as cnt FROM comment_likes GROUP BY comment_id) cl ON c.id=cl.comment_id WHERE c.post_id=? ORDER BY c.created_at ASC'
        ).bind(postId).all();
        let likedSet = new Set();
        if (userId) {
          const liked = await env.DB.prepare('SELECT comment_id FROM comment_likes WHERE user_id=?').bind(userId).all();
          likedSet = new Set(liked.results.map(r => r.comment_id));
        }
        // 대댓글 일괄 조회
        const allReplies = await env.DB.prepare(
          'SELECT * FROM comment_replies WHERE comment_id IN (SELECT id FROM comments WHERE post_id=?) ORDER BY created_at ASC'
        ).bind(postId).all();
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
        await addMileageDB(env, b.author, 1);
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
          if (comment_author && comment_author !== user_id) await addMileageDB(env, comment_author, 0.5);
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
        await addMileageDB(env, author, 1);
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
        let created = 0, skipped = 0;
        for (const u of list) {
          const uid = (u.id || '').trim(), uname = (u.name || '').trim(), upw = (u.password || '1234').trim(), udept = (u.dept || '').trim();
          if (!uid || !/^\d{9}$/.test(uid) || !uname) { skipped++; continue; }
          const exists = await env.DB.prepare('SELECT 1 FROM users WHERE id=?').bind(uid).first();
          if (exists) { skipped++; continue; }
          await env.DB.prepare('INSERT INTO users(id,name,dept,password,status,created_at) VALUES(?,?,?,?,?,?)')
            .bind(uid, uname, udept, upw, 'active', Math.floor(Date.now() / 1000)).run();
          await env.DB.prepare('INSERT INTO user_roles(user_id,role) VALUES(?,?) ON CONFLICT(user_id) DO NOTHING').bind(uid, 'user').run();
          created++;
        }
        return json({ ok: true, created, skipped });
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
        const user = await env.DB.prepare('SELECT * FROM users WHERE id=?').bind(id).first();
        if (!user || user.password !== password) return json({ error: '사번 또는 비밀번호가 올바르지 않습니다.' }, 401);
        if (user.status === 'pending') return json({ error: '관리자 승인 대기 중입니다.' }, 403);
        const token = crypto.randomUUID();
        await env.DB.prepare('INSERT INTO sessions(token,user_id,created_at) VALUES(?,?,?)').bind(token, id, Math.floor(Date.now()/1000)).run();
        // 오래된 세션 정리 (최근 5개만 유지)
        await env.DB.prepare('DELETE FROM sessions WHERE user_id=? AND token NOT IN (SELECT token FROM sessions WHERE user_id=? ORDER BY created_at DESC LIMIT 5)').bind(id, id).run();
        return json({ ok: true, id: user.id, name: user.name || user.id, token, must_change_password: user.password === '1234' });
      }
      if (p === '/api/verify-session' && m === 'POST') {
        const { token } = await request.json();
        if (!token) return json({ error: 'no token' }, 401);
        const sess = await env.DB.prepare('SELECT * FROM sessions WHERE token=?').bind(token).first();
        if (!sess) return json({ error: 'invalid' }, 401);
        const user = await env.DB.prepare('SELECT * FROM users WHERE id=?').bind(sess.user_id).first();
        if (!user || user.status !== 'active') return json({ error: 'user not found' }, 401);
        return json({ ok: true, id: user.id, name: user.name || user.id, must_change_password: user.password === '1234' });
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
        const comments = await env.DB.prepare(
          'SELECT "comment" as type, c.author, substr(c.content,1,60) as excerpt, c.post_id as ref_id, c.created_at FROM comments c JOIN posts p ON c.post_id=p.id WHERE p.author=? AND c.author!=? ORDER BY c.created_at DESC LIMIT 10'
        ).bind(userId, userId).all();
        const replies = await env.DB.prepare(
          'SELECT "reply" as type, r.author, substr(r.content,1,60) as excerpt, r.comment_id as ref_id, r.created_at FROM comment_replies r JOIN comments c ON r.comment_id=c.id WHERE c.author=? AND r.author!=? ORDER BY r.created_at DESC LIMIT 10'
        ).bind(userId, userId).all();
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
        const recentPosts = await env.DB.prepare(
          'SELECT id, blocks, created_at, like_count, mode FROM posts WHERE author=? ORDER BY created_at DESC LIMIT 5'
        ).bind(userId).all();
        const recentComments = await env.DB.prepare(
          'SELECT id, content, created_at, post_id FROM comments WHERE author=? ORDER BY created_at DESC LIMIT 5'
        ).bind(userId).all();
        return json({
          posts: recentPosts.results.map(r => ({ ...r, blocks: JSON.parse(r.blocks) })),
          comments: recentComments.results,
        });
      }
      if (p.match(/^\/api\/profile\/[^/]+$/) && m === 'GET') {
        const userId = decodeURIComponent(p.split('/')[3]);
        const profile = await env.DB.prepare('SELECT * FROM user_profiles WHERE user_id=?').bind(userId).first();
        const postCount = await env.DB.prepare('SELECT COUNT(*) as c FROM posts WHERE author=?').bind(userId).first();
        const commentCount = await env.DB.prepare('SELECT COUNT(*) as c FROM comments WHERE author=?').bind(userId).first();
        const mileage = await env.DB.prepare('SELECT points FROM user_mileage WHERE user_id=?').bind(userId).first();
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
        const row = await env.DB.prepare('SELECT * FROM gemini_usage WHERE id=1').first();
        return json(row || { tokens_in: 0, tokens_out: 0, calls: 0 });
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
        // 기존 health 뉴스 캐시 우선 사용, 없으면 직접 fetch
        let items = [];
        const cached = await env.DB.prepare('SELECT data FROM news_cache WHERE category=?').bind('health').first();
        if (cached) { try { items = JSON.parse(cached.data); } catch(e) {} }
        if (!items.length) {
          const feedUrl = 'https://news.google.com/rss/search?q=' + encodeURIComponent('질병관리청 OR 보건복지부 건강') + '&hl=ko&gl=KR&ceid=KR:ko';
          const resp = await fetch(feedUrl, { headers: { 'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36' } });
          if (!resp.ok) return json({ error: '뉴스를 불러오지 못했습니다. 건강 탭에서 먼저 뉴스를 로드해주세요.' }, 502);
          items = parseRSS(await resp.text());
        }
        if (!items.length) return json({ error: '기사를 찾지 못했습니다' }, 404);
        // 최근 5개 중 랜덤 선택
        const item = items[Math.floor(Math.random() * Math.min(5, items.length))];
        const srcLabel = item.source || '건강 정보';
        const content = `📋 [${srcLabel}]\n\n${item.title}${item.link ? `\n\n🔗 원문: ${item.link}` : ''}\n\n─\n출처: 공신력 있는 건강 정보를 제공합니다.\n더 궁금한 점은 질병관리청 1339 또는 보건복지부 129로 문의하세요.`;
        const blocks = [{ type: 'text', content }];
        const postId = 'post_' + Date.now();
        await env.DB.prepare('INSERT INTO posts(id,author,blocks,created_at) VALUES(?,?,?,?)')
          .bind(postId, AGENT_ID, JSON.stringify(blocks), Math.floor(Date.now() / 1000)).run();
        await env.DB.prepare('INSERT INTO post_keywords(post_id,keyword) VALUES(?,?) ON CONFLICT(post_id) DO UPDATE SET keyword=?')
          .bind(postId, '건강', '건강').run();
        return json({ ok: true, id: postId, title: item.title });
      }
      if (p === '/api/agent/health/reply' && m === 'POST') {
        // 에이전트 게시글에 달린 미답변 댓글 찾기
        const agentPosts = await env.DB.prepare('SELECT id FROM posts WHERE author=? ORDER BY created_at DESC LIMIT 20').bind(AGENT_ID).all();
        if (!agentPosts.results.length) return json({ error: '에이전트 게시글 없음' }, 404);
        let targetComment = null;
        for (const post of agentPosts.results) {
          const cmts = await env.DB.prepare('SELECT id, content FROM comments WHERE post_id=? ORDER BY created_at ASC').bind(post.id).all();
          for (const cmt of cmts.results) {
            const already = await env.DB.prepare('SELECT 1 FROM comment_replies WHERE comment_id=? AND author=?').bind(cmt.id, AGENT_ID).first();
            if (!already) { targetComment = cmt; break; }
          }
          if (targetComment) break;
        }
        if (!targetComment) return json({ error: '답변할 댓글이 없습니다' });
        // Gemini(무료) 우선 → 한도초과 시 Claude Haiku 폴백
        const SYSTEM_PROMPT = '당신은 서울서부고용노동지청 내부 커뮤니티의 건강 정보 봇입니다. 직원의 댓글에 공신력 있는 건강 정보를 바탕으로 친절하고 실용적으로 2~3문장 내외로 답변하세요. 인사말 없이 바로 내용으로 시작하세요.';
        const FOOTER = '\n\n더 궁금한 점은 질병관리청(☎1339) 또는 보건복지부 콜센터(☎129)에서 전문 상담을 받으실 수 있습니다.';
        let replyContent = null;
        let apiUsed = false;
        let apiError = null;
        let usedModel = null;

        // 1단계: Gemini 무료 API 시도
        if (env.GEMINI_API_KEY) {
          try {
            const gResp = await fetch(
              `https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key=${env.GEMINI_API_KEY}`,
              {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                  contents: [{ role: 'user', parts: [{ text: SYSTEM_PROMPT + '\n\n직원 질문: ' + targetComment.content }] }],
                  generationConfig: { maxOutputTokens: 1024, thinkingConfig: { thinkingBudget: 0 } },
                }),
              }
            );
            if (gResp.status === 429) {
              // 분당 한도 초과 → Claude로 폴백
              apiError = 'Gemini 한도초과(429), Claude로 폴백';
            } else {
              const gData = await gResp.json();
              const gText = gData.candidates?.[0]?.content?.parts?.[0]?.text;
              if (gText) {
                replyContent = gText + FOOTER;
                apiUsed = true;
                usedModel = 'gemini';
                const gIn = gData.usageMetadata?.promptTokenCount || 0;
                const gOut = gData.usageMetadata?.candidatesTokenCount || 0;
                await env.DB.prepare(
                  'INSERT INTO gemini_usage(id,tokens_in,tokens_out,calls,updated_at) VALUES(1,?,?,1,?) ON CONFLICT(id) DO UPDATE SET tokens_in=tokens_in+?,tokens_out=tokens_out+?,calls=calls+1,updated_at=?'
                ).bind(gIn, gOut, Math.floor(Date.now()/1000), gIn, gOut, Math.floor(Date.now()/1000)).run();
              } else {
                apiError = 'Gemini 응답 없음: ' + JSON.stringify(gData).slice(0, 100);
              }
            }
          } catch(e) { apiError = 'Gemini 오류: ' + e.message; }
        }

        // 2단계: Gemini 실패/한도초과/미설정 시 Claude Haiku 폴백 (관리자가 켠 경우에만)
        const claudeSetting = await env.DB.prepare("SELECT value FROM settings WHERE key='claude_enabled'").first();
        const claudeEnabled = !claudeSetting || claudeSetting.value !== 'false';
        if (!replyContent && claudeEnabled) {
          if (!env.ANTHROPIC_API_KEY) {
            apiError = (apiError ? apiError + ' | ' : '') + 'ANTHROPIC_API_KEY 미설정';
          } else {
            try {
              const aiResp = await fetch('https://api.anthropic.com/v1/messages', {
                method: 'POST',
                headers: {
                  'Content-Type': 'application/json',
                  'x-api-key': env.ANTHROPIC_API_KEY,
                  'anthropic-version': '2023-06-01',
                },
                body: JSON.stringify({
                  model: 'claude-haiku-4-5-20251001',
                  max_tokens: 300,
                  system: SYSTEM_PROMPT,
                  messages: [{ role: 'user', content: targetComment.content }],
                }),
              });
              const aiData = await aiResp.json();
              const aiText = aiData.content?.[0]?.text;
              if (aiText) {
                replyContent = aiText + FOOTER;
                apiUsed = true;
                usedModel = 'claude';
              } else {
                apiError = (apiError ? apiError + ' | ' : '') + (aiData.error?.message || JSON.stringify(aiData).slice(0, 100));
              }
              if (aiData.usage) {
                await env.DB.prepare(
                  'INSERT INTO claude_usage(id,tokens_in,tokens_out,calls,updated_at) VALUES(1,?,?,1,?) ON CONFLICT(id) DO UPDATE SET tokens_in=tokens_in+?,tokens_out=tokens_out+?,calls=calls+1,updated_at=?'
                ).bind(aiData.usage.input_tokens||0, aiData.usage.output_tokens||0, Math.floor(Date.now()/1000), aiData.usage.input_tokens||0, aiData.usage.output_tokens||0, Math.floor(Date.now()/1000)).run();
              }
            } catch(e) { apiError = (apiError ? apiError + ' | ' : '') + 'Claude 오류: ' + e.message; }
          }
        }

        if (!replyContent) {
          replyContent = '관련하여 더 궁금한 점은 질병관리청(☎1339) 또는 보건복지부 콜센터(☎129)에서 전문 상담을 받으실 수 있습니다.';
        }
        const replyId = 'rep_' + Date.now() + '_' + Math.random().toString(36).slice(2, 5);
        await env.DB.prepare('INSERT INTO comment_replies(id,comment_id,author,content,created_at) VALUES(?,?,?,?,?)')
          .bind(replyId, targetComment.id, AGENT_ID, replyContent, Math.floor(Date.now() / 1000)).run();
        return json({ ok: true, comment_id: targetComment.id, api_used: apiUsed, used_model: usedModel, api_error: apiError });
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

      return new Response('Not found', { status: 404, headers: CORS });
    } catch (e) {
      return json({ error: e.message }, 500);
    }
  },

  // Cloudflare Cron 트리거 (10분마다 자동 실행)
  async scheduled(_event, env, ctx) {
    await initDB(env);
    ctx.waitUntil(
      fetch('https://band-archive-api.cm99i.workers.dev/api/agent/health/reply', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: '{}',
      }).catch(() => {})
    );
  },
};
