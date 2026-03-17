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
    `CREATE TABLE IF NOT EXISTS users (id TEXT PRIMARY KEY, password TEXT NOT NULL, status TEXT DEFAULT 'pending', created_at INTEGER)`,
    `CREATE TABLE IF NOT EXISTS news_cache (category TEXT PRIMARY KEY, data TEXT, cached_at INTEGER DEFAULT 0)`,
    `CREATE TABLE IF NOT EXISTS user_presence (user_id TEXT PRIMARY KEY, last_seen INTEGER DEFAULT 0)`,
    `CREATE TABLE IF NOT EXISTS chat_messages (id TEXT PRIMARY KEY, author TEXT, content TEXT, created_at INTEGER)`,
    `CREATE TABLE IF NOT EXISTS claude_usage (id INTEGER PRIMARY KEY, tokens_in INTEGER DEFAULT 0, tokens_out INTEGER DEFAULT 0, calls INTEGER DEFAULT 0, updated_at INTEGER DEFAULT 0)`,
    `CREATE TABLE IF NOT EXISTS kudos (id TEXT PRIMARY KEY, tag TEXT, source TEXT, content TEXT, added_by TEXT, created_at INTEGER)`,
    `CREATE TABLE IF NOT EXISTS monthly_contests (id TEXT PRIMARY KEY, title TEXT, description TEXT, nominate_start INTEGER, nominate_end INTEGER, vote_start INTEGER, vote_end INTEGER, created_by TEXT, winner TEXT, created_at INTEGER)`,
    `CREATE TABLE IF NOT EXISTS nominations (id TEXT PRIMARY KEY, contest_id TEXT, nominee TEXT, nominated_by TEXT, message TEXT, is_anonymous INTEGER DEFAULT 0, created_at INTEGER, UNIQUE(contest_id, nominated_by))`,
    `CREATE TABLE IF NOT EXISTS nominee_msgs (id TEXT PRIMARY KEY, contest_id TEXT, nominee TEXT, content TEXT, author_display TEXT, created_at INTEGER)`,
    `CREATE TABLE IF NOT EXISTS contest_votes (contest_id TEXT, voter TEXT, nominee TEXT, PRIMARY KEY(contest_id, voter))`,
  ];
  await env.DB.batch(tables.map(t => env.DB.prepare(t)));
  await env.DB.batch([
    env.DB.prepare('INSERT INTO user_roles(user_id,role) VALUES(?,?) ON CONFLICT(user_id) DO UPDATE SET role=?').bind('관리자','admin','admin'),
    env.DB.prepare('INSERT INTO users(id,password,status,created_at) VALUES(?,?,?,?) ON CONFLICT(id) DO NOTHING').bind('관리자','9999','active',0),
  ]);
  _dbReady = true;
}

export default {
  async fetch(request, env) {
    if (request.method === 'OPTIONS') return new Response(null, { headers: CORS });
    await initDB(env);

    const url = new URL(request.url);
    const p = url.pathname;
    const m = request.method;

    try {
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
          labor: '고용 노동 최저임금 근로 취업 노동부 고용보험 산업재해 워크넷 실업급여 직업훈련 일자리 채용',
          local: '마포구 OR 용산구 OR 서대문구 OR 은평구 고용 취업',
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
          'SELECT p.*, COALESCE(cc.cnt,0) as comment_count, pk.keyword FROM posts p LEFT JOIN (SELECT post_id, COUNT(*) as cnt FROM comments GROUP BY post_id) cc ON p.id=cc.post_id LEFT JOIN post_keywords pk ON p.id=pk.post_id ORDER BY p.created_at DESC'
        ).all();
        return json(rows.results.map(r => ({ ...r, blocks: JSON.parse(r.blocks) })));
      }

      // ── 글 작성 ──
      if (p === '/api/posts' && m === 'POST') {
        const b = await request.json();
        const id = 'post_' + Date.now();
        const now = Math.floor(Date.now() / 1000);
        await env.DB.prepare('INSERT INTO posts(id,author,blocks,created_at) VALUES(?,?,?,?)')
          .bind(id, b.author, JSON.stringify(b.blocks), now).run();
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
        await env.DB.prepare('UPDATE posts SET blocks=? WHERE id=?').bind(JSON.stringify(b.blocks), id).run();
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

      // ── 댓글 목록 ──
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
        return json(rows.results.map(r => ({ ...r, user_liked: likedSet.has(r.id) })));
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
        await env.DB.prepare('INSERT INTO events(id,author,type,title,content,created_at) VALUES(?,?,?,?,?,?)')
          .bind(id, b.author, b.type || '기타', b.title, b.content || '', Math.floor(Date.now() / 1000)).run();
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
        await env.DB.prepare('INSERT INTO kudos(id,tag,source,content,added_by,created_at) VALUES(?,?,?,?,?,?)')
          .bind(id, b.tag, b.source || '', b.content, b.added_by, Math.floor(Date.now() / 1000)).run();
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
        const rows = await env.DB.prepare('SELECT id, status, created_at FROM users ORDER BY created_at ASC').all();
        return json(rows.results);
      }
      if (p.match(/^\/api\/users\/[^/]+\/approve$/) && m === 'PUT') {
        const userId = decodeURIComponent(p.split('/')[3]);
        await env.DB.prepare('UPDATE users SET status=? WHERE id=?').bind('active', userId).run();
        return json({ ok: true });
      }
      if (p.match(/^\/api\/users\/[^/]+$/) && m === 'DELETE') {
        const userId = decodeURIComponent(p.split('/')[3]);
        if (userId === '관리자') return json({ error: '관리자는 삭제할 수 없습니다.' }, 400);
        await env.DB.prepare('DELETE FROM users WHERE id=?').bind(userId).run();
        return json({ ok: true });
      }

      // ── 로그인 / 회원가입 ──
      if (p === '/api/login' && m === 'POST') {
        const { id, password } = await request.json();
        const user = await env.DB.prepare('SELECT * FROM users WHERE id=?').bind(id).first();
        if (!user || user.password !== password) return json({ error: '아이디 또는 비밀번호가 올바르지 않습니다.' }, 401);
        if (user.status === 'pending') return json({ error: '관리자 승인 대기 중입니다.' }, 403);
        return json({ ok: true, id: user.id });
      }
      if (p === '/api/register' && m === 'POST') {
        const { id, password } = await request.json();
        if (!id || !password || id.length < 2) return json({ error: '아이디는 2자 이상이어야 합니다.' }, 400);
        const exists = await env.DB.prepare('SELECT 1 FROM users WHERE id=?').bind(id).first();
        if (exists) return json({ error: '이미 사용 중인 아이디입니다.' }, 409);
        await env.DB.prepare('INSERT INTO users(id,password,status,created_at) VALUES(?,?,?,?)')
          .bind(id, password, 'pending', Math.floor(Date.now() / 1000)).run();
        return json({ ok: true });
      }

      // ── 마일리지 ──
      if (p === '/api/mileage' && m === 'GET') {
        const rows = await env.DB.prepare('SELECT * FROM user_mileage ORDER BY points DESC').all();
        return json(rows.results);
      }

      // ── 프로필 ──
      if (p === '/api/profiles' && m === 'GET') {
        const rows = await env.DB.prepare('SELECT * FROM user_profiles').all();
        return json(rows.results);
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
          post_count: postCount?.c || 0,
          comment_count: commentCount?.c || 0,
          mileage: mileage?.points || 0,
        });
      }
      if (p.match(/^\/api\/profile\/[^/]+$/) && m === 'PUT') {
        const userId = decodeURIComponent(p.split('/')[3]);
        const { avatar_url } = await request.json();
        await env.DB.prepare('INSERT INTO user_profiles(user_id,avatar_url) VALUES(?,?) ON CONFLICT(user_id) DO UPDATE SET avatar_url=?')
          .bind(userId, avatar_url, avatar_url).run();
        return json({ ok: true });
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

      // ── 용량 확인 ──
      if (p === '/api/usage' && m === 'GET') {
        const row = await env.DB.prepare('SELECT bytes FROM usage WHERE id=1').first();
        return json({ bytes: row?.bytes || 0, max: MAX_BYTES });
      }

      return new Response('Not found', { status: 404, headers: CORS });
    } catch (e) {
      return json({ error: e.message }, 500);
    }
  }
};
