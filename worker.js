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

const NEWS_FEEDS = [
  { url: 'https://www.moel.go.kr/rss/news.do',              source: '고용노동부' },
  { url: 'https://www.labortoday.co.kr/rss/allArticle.xml', source: '매일노동뉴스' },
  { url: 'https://www.hani.co.kr/rss/economy/index.xml',    source: '한겨레 경제' },
  { url: 'https://www.yonhapnews.co.kr/rss/society.xml',    source: '연합뉴스' },
  { url: 'https://www.khan.co.kr/rss/rssdata/economy.xml',  source: '경향신문' },
];
const NEWS_KEYWORDS = [
  '고용','노동','취업','실업','임금','채용','근로','일자리','산업재해',
  '고용보험','워크넷','최저임금','고용부','노동부','실업급여','고용센터',
  '근로자','단체협약','직업훈련','고용촉진','노사','직훈','구직','구인',
  '고용노동','노동시장','임금체불','퇴직금','육아휴직','출산휴가',
];

function parseRSS(xml, source) {
  const items = [];
  const rx = /<item>([\s\S]*?)<\/item>/g;
  let m;
  const get = (block, tag) => {
    const r = new RegExp(`<${tag}[^>]*>(?:<!\\[CDATA\\[([\\s\\S]*?)\\]\\]>|([^<]*))<\\/${tag}>`);
    const mm = r.exec(block);
    return mm ? (mm[1] || mm[2] || '').trim() : '';
  };
  while ((m = rx.exec(xml)) !== null) {
    const b = m[1];
    const title = get(b, 'title');
    if (!title) continue;
    let link = get(b, 'link');
    if (!link) link = get(b, 'guid');
    const pubDate = get(b, 'pubDate') || get(b, 'dc:date') || get(b, 'pubdate');
    items.push({ title, link, pubDate, source });
  }
  return items;
}

async function fetchNews() {
  const results = await Promise.allSettled(
    NEWS_FEEDS.map(async ({ url, source }) => {
      const r = await fetch(url, { headers: { 'User-Agent': 'Mozilla/5.0' }, cf: { cacheTtl: 600 } });
      const text = await r.text();
      return parseRSS(text, source);
    })
  );
  let all = [];
  for (const r of results) {
    if (r.status === 'fulfilled') all = all.concat(r.value);
  }
  // filter by keywords
  const filtered = all.filter(item => {
    const text = (item.title + ' ' + item.link).toLowerCase();
    return NEWS_KEYWORDS.some(k => text.includes(k));
  });
  // dedupe by title
  const seen = new Set();
  const deduped = filtered.filter(item => {
    const key = item.title.slice(0, 30);
    if (seen.has(key)) return false;
    seen.add(key); return true;
  });
  // sort by pubDate desc
  deduped.sort((a, b) => {
    const da = a.pubDate ? new Date(a.pubDate).getTime() : 0;
    const db = b.pubDate ? new Date(b.pubDate).getTime() : 0;
    return db - da;
  });
  return deduped.slice(0, 40);
}

async function initDB(env) {
  const tables = [
    `CREATE TABLE IF NOT EXISTS posts (id TEXT PRIMARY KEY, author TEXT, blocks TEXT, created_at INTEGER, like_count INTEGER DEFAULT 0)`,
    `CREATE TABLE IF NOT EXISTS usage (id INTEGER PRIMARY KEY, bytes INTEGER DEFAULT 0)`,
    `CREATE TABLE IF NOT EXISTS likes (post_id TEXT, user_id TEXT, PRIMARY KEY(post_id, user_id))`,
    `CREATE TABLE IF NOT EXISTS comments (id TEXT PRIMARY KEY, post_id TEXT, author TEXT, content TEXT, created_at INTEGER)`,
    `CREATE TABLE IF NOT EXISTS events (id TEXT PRIMARY KEY, author TEXT, type TEXT, title TEXT, content TEXT, created_at INTEGER)`,
    `CREATE TABLE IF NOT EXISTS user_roles (user_id TEXT PRIMARY KEY, role TEXT DEFAULT 'user')`,
    `CREATE TABLE IF NOT EXISTS kudos (id TEXT PRIMARY KEY, tag TEXT, source TEXT, content TEXT, added_by TEXT, created_at INTEGER)`,
    `CREATE TABLE IF NOT EXISTS monthly_contests (id TEXT PRIMARY KEY, title TEXT, description TEXT, nominate_start INTEGER, nominate_end INTEGER, vote_start INTEGER, vote_end INTEGER, created_by TEXT, winner TEXT, created_at INTEGER)`,
    `CREATE TABLE IF NOT EXISTS nominations (id TEXT PRIMARY KEY, contest_id TEXT, nominee TEXT, nominated_by TEXT, message TEXT, is_anonymous INTEGER DEFAULT 0, created_at INTEGER, UNIQUE(contest_id, nominated_by))`,
    `CREATE TABLE IF NOT EXISTS nominee_msgs (id TEXT PRIMARY KEY, contest_id TEXT, nominee TEXT, content TEXT, author_display TEXT, created_at INTEGER)`,
    `CREATE TABLE IF NOT EXISTS contest_votes (contest_id TEXT, voter TEXT, nominee TEXT, PRIMARY KEY(contest_id, voter))`,
  ];
  for (const t of tables) await env.DB.exec(t);
}

export default {
  async fetch(request, env) {
    if (request.method === 'OPTIONS') return new Response(null, { headers: CORS });
    await initDB(env);

    const url = new URL(request.url);
    const p = url.pathname;
    const m = request.method;

    try {
      // ── 이미지 ──
      if (p.startsWith('/img/') && m === 'GET') {
        const obj = await env.R2.get(p.slice(1));
        if (!obj) return new Response('Not found', { status: 404 });
        return new Response(obj.body, {
          headers: { 'Content-Type': obj.httpMetadata?.contentType || 'image/jpeg', 'Cache-Control': 'public,max-age=31536000', ...CORS }
        });
      }
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

      // ── 뉴스 ──
      if (p === '/api/news' && m === 'GET') {
        const news = await fetchNews();
        return json(news);
      }

      // ── 피드 ──
      if (p === '/api/posts' && m === 'GET') {
        const rows = await env.DB.prepare('SELECT * FROM posts ORDER BY created_at DESC').all();
        return json(rows.results.map(r => ({ ...r, blocks: JSON.parse(r.blocks) })));
      }
      if (p === '/api/posts' && m === 'POST') {
        const b = await request.json();
        const id = 'post_' + Date.now();
        await env.DB.prepare('INSERT INTO posts(id,author,blocks,created_at) VALUES(?,?,?,?)')
          .bind(id, b.author, JSON.stringify(b.blocks), Math.floor(Date.now()/1000)).run();
        return json({ id });
      }
      if (p.match(/^\/api\/posts\/[^/]+$/) && m === 'PUT') {
        const id = p.split('/')[3];
        const b = await request.json();
        await env.DB.prepare('UPDATE posts SET blocks=? WHERE id=?').bind(JSON.stringify(b.blocks), id).run();
        return json({ ok: true });
      }
      if (p.match(/^\/api\/posts\/[^/]+$/) && m === 'DELETE') {
        const id = p.split('/')[3];
        await env.DB.prepare('DELETE FROM posts WHERE id=?').bind(id).run();
        await env.DB.prepare('DELETE FROM comments WHERE post_id=?').bind(id).run();
        await env.DB.prepare('DELETE FROM likes WHERE post_id=?').bind(id).run();
        return json({ ok: true });
      }

      // ── 좋아요 ──
      if (p === '/api/likes' && m === 'GET') {
        const userId = url.searchParams.get('user_id');
        if (!userId) return json([]);
        const rows = await env.DB.prepare('SELECT post_id FROM likes WHERE user_id=?').bind(userId).all();
        return json(rows.results.map(r => r.post_id));
      }
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
          return json({ liked: true });
        }
      }

      // ── 댓글 (피드용) ──
      if (p.match(/^\/api\/posts\/[^/]+\/comments$/) && m === 'GET') {
        const postId = p.split('/')[3];
        const rows = await env.DB.prepare('SELECT * FROM comments WHERE post_id=? ORDER BY created_at ASC').bind(postId).all();
        return json(rows.results);
      }
      if (p.match(/^\/api\/posts\/[^/]+\/comments$/) && m === 'POST') {
        const postId = p.split('/')[3];
        const b = await request.json();
        const id = 'cmt_' + Date.now() + '_' + Math.random().toString(36).slice(2,6);
        await env.DB.prepare('INSERT INTO comments(id,post_id,author,content,created_at) VALUES(?,?,?,?,?)')
          .bind(id, postId, b.author, b.content, Math.floor(Date.now()/1000)).run();
        return json({ id, post_id: postId, author: b.author, content: b.content, created_at: Math.floor(Date.now()/1000) });
      }
      if (p.match(/^\/api\/comments\/[^/]+$/) && m === 'DELETE') {
        const id = p.split('/')[3];
        await env.DB.prepare('DELETE FROM comments WHERE id=?').bind(id).run();
        return json({ ok: true });
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
          .bind(id, b.author, b.type||'기타', b.title, b.content||'', Math.floor(Date.now()/1000)).run();
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
          .bind(id, b.tag, b.source||'', b.content, b.added_by, Math.floor(Date.now()/1000)).run();
        return json({ id });
      }
      if (p.match(/^\/api\/kudos\/[^/]+$/) && m === 'DELETE') {
        const id = p.split('/')[3];
        await env.DB.prepare('DELETE FROM kudos WHERE id=?').bind(id).run();
        return json({ ok: true });
      }

      // ── 이달의 서부인 (contests) ──
      if (p === '/api/contests' && m === 'GET') {
        const rows = await env.DB.prepare('SELECT * FROM monthly_contests ORDER BY created_at DESC').all();
        return json(rows.results);
      }
      if (p === '/api/contests' && m === 'POST') {
        const b = await request.json();
        const id = 'mc_' + Date.now();
        await env.DB.prepare('INSERT INTO monthly_contests(id,title,description,nominate_start,nominate_end,vote_start,vote_end,created_by,created_at) VALUES(?,?,?,?,?,?,?,?,?)')
          .bind(id, b.title, b.description||'', b.nominate_start||0, b.nominate_end||0, b.vote_start||0, b.vote_end||0, b.created_by, Math.floor(Date.now()/1000)).run();
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

      // 우승자 설정 (admin)
      if (p.match(/^\/api\/contests\/[^/]+\/winner$/) && m === 'POST') {
        const id = p.split('/')[3];
        const { winner } = await request.json();
        await env.DB.prepare('UPDATE monthly_contests SET winner=? WHERE id=?').bind(winner, id).run();
        return json({ ok: true });
      }

      // 추천 (nomination) 목록
      if (p.match(/^\/api\/contests\/[^/]+\/nominations$/) && m === 'GET') {
        const contestId = p.split('/')[3];
        const rows = await env.DB.prepare('SELECT * FROM nominations WHERE contest_id=? ORDER BY created_at ASC').bind(contestId).all();
        // msgs per nominee
        const msgs = await env.DB.prepare('SELECT * FROM nominee_msgs WHERE contest_id=? ORDER BY created_at ASC').bind(contestId).all();
        return json({ nominations: rows.results, msgs: msgs.results });
      }

      // 추천하기
      if (p.match(/^\/api\/contests\/[^/]+\/nominations$/) && m === 'POST') {
        const contestId = p.split('/')[3];
        const b = await request.json();
        const contest = await env.DB.prepare('SELECT * FROM monthly_contests WHERE id=?').bind(contestId).first();
        if (!contest) return json({ error: 'not found' }, 404);
        const now = Math.floor(Date.now()/1000);
        if (now < contest.nominate_start || now > contest.nominate_end)
          return json({ error: '추천 기간이 아닙니다' }, 400);
        // check if winner in same year
        const year = new Date().getFullYear();
        const yearStart = Math.floor(new Date(year, 0, 1).getTime()/1000);
        const yearEnd = Math.floor(new Date(year+1, 0, 1).getTime()/1000);
        const prevWin = await env.DB.prepare('SELECT 1 FROM monthly_contests WHERE winner=? AND created_at>=? AND created_at<?').bind(b.nominee, yearStart, yearEnd).first();
        if (prevWin) return json({ error: '올해 이미 수상한 분입니다' }, 400);
        const existing = await env.DB.prepare('SELECT id FROM nominations WHERE contest_id=? AND nominated_by=?').bind(contestId, b.nominated_by).first();
        if (existing) return json({ error: '이미 추천하셨습니다' }, 400);
        const id = 'nom_' + Date.now() + '_' + Math.random().toString(36).slice(2,5);
        await env.DB.prepare('INSERT INTO nominations(id,contest_id,nominee,nominated_by,message,is_anonymous,created_at) VALUES(?,?,?,?,?,?,?)')
          .bind(id, contestId, b.nominee, b.nominated_by, b.message||'', b.is_anonymous?1:0, now).run();
        return json({ id });
      }

      // 후보자 메시지 추가 (투표기간 댓글)
      if (p.match(/^\/api\/contests\/[^/]+\/msgs$/) && m === 'POST') {
        const contestId = p.split('/')[3];
        const b = await request.json();
        const contest = await env.DB.prepare('SELECT * FROM monthly_contests WHERE id=?').bind(contestId).first();
        if (!contest) return json({ error: 'not found' }, 404);
        const now = Math.floor(Date.now()/1000);
        // check not already commented
        const existing = await env.DB.prepare('SELECT 1 FROM nominee_msgs WHERE contest_id=? AND json_extract(author_display,\'$.voter\')=?').bind(contestId, b.voter).first();
        if (existing) return json({ error: '이미 댓글을 작성하셨습니다' }, 400);
        const id = 'msg_' + Date.now() + '_' + Math.random().toString(36).slice(2,5);
        const authorDisplay = b.is_anonymous ? '익명' : b.voter;
        await env.DB.prepare('INSERT INTO nominee_msgs(id,contest_id,nominee,content,author_display,created_at) VALUES(?,?,?,?,?,?)')
          .bind(id, contestId, b.nominee, b.content, JSON.stringify({display: authorDisplay, voter: b.voter}), now).run();
        return json({ id });
      }

      // 투표하기
      if (p.match(/^\/api\/contests\/[^/]+\/vote$/) && m === 'POST') {
        const contestId = p.split('/')[3];
        const { voter, nominee } = await request.json();
        const contest = await env.DB.prepare('SELECT * FROM monthly_contests WHERE id=?').bind(contestId).first();
        if (!contest) return json({ error: 'not found' }, 404);
        const now = Math.floor(Date.now()/1000);
        if (now < contest.vote_start || now > contest.vote_end)
          return json({ error: '투표 기간이 아닙니다' }, 400);
        const existing = await env.DB.prepare('SELECT 1 FROM contest_votes WHERE contest_id=? AND voter=?').bind(contestId, voter).first();
        if (existing) return json({ error: '이미 투표하셨습니다' }, 400);
        await env.DB.prepare('INSERT INTO contest_votes(contest_id,voter,nominee) VALUES(?,?,?)').bind(contestId, voter, nominee).run();
        return json({ ok: true });
      }

      // 투표 현황
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

      if (p === '/api/usage' && m === 'GET') {
        const row = await env.DB.prepare('SELECT bytes FROM usage WHERE id=1').first();
        return json({ bytes: row?.bytes || 0, max: MAX_BYTES });
      }

      return new Response('Not found', { status: 404, headers: CORS });
    } catch(e) {
      return json({ error: e.message }, 500);
    }
  }
};
