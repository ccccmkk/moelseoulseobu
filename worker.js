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

async function addMileageDB(env, userId, delta) {
  if (!userId || !delta) return;
  await env.DB.prepare(
    'INSERT INTO user_mileage(user_id,points) VALUES(?,?) ON CONFLICT(user_id) DO UPDATE SET points=ROUND(points+?,10)'
  ).bind(userId, delta, delta).run();
}

async function initDB(env) {
  const tables = [
    `CREATE TABLE IF NOT EXISTS posts (id TEXT PRIMARY KEY, author TEXT, blocks TEXT, created_at INTEGER, like_count INTEGER DEFAULT 0)`,
    `CREATE TABLE IF NOT EXISTS usage (id INTEGER PRIMARY KEY, bytes INTEGER DEFAULT 0)`,
    `CREATE TABLE IF NOT EXISTS likes (post_id TEXT, user_id TEXT, PRIMARY KEY(post_id, user_id))`,
    `CREATE TABLE IF NOT EXISTS comments (id TEXT PRIMARY KEY, post_id TEXT, author TEXT, content TEXT, created_at INTEGER)`,
    `CREATE TABLE IF NOT EXISTS events (id TEXT PRIMARY KEY, author TEXT, type TEXT, title TEXT, content TEXT, created_at INTEGER)`,
    `CREATE TABLE IF NOT EXISTS vote_sessions (id TEXT PRIMARY KEY, title TEXT, description TEXT, end_at INTEGER, created_by TEXT, active INTEGER DEFAULT 1, created_at INTEGER)`,
    `CREATE TABLE IF NOT EXISTS user_votes (session_id TEXT, voter TEXT, nominee TEXT, PRIMARY KEY(session_id, voter))`,
    `CREATE TABLE IF NOT EXISTS user_roles (user_id TEXT PRIMARY KEY, role TEXT DEFAULT 'user')`,
    `CREATE TABLE IF NOT EXISTS comment_likes (comment_id TEXT, user_id TEXT, PRIMARY KEY(comment_id, user_id))`,
    `CREATE TABLE IF NOT EXISTS comment_replies (id TEXT PRIMARY KEY, comment_id TEXT, author TEXT, content TEXT, created_at INTEGER)`,
    `CREATE TABLE IF NOT EXISTS user_mileage (user_id TEXT PRIMARY KEY, points REAL DEFAULT 0)`,
    `CREATE TABLE IF NOT EXISTS user_profiles (user_id TEXT PRIMARY KEY, avatar_url TEXT)`,
    `CREATE TABLE IF NOT EXISTS post_keywords (post_id TEXT PRIMARY KEY, keyword TEXT)`,
  ];
  for (const t of tables) await env.DB.exec(t);
  // 관리자는 항상 admin
  await env.DB.prepare(
    'INSERT INTO user_roles(user_id,role) VALUES(?,?) ON CONFLICT(user_id) DO UPDATE SET role=?'
  ).bind('관리자', 'admin', 'admin').run();
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

      // ── 글 목록 ──
      if (p === '/api/posts' && m === 'GET') {
        const rows = await env.DB.prepare('SELECT * FROM posts ORDER BY created_at DESC').all();
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
        await env.DB.prepare('DELETE FROM post_keywords WHERE post_id=?').bind(id).run();
        return json({ ok: true });
      }

      // ── 내가 좋아요한 포스트 목록 ──
      if (p === '/api/likes' && m === 'GET') {
        const userId = url.searchParams.get('user_id');
        if (!userId) return json([]);
        const rows = await env.DB.prepare('SELECT post_id FROM likes WHERE user_id=?').bind(userId).all();
        return json(rows.results.map(r => r.post_id));
      }

      // ── 포스트 좋아요 토글 ──
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
          if (post && post.author !== user_id) {
            await addMileageDB(env, post.author, 0.5);
          }
          return json({ liked: true });
        }
      }

      // ── 댓글 목록 ──
      if (p.match(/^\/api\/posts\/[^/]+\/comments$/) && m === 'GET') {
        const postId = p.split('/')[3];
        const rows = await env.DB.prepare('SELECT * FROM comments WHERE post_id=? ORDER BY created_at ASC').bind(postId).all();
        return json(rows.results);
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

      // ── 댓글 좋아요 (포스트별 일괄) ──
      if (p.match(/^\/api\/posts\/[^/]+\/comment-likes$/) && m === 'GET') {
        const postId = p.split('/')[3];
        const userId = url.searchParams.get('user_id') || '';
        const counts = await env.DB.prepare(
          `SELECT cl.comment_id, COUNT(*) as count FROM comment_likes cl JOIN comments c ON cl.comment_id=c.id WHERE c.post_id=? GROUP BY cl.comment_id`
        ).bind(postId).all();
        const liked = userId ? await env.DB.prepare(
          `SELECT cl.comment_id FROM comment_likes cl JOIN comments c ON cl.comment_id=c.id WHERE c.post_id=? AND cl.user_id=?`
        ).bind(postId, userId).all() : { results: [] };
        const likedSet = new Set(liked.results.map(r => r.comment_id));
        return json(counts.results.map(r => ({ comment_id: r.comment_id, count: r.count, liked: likedSet.has(r.comment_id) })));
      }

      // ── 댓글 좋아요 토글 ──
      if (p.match(/^\/api\/comments\/[^/]+\/like$/) && m === 'POST') {
        const commentId = p.split('/')[3];
        const { user_id, comment_author } = await request.json();
        const existing = await env.DB.prepare('SELECT 1 FROM comment_likes WHERE comment_id=? AND user_id=?').bind(commentId, user_id).first();
        if (existing) {
          await env.DB.prepare('DELETE FROM comment_likes WHERE comment_id=? AND user_id=?').bind(commentId, user_id).run();
          const cnt = await env.DB.prepare('SELECT COUNT(*) as c FROM comment_likes WHERE comment_id=?').bind(commentId).first();
          return json({ liked: false, count: cnt?.c || 0 });
        } else {
          await env.DB.prepare('INSERT INTO comment_likes(comment_id,user_id) VALUES(?,?)').bind(commentId, user_id).run();
          if (comment_author && comment_author !== user_id) {
            await addMileageDB(env, comment_author, 0.5);
          }
          const cnt = await env.DB.prepare('SELECT COUNT(*) as c FROM comment_likes WHERE comment_id=?').bind(commentId).first();
          return json({ liked: true, count: cnt?.c || 0 });
        }
      }

      // ── 답글 목록 (포스트별 일괄) ──
      if (p.match(/^\/api\/posts\/[^/]+\/replies$/) && m === 'GET') {
        const postId = p.split('/')[3];
        const rows = await env.DB.prepare(
          `SELECT cr.* FROM comment_replies cr JOIN comments c ON cr.comment_id=c.id WHERE c.post_id=? ORDER BY cr.created_at ASC`
        ).bind(postId).all();
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

      // ── 답글 삭제 ──
      if (p.match(/^\/api\/replies\/[^/]+$/) && m === 'DELETE') {
        const id = p.split('/')[3];
        await env.DB.prepare('DELETE FROM comment_replies WHERE id=?').bind(id).run();
        return json({ ok: true });
      }

      // ── 프로필 전체 조회 ──
      if (p === '/api/profiles' && m === 'GET') {
        const rows = await env.DB.prepare('SELECT * FROM user_profiles').all();
        return json(rows.results);
      }

      // ── 프로필 업데이트 ──
      if (p.match(/^\/api\/profiles\/[^/]+$/) && m === 'PUT') {
        const userId = decodeURIComponent(p.split('/')[3]);
        const { avatar_url } = await request.json();
        await env.DB.prepare('INSERT INTO user_profiles(user_id,avatar_url) VALUES(?,?) ON CONFLICT(user_id) DO UPDATE SET avatar_url=?')
          .bind(userId, avatar_url, avatar_url).run();
        return json({ ok: true });
      }

      // ── 마일리지 전체 조회 (관리자용) ──
      if (p === '/api/mileage' && m === 'GET') {
        const rows = await env.DB.prepare('SELECT * FROM user_mileage ORDER BY points DESC').all();
        return json(rows.results);
      }

      // ── 마일리지 개인 조회 ──
      if (p.match(/^\/api\/mileage\/[^/]+$/) && m === 'GET') {
        const userId = decodeURIComponent(p.split('/')[3]);
        const row = await env.DB.prepare('SELECT points FROM user_mileage WHERE user_id=?').bind(userId).first();
        return json({ points: row?.points || 0 });
      }

      // ── 포스트 키워드 전체 조회 ──
      if (p === '/api/post-keywords' && m === 'GET') {
        const rows = await env.DB.prepare('SELECT * FROM post_keywords').all();
        return json(rows.results);
      }

      // ── 경조사 목록 ──
      if (p === '/api/events' && m === 'GET') {
        const rows = await env.DB.prepare('SELECT * FROM events ORDER BY created_at DESC').all();
        return json(rows.results);
      }

      // ── 경조사 작성 ──
      if (p === '/api/events' && m === 'POST') {
        const b = await request.json();
        const id = 'evt_' + Date.now();
        await env.DB.prepare('INSERT INTO events(id,author,type,title,content,created_at) VALUES(?,?,?,?,?,?)')
          .bind(id, b.author, b.type || '기타', b.title, b.content || '', Math.floor(Date.now() / 1000)).run();
        return json({ id });
      }

      // ── 경조사 삭제 ──
      if (p.match(/^\/api\/events\/[^/]+$/) && m === 'DELETE') {
        const id = p.split('/')[3];
        await env.DB.prepare('DELETE FROM events WHERE id=?').bind(id).run();
        return json({ ok: true });
      }

      // ── 투표 세션 목록 ──
      if (p === '/api/votes' && m === 'GET') {
        const rows = await env.DB.prepare('SELECT * FROM vote_sessions ORDER BY created_at DESC').all();
        return json(rows.results);
      }

      // ── 투표 세션 생성 ──
      if (p === '/api/votes' && m === 'POST') {
        const b = await request.json();
        const id = 'vote_' + Date.now();
        await env.DB.prepare('INSERT INTO vote_sessions(id,title,description,end_at,created_by,active,created_at) VALUES(?,?,?,?,?,1,?)')
          .bind(id, b.title, b.description || '', b.end_at || 0, b.created_by, Math.floor(Date.now() / 1000)).run();
        return json({ id });
      }

      // ── 투표 세션 상세 ──
      if (p.match(/^\/api\/votes\/[^/]+$/) && m === 'GET') {
        const id = p.split('/')[3];
        const session = await env.DB.prepare('SELECT * FROM vote_sessions WHERE id=?').bind(id).first();
        if (!session) return json({ error: 'not found' }, 404);
        const votes = await env.DB.prepare('SELECT nominee, COUNT(*) as count FROM user_votes WHERE session_id=? GROUP BY nominee ORDER BY count DESC').bind(id).all();
        const voter = url.searchParams.get('voter');
        const myVote = voter ? (await env.DB.prepare('SELECT nominee FROM user_votes WHERE session_id=? AND voter=?').bind(id, voter).first())?.nominee : null;
        return json({ ...session, results: votes.results, my_vote: myVote });
      }

      // ── 투표하기 ──
      if (p.match(/^\/api\/votes\/[^/]+\/vote$/) && m === 'POST') {
        const sessionId = p.split('/')[3];
        const { voter, nominee } = await request.json();
        const session = await env.DB.prepare('SELECT * FROM vote_sessions WHERE id=?').bind(sessionId).first();
        if (!session || !session.active) return json({ error: '투표가 종료되었습니다' }, 400);
        const now = Math.floor(Date.now() / 1000);
        if (session.end_at && now > session.end_at) return json({ error: '투표 기간이 지났습니다' }, 400);
        const existing = await env.DB.prepare('SELECT 1 FROM user_votes WHERE session_id=? AND voter=?').bind(sessionId, voter).first();
        if (existing) return json({ error: '이미 투표하셨습니다' }, 400);
        await env.DB.prepare('INSERT INTO user_votes(session_id,voter,nominee) VALUES(?,?,?)').bind(sessionId, voter, nominee).run();
        return json({ ok: true });
      }

      // ── 투표 종료 ──
      if (p.match(/^\/api\/votes\/[^/]+\/close$/) && m === 'POST') {
        const id = p.split('/')[3];
        await env.DB.prepare('UPDATE vote_sessions SET active=0 WHERE id=?').bind(id).run();
        return json({ ok: true });
      }

      // ── 투표 삭제 ──
      if (p.match(/^\/api\/votes\/[^/]+$/) && m === 'DELETE') {
        const id = p.split('/')[3];
        await env.DB.prepare('DELETE FROM vote_sessions WHERE id=?').bind(id).run();
        await env.DB.prepare('DELETE FROM user_votes WHERE session_id=?').bind(id).run();
        return json({ ok: true });
      }

      // ── 사용자 역할 목록 ──
      if (p === '/api/roles' && m === 'GET') {
        const rows = await env.DB.prepare('SELECT * FROM user_roles').all();
        return json(rows.results);
      }

      // ── 사용자 역할 설정 ──
      if (p.match(/^\/api\/roles\/[^/]+$/) && m === 'PUT') {
        const userId = decodeURIComponent(p.split('/')[3]);
        const { role } = await request.json();
        await env.DB.prepare('INSERT INTO user_roles(user_id,role) VALUES(?,?) ON CONFLICT(user_id) DO UPDATE SET role=?')
          .bind(userId, role, role).run();
        return json({ ok: true });
      }

      // ── 용량 확인 ──
      if (p === '/api/usage' && m === 'GET') {
        const row = await env.DB.prepare('SELECT bytes FROM usage WHERE id=1').first();
        return json({ bytes: row?.bytes || 0, max: MAX_BYTES });
      }

      // ── 관리자: 사용자 통계 ──
      if (p === '/api/admin/stats' && m === 'GET') {
        const postCounts = await env.DB.prepare('SELECT author, COUNT(*) as cnt FROM posts GROUP BY author').all();
        const commentCounts = await env.DB.prepare('SELECT author, COUNT(*) as cnt FROM comments GROUP BY author').all();
        const mileageRows = await env.DB.prepare('SELECT * FROM user_mileage').all();
        return json({
          posts: postCounts.results,
          comments: commentCounts.results,
          mileage: mileageRows.results,
        });
      }

      return new Response('Not found', { status: 404, headers: CORS });
    } catch (e) {
      return json({ error: e.message }, 500);
    }
  }
};
