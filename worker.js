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
      // 이미지 서빙
      if (p.startsWith('/img/') && m === 'GET') {
        const obj = await env.R2.get(p.slice(1));
        if (!obj) return new Response('Not found', { status: 404 });
        return new Response(obj.body, {
          headers: { 'Content-Type': obj.httpMetadata?.contentType || 'image/jpeg', 'Cache-Control': 'public,max-age=31536000', ...CORS }
        });
      }

      // 이미지 업로드
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

      // 글 목록
      if (p === '/api/posts' && m === 'GET') {
        const rows = await env.DB.prepare('SELECT * FROM posts ORDER BY created_at DESC').all();
        return json(rows.results.map(r => ({ ...r, blocks: JSON.parse(r.blocks) })));
      }

      // 글 작성
      if (p === '/api/posts' && m === 'POST') {
        const b = await request.json();
        const id = 'post_' + Date.now();
        await env.DB.prepare('INSERT INTO posts(id,author,blocks,created_at) VALUES(?,?,?,?)')
          .bind(id, b.author, JSON.stringify(b.blocks), Math.floor(Date.now()/1000)).run();
        return json({ id });
      }

      // 글 수정
      if (p.match(/^\/api\/posts\/[^/]+$/) && m === 'PUT') {
        const id = p.split('/')[3];
        const b = await request.json();
        await env.DB.prepare('UPDATE posts SET blocks=? WHERE id=?').bind(JSON.stringify(b.blocks), id).run();
        return json({ ok: true });
      }

      // 글 삭제
      if (p.match(/^\/api\/posts\/[^/]+$/) && m === 'DELETE') {
        const id = p.split('/')[3];
        await env.DB.prepare('DELETE FROM posts WHERE id=?').bind(id).run();
        await env.DB.prepare('DELETE FROM comments WHERE post_id=?').bind(id).run();
        await env.DB.prepare('DELETE FROM likes WHERE post_id=?').bind(id).run();
        return json({ ok: true });
      }

      // 내가 좋아요한 포스트 목록
      if (p === '/api/likes' && m === 'GET') {
        const userId = url.searchParams.get('user_id');
        if (!userId) return json([]);
        const rows = await env.DB.prepare('SELECT post_id FROM likes WHERE user_id=?').bind(userId).all();
        return json(rows.results.map(r => r.post_id));
      }

      // 좋아요 토글
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

      // 댓글 목록
      if (p.match(/^\/api\/posts\/[^/]+\/comments$/) && m === 'GET') {
        const postId = p.split('/')[3];
        const rows = await env.DB.prepare('SELECT * FROM comments WHERE post_id=? ORDER BY created_at ASC').bind(postId).all();
        return json(rows.results);
      }

      // 댓글 작성
      if (p.match(/^\/api\/posts\/[^/]+\/comments$/) && m === 'POST') {
        const postId = p.split('/')[3];
        const b = await request.json();
        const id = 'cmt_' + Date.now() + '_' + Math.random().toString(36).slice(2,6);
        await env.DB.prepare('INSERT INTO comments(id,post_id,author,content,created_at) VALUES(?,?,?,?,?)')
          .bind(id, postId, b.author, b.content, Math.floor(Date.now()/1000)).run();
        return json({ id, post_id: postId, author: b.author, content: b.content, created_at: Math.floor(Date.now()/1000) });
      }

      // 댓글 삭제
      if (p.match(/^\/api\/comments\/[^/]+$/) && m === 'DELETE') {
        const id = p.split('/')[3];
        await env.DB.prepare('DELETE FROM comments WHERE id=?').bind(id).run();
        return json({ ok: true });
      }

      // 경조사 목록
      if (p === '/api/events' && m === 'GET') {
        const rows = await env.DB.prepare('SELECT * FROM events ORDER BY created_at DESC').all();
        return json(rows.results);
      }

      // 경조사 작성
      if (p === '/api/events' && m === 'POST') {
        const b = await request.json();
        const id = 'evt_' + Date.now();
        await env.DB.prepare('INSERT INTO events(id,author,type,title,content,created_at) VALUES(?,?,?,?,?,?)')
          .bind(id, b.author, b.type||'기타', b.title, b.content||'', Math.floor(Date.now()/1000)).run();
        return json({ id });
      }

      // 경조사 삭제
      if (p.match(/^\/api\/events\/[^/]+$/) && m === 'DELETE') {
        const id = p.split('/')[3];
        await env.DB.prepare('DELETE FROM events WHERE id=?').bind(id).run();
        return json({ ok: true });
      }

      // 투표 세션 목록
      if (p === '/api/votes' && m === 'GET') {
        const rows = await env.DB.prepare('SELECT * FROM vote_sessions ORDER BY created_at DESC').all();
        return json(rows.results);
      }

      // 투표 세션 생성 (admin)
      if (p === '/api/votes' && m === 'POST') {
        const b = await request.json();
        const id = 'vote_' + Date.now();
        await env.DB.prepare('INSERT INTO vote_sessions(id,title,description,end_at,created_by,active,created_at) VALUES(?,?,?,?,?,1,?)')
          .bind(id, b.title, b.description||'', b.end_at||0, b.created_by, Math.floor(Date.now()/1000)).run();
        return json({ id });
      }

      // 투표 세션 상세 + 결과
      if (p.match(/^\/api\/votes\/[^/]+$/) && m === 'GET') {
        const id = p.split('/')[3];
        const session = await env.DB.prepare('SELECT * FROM vote_sessions WHERE id=?').bind(id).first();
        if (!session) return json({ error: 'not found' }, 404);
        const votes = await env.DB.prepare('SELECT nominee, COUNT(*) as count FROM user_votes WHERE session_id=? GROUP BY nominee ORDER BY count DESC').bind(id).all();
        const voter = url.searchParams.get('voter');
        const myVote = voter ? (await env.DB.prepare('SELECT nominee FROM user_votes WHERE session_id=? AND voter=?').bind(id, voter).first())?.nominee : null;
        return json({ ...session, results: votes.results, my_vote: myVote });
      }

      // 투표하기
      if (p.match(/^\/api\/votes\/[^/]+\/vote$/) && m === 'POST') {
        const sessionId = p.split('/')[3];
        const { voter, nominee } = await request.json();
        const session = await env.DB.prepare('SELECT * FROM vote_sessions WHERE id=?').bind(sessionId).first();
        if (!session || !session.active) return json({ error: '투표가 종료되었습니다' }, 400);
        const now = Math.floor(Date.now()/1000);
        if (session.end_at && now > session.end_at) return json({ error: '투표 기간이 지났습니다' }, 400);
        const existing = await env.DB.prepare('SELECT 1 FROM user_votes WHERE session_id=? AND voter=?').bind(sessionId, voter).first();
        if (existing) return json({ error: '이미 투표하셨습니다' }, 400);
        await env.DB.prepare('INSERT INTO user_votes(session_id,voter,nominee) VALUES(?,?,?)').bind(sessionId, voter, nominee).run();
        return json({ ok: true });
      }

      // 투표 세션 종료 (admin)
      if (p.match(/^\/api\/votes\/[^/]+\/close$/) && m === 'POST') {
        const id = p.split('/')[3];
        await env.DB.prepare('UPDATE vote_sessions SET active=0 WHERE id=?').bind(id).run();
        return json({ ok: true });
      }

      // 투표 세션 삭제 (admin)
      if (p.match(/^\/api\/votes\/[^/]+$/) && m === 'DELETE') {
        const id = p.split('/')[3];
        await env.DB.prepare('DELETE FROM vote_sessions WHERE id=?').bind(id).run();
        await env.DB.prepare('DELETE FROM user_votes WHERE session_id=?').bind(id).run();
        return json({ ok: true });
      }

      // 사용자 역할 목록
      if (p === '/api/roles' && m === 'GET') {
        const rows = await env.DB.prepare('SELECT * FROM user_roles').all();
        return json(rows.results);
      }

      // 사용자 역할 설정 (admin)
      if (p.match(/^\/api\/roles\/[^/]+$/) && m === 'PUT') {
        const userId = decodeURIComponent(p.split('/')[3]);
        const { role } = await request.json();
        await env.DB.prepare('INSERT INTO user_roles(user_id,role) VALUES(?,?) ON CONFLICT(user_id) DO UPDATE SET role=?')
          .bind(userId, role, role).run();
        return json({ ok: true });
      }

      // 용량 확인
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
