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

export default {
  async fetch(request, env) {
    if (request.method === 'OPTIONS') return new Response(null, { headers: CORS });

    await env.DB.exec(`CREATE TABLE IF NOT EXISTS posts (id TEXT PRIMARY KEY, author TEXT, blocks TEXT, created_at INTEGER, like_count INTEGER DEFAULT 0)`);
    await env.DB.exec(`CREATE TABLE IF NOT EXISTS usage (id INTEGER PRIMARY KEY, bytes INTEGER DEFAULT 0)`);

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
      if (p.startsWith('/api/posts/') && m === 'PUT') {
        const id = p.split('/')[3];
        const b = await request.json();
        await env.DB.prepare('UPDATE posts SET blocks=? WHERE id=?')
          .bind(JSON.stringify(b.blocks), id).run();
        return json({ ok: true });
      }

      // 글 삭제
      if (p.startsWith('/api/posts/') && m === 'DELETE') {
        const id = p.split('/')[3];
        await env.DB.prepare('DELETE FROM posts WHERE id=?').bind(id).run();
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
