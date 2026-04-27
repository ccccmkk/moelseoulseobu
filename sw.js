const CACHE = 'step-app-v2';

self.addEventListener('install', () => self.skipWaiting());

self.addEventListener('activate', e => {
  e.waitUntil(
    caches.keys()
      .then(keys => Promise.all(keys.filter(k => k !== CACHE).map(k => caches.delete(k))))
      .then(() => self.clients.claim())
  );
});

self.addEventListener('fetch', e => {
  const req = e.request;
  if (req.method !== 'GET') return;

  const url = new URL(req.url);

  // API 요청은 캐시 안 함
  if (url.hostname !== location.hostname) return;

  // index.html (/, /index.html) 만 캐시
  const isHtml = url.pathname === '/' || url.pathname.endsWith('/') || url.pathname.endsWith('.html');
  if (!isHtml) return;

  // stale-while-revalidate: 캐시 버전 즉시 반환 + 백그라운드에서 업데이트
  e.respondWith(
    caches.open(CACHE).then(async cache => {
      const cached = await cache.match(req);
      const networkFetch = fetch(req).then(res => {
        if (res.ok) cache.put(req, res.clone());
        return res;
      }).catch(() => null);
      return cached || networkFetch;
    })
  );
});

// forceUpdate 메시지: 캐시 전체 삭제
self.addEventListener('message', e => {
  if (e.data === 'clearCache') {
    caches.keys().then(keys => Promise.all(keys.map(k => caches.delete(k))));
  }
});
