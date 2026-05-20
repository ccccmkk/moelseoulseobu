const CACHE = 'step-app-v5';

self.addEventListener('install', () => self.skipWaiting());

self.addEventListener('activate', e => {
  e.waitUntil(
    caches.keys()
      .then(keys => Promise.all(keys.filter(k => k !== CACHE).map(k => caches.delete(k))))
      .then(() => self.clients.claim())
      .then(() => {
        // 업데이트된 SW가 활성화되면 열린 탭 전체에 알림
        self.clients.matchAll({ type: 'window' }).then(clients => {
          clients.forEach(client => client.postMessage('sw-updated'));
        });
      })
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

  // network-first: 항상 최신 버전 먼저 시도, 오프라인이면 캐시 폴백
  e.respondWith(
    fetch(req).then(res => {
      if (res.ok) {
        const clone = res.clone();
        caches.open(CACHE).then(cache => cache.put(req, clone));
      }
      return res;
    }).catch(() => caches.match(req))
  );
});

// forceUpdate 메시지: 캐시 전체 삭제
self.addEventListener('message', e => {
  if (e.data === 'clearCache') {
    caches.keys().then(keys => Promise.all(keys.map(k => caches.delete(k))));
  }
});
