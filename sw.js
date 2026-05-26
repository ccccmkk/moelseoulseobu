const CACHE = 'step-app-v6';

self.addEventListener('install', () => self.skipWaiting());

self.addEventListener('activate', e => {
  e.waitUntil(
    caches.keys()
      .then(keys => Promise.all(keys.filter(k => k !== CACHE).map(k => caches.delete(k))))
      .then(() => self.clients.claim())
      .then(() => {
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
  if (url.hostname !== location.hostname) return;
  const isHtml = url.pathname === '/' || url.pathname.endsWith('/') || url.pathname.endsWith('.html');
  if (!isHtml) return;
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

self.addEventListener('message', e => {
  if (e.data === 'clearCache') {
    caches.keys().then(keys => Promise.all(keys.map(k => caches.delete(k))));
  }
});

// ── 푸시 알림 ──
self.addEventListener('push', e => {
  let title = 'STEP';
  let body = '새 게시물이 올라왔습니다!';
  let data = {};
  if (e.data) {
    try {
      const d = e.data.json();
      title = d.title || title;
      body = d.body || body;
      data = d;
    } catch (_) {
      body = e.data.text() || body;
    }
  }
  e.waitUntil(
    self.registration.showNotification(title, {
      body,
      icon: '/icon-192.png',
      badge: '/icon-192.png',
      data,
      vibrate: [200, 100, 200],
    })
  );
});

self.addEventListener('notificationclick', e => {
  e.notification.close();
  e.waitUntil(
    self.clients.matchAll({ type: 'window', includeUncontrolled: true }).then(clients => {
      for (const c of clients) {
        if (c.url && 'focus' in c) return c.focus();
      }
      return self.clients.openWindow('/');
    })
  );
});
