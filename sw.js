const CACHE_NAME = 'inventory-app-cache-v1';
const ASSET_PATTERNS = [
  /^\/$/,
  /^\/index\.html$/,
  /^\/assets\//,
  /^\/favicon\.svg$/,
  /^\/brand-logo\.svg$/,
  /^\/splash-screen\.svg$/,
];

self.addEventListener('install', (event) => {
  event.waitUntil(
    caches.open(CACHE_NAME).then((cache) => cache.addAll(['/']))
  );
  self.skipWaiting();
});

self.addEventListener('activate', (event) => {
  event.waitUntil(
    caches.keys().then((keys) => Promise.all(
      keys.filter((key) => key !== CACHE_NAME).map((key) => caches.delete(key))
    ))
  );
  self.clients.claim();
});

self.addEventListener('fetch', (event) => {
  const url = new URL(event.request.url);
  if (url.origin !== self.location.origin) return;

  if (ASSET_PATTERNS.some((pattern) => pattern.test(url.pathname))) {
    event.respondWith(
      caches.match(event.request).then((cached) => {
        if (cached) return cached;
        return fetch(event.request).then((response) => {
          const copy = response.clone();
          caches.open(CACHE_NAME).then((cache) => cache.put(event.request, copy));
          return response;
        });
      })
    );
    return;
  }

  if (url.pathname.startsWith('/sync/') || url.pathname.startsWith('/health')) {
    event.respondWith(fetch(event.request).catch(() => caches.match('/')));
  }
});
