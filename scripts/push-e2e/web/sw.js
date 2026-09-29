import { installPushHandlers } from './core/sw.js';
// Record every push for the driver, next to the real bridge.
self.addEventListener('push', (e) => {
  let payload = null;
  try { payload = e.data ? e.data.json() : null; } catch (err) { payload = { parseError: String(err) }; }
  e.waitUntil(self.clients.matchAll({ includeUncontrolled: true, type: 'window' })
    .then((cs) => cs.forEach((c) => c.postMessage({ type: 'e2e-push', at: Date.now(), payload }))));
});
installPushHandlers({ suppressWhenVisible: false });
self.addEventListener('install', () => self.skipWaiting());
self.addEventListener('activate', (e) => e.waitUntil(self.clients.claim()));
