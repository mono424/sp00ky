import { installPushHandlers } from '../core/sw.js';
import { createLiveFeed } from '../core/live.js';
installPushHandlers({
  suppressWhenVisible: false,
  live: {
    feed: (t) => createLiveFeed({ endpoint: t.endpoint, namespace: t.namespace, database: t.database, token: t.token, onError: (e) => console.log('feed error', String(e)) }),
    query: () => ({ surql: 'SELECT * FROM alert' }),
    visible: (row) => !row.seen,
    render: (row) => ({ tag: 'alert:' + String(row.id), title: row.title, body: 'rendered from the live feed' }),
  },
});
self.addEventListener('install', () => self.skipWaiting());
self.addEventListener('activate', (e) => e.waitUntil(self.clients.claim()));
