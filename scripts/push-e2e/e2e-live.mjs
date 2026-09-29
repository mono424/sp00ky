// A nudge rule rendered by the service worker from live data
// (`installPushHandlers({ live })` over `createLiveFeed`). See README.md.
import { DB, NS, PORT, browser, log, prepareWeb, serve, user } from './lib.mjs';

prepareWeb();
const server = serve();
const alice = await user('la');
const bob = await user('lb');
const [info] = await bob.db.query('RETURN fn::push::info()');

const ctx = await browser();
const page = await ctx.newPage();
await page.goto(`http://localhost:${PORT}/live/`);
const sub = await page.evaluate((k) => window.subscribeWith(k), info.publicKey);
await bob.db.query('RETURN fn::push::subscribe($s, { label: "live e2e", rules: ["alert-nudge"] })', { s: sub });
await page.evaluate((m) => window.bridge(m), { type: 'sp00ky:token', token: bob.token, userId: bob.id, endpoint: DB, ...NS });

async function until(pred, ms = 30000) {
  const t0 = Date.now();
  for (;;) {
    const s = await page.evaluate(() => window.shown());
    if (pred(s) || Date.now() - t0 > ms) return { shown: s, ms: Date.now() - t0 };
    await new Promise((r) => setTimeout(r, 200));
  }
}
// alice cannot read bob's alert back (select is owner-only), so name the id.
const id = 'alert:a' + Date.now().toString(36);
await alice.db.query('CREATE type::record($id) CONTENT { owner: type::record($b), title: "Server is on fire" }', { id, b: bob.id });
const shown = await until((s) => s.some((n) => n.title === 'Server is on fire'));
const ok1 = shown.shown.some((n) => n.title === 'Server is on fire');
log(ok1 ? 'PASS' : 'FAIL', 'rendered from the live feed', `${shown.ms} ms`);
await bob.db.query('UPDATE type::record($a) SET seen = true', { a: id });
const closed = await until((s) => !s.some((n) => n.title === 'Server is on fire'));
const ok2 = !closed.shown.some((n) => n.title === 'Server is on fire') && closed.shown.length === 0;
log(ok2 ? 'PASS' : 'FAIL', 'closed silently once seen', `${closed.ms} ms, ${JSON.stringify(closed.shown)}`);

await ctx.close();
server.close();
await alice.db.close();
await bob.db.close();
process.exit(ok1 && ok2 ? 0 : 1);
