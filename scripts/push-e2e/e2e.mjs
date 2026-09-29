// Content rule, direct messages (fn::push::test, `spky push send`) and
// throttling through a real push service. See README.md.
import { execFileSync } from 'node:child_process';
import { PROJECT, SPKY, PORT, browser, log, prepareWeb, serve, user } from './lib.mjs';

prepareWeb();
const server = serve();
const alice = await user('alice');
const bob = await user('bob');
const [info] = await bob.db.query('RETURN fn::push::info()');
if (!info?.enabled) throw new Error('no host published a VAPID key: is `spky dev` running on project/?');

const ctx = await browser();
const page = await ctx.newPage();
await page.goto(`http://localhost:${PORT}/`);
const sub = await page.evaluate((k) => window.subscribeWith(k), info.publicKey);
await bob.db.query('RETURN fn::push::subscribe($s, { label: "e2e chrome", rules: ["new-message"] })', { s: sub });
log('subscribed', sub.endpoint.slice(0, 48) + '...');

const pushes = () => page.evaluate(() => window.__pushes.filter((m) => m.type === 'e2e-push'));
async function next(count, ms = 30000) {
  const t0 = Date.now();
  for (;;) {
    const got = await pushes();
    if (got.length >= count || Date.now() - t0 > ms) return got[count - 1];
    await new Promise((r) => setTimeout(r, 200));
  }
}
const checks = [];
const check = (name, ok, detail) => { checks.push({ name, ok }); log(ok ? 'PASS' : 'FAIL', name, detail ?? ''); };

let t = Date.now();
await alice.db.query('CREATE message CONTENT { sender: $auth.id, recipient: type::record($r), text: "hello from alice" }', { r: bob.id });
let p = await next(1);
check('rule push', p?.payload?.notification?.title === `${alice.name} wrote` && p.payload.notification.body === 'hello from alice', p ? `${p.at - t} ms` : 'nothing');

t = Date.now();
await bob.db.query('RETURN fn::push::test()');
p = await next(2);
check('fn::push::test', p?.payload?.kind === 'message' && p.payload.data?.test === true, p ? `${p.at - t} ms` : 'nothing');

t = Date.now();
execFileSync(SPKY, ['push', 'send', '--to', bob.id, '--title', 'From the CLI', '--body', 'root direct message', '--link', '/cli'], { cwd: PROJECT });
p = await next(3);
check('spky push send', p?.payload?.notification?.title === 'From the CLI', p ? `${p.at - t} ms` : 'nothing');

// Outside the rule's 5 s throttle window, three quick messages give one push
// now and one trailing push carrying the last one.
await new Promise((r) => setTimeout(r, 5500));
t = Date.now();
for (let i = 1; i <= 3; i++) {
  await alice.db.query('CREATE message CONTENT { sender: $auth.id, recipient: type::record($r), text: $x }', { r: bob.id, x: 'burst ' + i });
}
await new Promise((r) => setTimeout(r, 8000));
const burst = (await pushes()).slice(3).map((m) => m.payload?.notification?.body);
// Observes run concurrently, so the immediate one may be burst 1 or 2; the
// trailing one must be the newest row.
check('throttle', burst.length === 2 && burst[0] !== 'burst 3' && burst[1] === 'burst 3', JSON.stringify(burst));

const shown = await page.evaluate(() => window.shown());
check('service worker showed them', shown.some((n) => n.title === 'From the CLI'), `${shown.length} on screen`);

await ctx.close();
server.close();
await alice.db.close();
await bob.db.close();
process.exit(checks.every((c) => c.ok) ? 0 : 1);
