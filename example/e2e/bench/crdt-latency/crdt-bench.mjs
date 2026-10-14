// CRDT latency benchmark for the threads example (local stack).
// Two browser contexts: A = author, B = collaborator. Measures
//   1. cold open of a thread on B (navigation -> editable editor)
//   2. single-edit propagation A -> B and B -> A (keystroke -> remote DOM)
//   3. continuous typing A -> B (per-character lag)
// and dumps every page's btrace (core probes) for the stage breakdown.
import { createRequire } from 'module';
import fs from 'fs';
const require = createRequire(new URL('../../package.json', import.meta.url));
const { chromium } = require('@playwright/test');

const BASE = process.env.BASE ?? 'http://localhost:3006';
const OUT = process.env.OUT ?? new URL('./out/', import.meta.url).pathname;
const TRIALS = Number(process.env.TRIALS ?? 12);
const BODY_KB = Number(process.env.BODY_KB ?? 0);
fs.mkdirSync(OUT, { recursive: true });

async function upstream(sql) {
  const resp = await fetch('http://localhost:8666/sql', {
    method: 'POST',
    headers: {
      Accept: 'application/json',
      'surreal-ns': 'main',
      'surreal-db': 'example',
      Authorization: 'Basic ' + Buffer.from('root:root').toString('base64'),
    },
    body: sql,
  });
  const body = await resp.json();
  return body[0]?.result ?? [];
}

const EXP = {
  liveInline: process.env.LIVE_INLINE === '1',
  debounce: process.env.CRDT_DEBOUNCE ? Number(process.env.CRDT_DEBOUNCE) : null,
  maxWait: process.env.CRDT_MAXWAIT ? Number(process.env.CRDT_MAXWAIT) : null,
};
const init = (exp) => {
  if (exp.liveInline) window.__liveInline = true;
  if (exp.debounce != null) window.__crdtDebounceMs = exp.debounce;
  if (exp.maxWait != null) window.__crdtMaxWaitMs = exp.maxWait;
  window.__btrace = [];
  window.__longtasks = [];
  try {
    new PerformanceObserver((list) => {
      for (const e of list.getEntries()) window.__longtasks.push({ t: performance.timeOrigin + e.startTime, ms: e.duration });
    }).observe({ type: 'longtask', buffered: true });
  } catch {}
  window.__now = () => performance.timeOrigin + performance.now();
  // Editor text log: every change of the editor's text, with its time.
  window.__textLog = [];
  // The document's text only: remote cursors render name labels as
  // non-editable widgets inside the editor, which textContent would count.
  window.__docText = (el) => {
    let out = '';
    const walk = (n) => {
      for (const c of n.childNodes) {
        if (c.nodeType === 3) out += c.nodeValue;
        else if (c.nodeType === 1 && c.getAttribute('contenteditable') !== 'false') walk(c);
      }
    };
    walk(el);
    return out;
  };
  window.__watchEditor = () => {
    const el = document.querySelector('.ProseMirror');
    if (!el || el.__watched) return !!el;
    el.__watched = true;
    let last = window.__docText(el);
    const mo = new MutationObserver(() => {
      const now = window.__now();
      const txt = window.__docText(el);
      if (txt !== last) {
        last = txt;
        window.__textLog.push({ t: now, len: txt.length, txt });
      }
    });
    mo.observe(el, { subtree: true, childList: true, characterData: true });
    return true;
  };
  window.__waitText = (token, timeoutMs = 30000) =>
    new Promise((resolve) => {
      const start = window.__now();
      const check = () => {
        const el = document.querySelector('.ProseMirror');
        if (el && el.textContent.includes(token)) return resolve(window.__now());
        if (window.__now() - start > timeoutMs) return resolve(null);
        requestAnimationFrame(check);
      };
      // MutationObserver for exactness, rAF as a fallback.
      const el = document.querySelector('.ProseMirror');
      if (el) {
        const mo = new MutationObserver(() => {
          if (el.textContent.includes(token)) {
            mo.disconnect();
            resolve(window.__now());
          }
        });
        mo.observe(el, { subtree: true, childList: true, characterData: true });
      }
      check();
    });
  window.__marks = {};
  const markSel = { editor: '.ProseMirror', editable: '.ProseMirror[contenteditable="true"]', titleRw: '#thread-title:not([readonly])' };
  const domMo = new MutationObserver(() => {
    for (const [name, sel] of Object.entries(markSel)) {
      if (!window.__marks[name] && document.querySelector(sel)) window.__marks[name] = window.__now();
    }
  });
  document.addEventListener('DOMContentLoaded', () => domMo.observe(document.documentElement, { subtree: true, childList: true, attributes: true }));
  window.__keyLog = [];
  document.addEventListener(
    'keydown',
    (e) => window.__keyLog.push({ t: window.__now(), key: e.key }),
    true
  );
};

const user = (p) => ({ username: `${p}_${Date.now().toString(36)}${Math.random().toString(36).slice(2, 6)}`, password: 'e2e_testpass' });

async function signUp(page, u) {
  await page.getByRole('button', { name: 'Sign up', exact: true }).first().click();
  const form = page.locator('form').filter({ has: page.locator('#username') });
  await form.locator('#username').fill(u.username);
  await form.locator('#password').fill(u.password);
  await form.getByRole('button', { name: 'Create account', exact: true }).click();
  await page.getByRole('button', { name: 'Sign out' }).waitFor({ timeout: 30000 });
}

async function createThread(page, title, body) {
  await page.getByRole('button', { name: 'New thread' }).click();
  await page.locator('#create-title').fill(title);
  const ed = page.locator('.ProseMirror[contenteditable="true"]');
  await ed.click();
  if (body.length > 2000) {
    await page.keyboard.insertText(body);
  } else {
    await page.keyboard.type(body);
  }
  await page.getByRole('button', { name: 'Publish' }).click();
  await page.waitForURL(/\/thread\//, { timeout: 20000 });
  return new URL(page.url()).pathname.split('/thread/')[1];
}

const editable = (page) => page.locator('.ProseMirror[contenteditable="true"]').first();

async function focusEnd(page) {
  await editable(page).click();
  await page.keyboard.press('ControlOrMeta+End');
}

const median = (xs) => {
  const s = [...xs].filter((x) => x != null).sort((a, b) => a - b);
  if (!s.length) return null;
  return s[Math.floor(s.length / 2)];
};
const pct = (xs, p) => {
  const s = [...xs].filter((x) => x != null).sort((a, b) => a - b);
  if (!s.length) return null;
  return s[Math.min(s.length - 1, Math.floor((p / 100) * s.length))];
};

async function oneEdit(from, to, token) {
  const waiting = to.evaluate((tok) => window.__waitText(tok), token);
  await from.evaluate(() => (window.__keyLog.length = 0));
  await from.keyboard.type(token);
  const keys = await from.evaluate(() => window.__keyLog.slice());
  const tKey = keys.length ? keys[keys.length - 1].t : null;
  const tSeen = await waiting;
  return { token, tKey, tSeen, ms: tSeen && tKey ? tSeen - tKey : null };
}

async function main() {
  const browser = await chromium.launch({ headless: process.env.HEADED ? false : true });
  const ctxA = await browser.newContext();
  const ctxB = await browser.newContext();
  await ctxA.addInitScript(init, EXP);
  await ctxB.addInitScript(init, EXP);
  const A = await ctxA.newPage();
  const B = await ctxB.newPage();
  const consoleErrors = [];
  for (const [n, p] of [['A', A], ['B', B]]) p.on('console', (m) => { if (m.type() === 'error' || m.type() === 'warning') consoleErrors.push(`${n} ${m.type()}: ${m.text().slice(0, 300)}`); });

  await A.goto(BASE);
  await A.waitForSelector('header', { timeout: 60000 });
  await B.goto(BASE);
  await B.waitForSelector('header', { timeout: 60000 });
  const ua = user('alice'), ub = user('bob');
  await signUp(A, ua);
  await signUp(B, ub);
  console.log('signed up', ua.username, ub.username, 'exp', JSON.stringify(EXP));

  const body = BODY_KB > 0 ? 'lorem ipsum dolor sit amet '.repeat(Math.ceil((BODY_KB * 1024) / 27)).slice(0, BODY_KB * 1024) : 'hello from alice';
  const title = `bench ${Date.now().toString(36)}`;
  const key = await createThread(A, title, body);
  console.log('thread', key);
  await A.locator('#thread-title').waitFor();
  await upstream(`UPDATE type::record('thread', '${key}') SET published = true`);
  const [bid] = await upstream(`SELECT VALUE id FROM user WHERE username = '${ub.username}'`);
  const rel = await upstream(`LET $t = type::record('thread', '${key}'); RELATE ${bid}->collaborates_on->$t;`); console.log('relate', JSON.stringify(await upstream(`SELECT * FROM collaborates_on WHERE out = type::record('thread', '${key}')`)));

  // Let A settle (its own open + first flush), then B opens cold.
  await editable(A).waitFor({ timeout: 30000 });
  await A.waitForTimeout(3000);

  const tNav = await B.evaluate(() => ((window.__marks = {}), window.__now()));
  await B.evaluate((k) => { history.pushState({}, '', `/thread/${k}`); dispatchEvent(new PopStateEvent('popstate')); }, key);
  try {
    await editable(B).waitFor({ timeout: 30000 });
  } catch (e) {
    await B.screenshot({ path: `${OUT}/B-fail.png` });
    console.log('B url', B.url(), (await B.locator('body').innerText()).slice(0, 800));
    console.log(consoleErrors.slice(-20).join('\n'));
    throw e;
  }
  const tEditable = await B.evaluate(() => window.__now());
  const tBody = await B.evaluate((tok) => window.__waitText(tok), body.slice(0, 16));
  const marks = await B.evaluate(() => window.__marks);
  console.log('B open DOM marks (ms):', Object.fromEntries(Object.entries(marks).map(([k, v]) => [k, +(v - tNav).toFixed(0)])));
  console.log(`B cold open: editable ${(tEditable - tNav).toFixed(0)} ms, body visible ${(tBody - tNav).toFixed(0)} ms`);

  await A.evaluate(() => window.__watchEditor());
  await B.evaluate(() => window.__watchEditor());
  await B.waitForTimeout(3000);

  // --- single edits, alternating idle gaps --------------------------------
  await focusEnd(A);
  await focusEnd(B);
  const ab = [], ba = [];
  for (let i = 0; i < TRIALS; i++) {
    ab.push(await oneEdit(A, B, ` a${i}x`));
    await A.waitForTimeout(800 + Math.random() * 1500);
  }
  for (let i = 0; i < Math.ceil(TRIALS / 2); i++) {
    ba.push(await oneEdit(B, A, ` b${i}y`));
    await B.waitForTimeout(800 + Math.random() * 1500);
  }
  console.log('A->B single edit ms:', ab.map((r) => r.ms?.toFixed(0)).join(' '));
  console.log('B->A single edit ms:', ba.map((r) => r.ms?.toFixed(0)).join(' '));
  console.log(`A->B median ${median(ab.map((r) => r.ms))?.toFixed(0)} p90 ${pct(ab.map((r) => r.ms), 90)?.toFixed(0)}`);

  // --- continuous typing ------------------------------------------------------
  await A.waitForTimeout(2000);
  await B.evaluate(() => (window.__textLog.length = 0));
  await A.evaluate(() => ((window.__textLog.length = 0), (window.__keyLog.length = 0)));
  const sentence = ' the quick brown fox jumps over the lazy dog and keeps on running far away';
  await A.keyboard.type(sentence, { delay: 110 });
  const end = await B.evaluate((tok) => window.__waitText(tok, 30000), sentence.slice(-12));
  const logA = await A.evaluate(() => window.__textLog.slice());
  const logB = await B.evaluate(() => window.__textLog.slice());
  // Per character typed on A: when B's document first held the sentence up
  // to and including it.
  const keysA = (await A.evaluate(() => window.__keyLog.slice())).slice(-sentence.length);
  const lags = [];
  for (let k = 0; k < keysA.length; k++) {
    const prefix = sentence.slice(0, k + 1);
    const b = logB.find((x) => x.t >= keysA[k].t - 1 && x.txt.includes(prefix));
    if (b) lags.push(b.t - keysA[k].t);
  }
  console.log(`typing A->B: chars ${keysA.length}, B updates ${logB.length}, lag median ${median(lags)?.toFixed(0)} p90 ${pct(lags, 90)?.toFixed(0)} max ${Math.max(...lags).toFixed(0)}; last char seen +${end && logA.length ? (end - logA[logA.length - 1].t).toFixed(0) : '?'} ms`);

  await A.waitForTimeout(8000);
  const dump = {
    exp: EXP, base: BASE,
    key, ua: ua.username, ub: ub.username, body_kb: BODY_KB,
    open: { tNav, tEditable, tBody, marks },
    ab, ba, typing: { logA, logB, lags },
    A: { btrace: await A.evaluate(() => window.__btrace), longtasks: await A.evaluate(() => window.__longtasks) },
    B: { btrace: await B.evaluate(() => window.__btrace), longtasks: await B.evaluate(() => window.__longtasks) },
    consoleErrors,
  };
  const file = `${OUT}/run-${Date.now()}.json`;
  fs.writeFileSync(file, JSON.stringify(dump));
  console.log('wrote', file, 'console errors/warnings:', consoleErrors.length);
  await browser.close();
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
