// Stage breakdown of single edits from a crdt-bench dump.
import fs from 'fs';
const file = process.argv[2];
const d = JSON.parse(fs.readFileSync(file, 'utf8'));

function stages(trial, src, dst) {
  const S = src.btrace, D = dst.btrace;
  const t0 = trial.tKey;
  const after = (arr, t, pred) => arr.find((e) => e.t >= t && pred(e));
  const local = after(S, t0 - 50, (e) => e.ev === 'crdt:local' && e.key.includes('|content'));
  const flush = after(S, t0, (e) => e.ev === 'crdt:flush' && e.key.includes('|content'));
  const enq = flush && after(S, flush.t, (e) => e.ev === 'crdt:enqueue' && e.key.includes('|content'));
  const rid = enq?.rid;
  const enqd = rid && after(S, enq.t, (e) => e.ev === 'crdt:enqueued' && e.rid === rid);
  const pushS = rid && after(S, enq.t, (e) => e.ev === 'push:send' && e.ids.includes(rid));
  const pushD = rid && after(S, enq.t, (e) => e.ev === 'push:done' && e.ids.includes(rid));
  const live = rid && after(D, enq.t, (e) => e.ev === 'live' && e.out === rid);
  const liveAny = after(D, enq?.t ?? t0, (e) => e.ev === 'live');
  const memS = live && after(D, live.t, (e) => e.ev === 'member:send');
  const memR = memS && after(D, memS.t, (e) => e.ev === 'member:recv');
  const fS = rid && after(D, enq.t, (e) => e.ev === 'fetch:send' && e.ids.includes(rid));
  const fR = rid && after(D, enq.t, (e) => e.ev === 'fetch:recv' && e.ids.includes(rid));
  const fL = rid && after(D, enq.t, (e) => e.ev === 'fetch:landed' && e.ids.includes(rid));
  const rows = rid && after(D, enq.t, (e) => e.ev === 'crdt:rows' && e.fresh.includes(rid));
  const imp = rows && after(D, rows.t, (e) => e.ev === 'crdt:imported');
  const r = (e) => (e ? +(e.t - t0).toFixed(0) : null);
  return {
    total: +trial.ms.toFixed(0),
    local: r(local), flush: r(flush), enqueue: r(enq), enqueued: r(enqd), pushSend: r(pushS), pushDone: r(pushD),
    live: r(live), liveFirstAny: r(liveAny), memSend: r(memS), memRecv: r(memR), fetchSend: r(fS), fetchRecv: r(fR), fetchLanded: r(fL),
    rows: r(rows), imported: r(imp), seen: r({ t: trial.tSeen }),
  };
}

const A = d.A, B = d.B;
const rowsAB = d.ab.map((t) => stages(t, A, B));
const rowsBA = d.ba.map((t) => stages(t, B, A));
console.table(rowsAB);
console.table(rowsBA);
const med = (k, rows) => {
  const s = rows.map((r) => r[k]).filter((x) => x != null).sort((a, b) => a - b);
  return s.length ? s[Math.floor(s.length / 2)] : null;
};
const keys = Object.keys(rowsAB[0]);
console.log('median A->B', Object.fromEntries(keys.map((k) => [k, med(k, rowsAB)])));

// Remote statements on the receiving side: which ran, how long, waits.
const stmts = B.btrace.filter((e) => e.ev === 'stmt:end' && e.kind !== 'LocalDatabaseService');
const byQ = {};
for (const s of stmts) {
  const k = s.kind + ' ' + s.q.replace(/\s+/g, ' ').slice(0, 90);
  (byQ[k] ??= []).push(s.ms);
}
console.log('\nB statements (count, median ms, max ms):');
for (const [k, v] of Object.entries(byQ).sort((a, b) => b[1].length - a[1].length).slice(0, 25)) {
  v.sort((a, b) => a - b);
  console.log(String(v.length).padStart(5), v[Math.floor(v.length / 2)].toFixed(1).padStart(8), v[v.length - 1].toFixed(1).padStart(8), ' ', k);
}
const persists = A.btrace.filter((e) => e.ev === 'crdt:persist');
console.log('\nA persist count', persists.length, 'median', JSON.stringify(persists[Math.floor(persists.length / 2)]));
const imports = B.btrace.filter((e) => e.ev === 'crdt:imported');
console.log('B imports', imports.length, 'max ms', Math.max(...imports.map((e) => e.ms)).toFixed(1));
console.log('longtasks A', A.longtasks.length, 'sum', A.longtasks.reduce((a, b) => a + b.ms, 0).toFixed(0), 'B', B.longtasks.length, B.longtasks.reduce((a, b) => a + b.ms, 0).toFixed(0));
const pres = A.btrace.filter((e) => e.ev === 'crdt:presence');
console.log('A presence sends', pres.length);
const live = B.btrace.filter((e) => e.ev === 'live');
console.log('B live notifications', live.length, 'hashes', new Set(live.map((e) => e.hash)).size);
