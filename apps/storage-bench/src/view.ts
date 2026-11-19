/**
 * The benchmark's page: every scenario as a row of bars, one bar per backend,
 * with the number beside it. Bars of one row share a scale and rows do not
 * (a 7 second load and a 0.2 ms read have no common one worth looking at), so
 * what a row shows is who is ahead and by how much; the number is the size.
 */
import type { TabsResults } from './tabs';

export interface Metric {
  /** Median of the timed runs, or the one total for a single run. */
  ms: number;
  p95?: number;
  runs?: number;
  note?: string;
}

export type Results = Record<string, Record<string, Metric>> & { tabs?: TabsResults };

export interface RunRecord {
  at: number;
  label: string;
  config: Record<string, number>;
  results: Results;
}

export const ORDER = ['indexeddb', 'sqlite', 'rows'] as const;

const NAMES: Record<string, string> = { indexeddb: 'IndexedDB', sqlite: 'SQLite engine', rows: 'Row store' };
const COLOR: Record<string, string> = { indexeddb: 'var(--s1)', sqlite: 'var(--s2)', rows: 'var(--s3)' };

type Row = [key: string, label: string, sub?: string];

const SECTIONS: [title: string, about: string, rows: Row[]][] = [
  [
    'Opening the app',
    'A fresh page, as an app gets to its first query: the script downloads (main thread idle), runs its startup work (main thread busy), then asks.',
    [
      ['boot_paint', 'First rows on screen', 'what a new query renders with: the row store seeds it from its paint cache'],
      ['boot_total', 'First verified read', 'open the store and read the 50 rows of the first screen'],
    ],
  ],
  [
    'Opening the app, worst case',
    'The same, asked the instant the script runs: nothing had time to start ahead of the app.',
    [
      ['cold_paint', 'First rows on screen'],
      ['cold_total', 'First verified read'],
      ['reload_total', 'First verified read', 'after 200 writes and a reload that closed nothing'],
    ],
  ],
  [
    'Reads',
    'The store is open. Median of a few hundred reads.',
    [
      ['window50', 'A window of 50 rows by id'],
      ['window50_fields', '6 fields of 50 rows'],
      ['screen30', 'A screen', '30 games, each with its player and top 3 comments'],
      ['children30', 'Comments of 30 games', 'by foreign key'],
      ['where_indexable', 'Filter, sort, limit 50', 'on a field IndexedDB was given an index for'],
      ['where_scan', 'Filter, sort, limit 50', 'on a field nobody declared an index for'],
      ['versions', '(id, version) of every game'],
    ],
  ],
  [
    'Writes',
    '',
    [
      ['ingest', 'Load the whole dataset', 'sync-down batches of 250 and 500 rows'],
      ['merge250', 'Merge a patch into 250 rows'],
      ['put1', 'Merge one row'],
      ['user_write', 'A user write, durable', 'the row and its outbox entry'],
    ],
  ],
  ['First time', 'The first call after loading: where a backend pays for what it builds lazily.', [
    ['first_screen30', 'A screen, first time'],
    ['first_where_scan', 'Filter on a new field, first time', 'the row store builds its index here, once'],
  ]],
  ['Disk', '', [['disk_mb', 'Size on disk', 'megabytes, not a time']]],
];

const TAB_ROWS: Row[] = [
  ['window50_idle', 'A window of 50 rows', 'leader tab idle'],
  ['window50_busy', 'A window of 50 rows', 'leader tab syncing down'],
  ['filter_idle', 'Filter, sort, limit 50', 'leader tab idle'],
  ['filter_busy', 'Filter, sort, limit 50', 'leader tab syncing down'],
];

const esc = (s: string): string => s.replace(/[&<>"]/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' })[c] ?? c);

export function fmt(key: string, v: number): string {
  if (key === 'disk_mb') return `${v.toFixed(0)} MB`;
  if (v >= 1000) return `${(v / 1000).toFixed(1)} s`;
  if (v >= 100) return `${v.toFixed(0)} ms`;
  if (v >= 10) return `${v.toFixed(1)} ms`;
  return `${v.toFixed(v < 1 ? 2 : 1)} ms`;
}

const times = (ratio: number): string => (ratio >= 10 ? ratio.toFixed(0) : ratio.toFixed(1)) + 'x';

interface Bar {
  name: string;
  color: string;
  metric: Metric | undefined;
  /** The same bar in the run this one is compared with. */
  before?: Metric;
}

function barRow(key: string, label: string, sub: string | undefined, bars: Bar[]): string {
  const present = bars.filter((b) => b.metric);
  if (present.length === 0) return '';
  const scale = Math.max(...present.map((b) => Math.max(b.metric!.ms, b.metric!.p95 ?? 0)));
  const best = Math.min(...present.map((b) => b.metric!.ms));
  let html = `<div class="row"><div class="label">${esc(label)}${sub ? `<small>${esc(sub)}</small>` : ''}</div><div class="bars">`;
  for (const bar of bars) {
    const m = bar.metric;
    if (!m) continue;
    const isBest = m.ms === best && present.length > 1;
    const ratio = m.ms / Math.max(best, 0.05);
    const tip = [
      bar.name,
      bar.color,
      fmt(key, m.ms) + (m.p95 !== undefined && key !== 'disk_mb' ? `, p95 ${fmt(key, m.p95)}` : '') + (m.runs ? `, ${m.runs} runs` : ''),
      present.length < 2 ? '' : isBest ? 'lowest in this row' : `${times(ratio)} the lowest`,
      bar.before ? `was ${fmt(key, bar.before.ms)} in the compared run` : '',
    ].join('|');
    const width = Math.max(0.6, (m.ms / scale) * 100);
    const tick = m.p95 !== undefined && key !== 'disk_mb' ? `<i class="p95" style="left:${Math.min(100, (m.p95 / scale) * 100)}%"></i>` : '';
    let delta = '';
    if (bar.before && bar.before.ms > 0) {
      const change = (m.ms - bar.before.ms) / bar.before.ms;
      // Under a tenth of a millisecond is the page clock's own step.
      if (Math.abs(change) >= 0.05 && Math.abs(m.ms - bar.before.ms) >= 0.1) {
        delta = `<span class="delta ${change < 0 ? 'down' : 'up'}">${change < 0 ? '▼' : '▲'} ${Math.abs(change * 100).toFixed(0)}%</span>`;
      }
    }
    html +=
      `<div class="bar" data-tip="${esc(tip)}"><div class="track"><div class="fill" style="width:${width}%;background:${bar.color}"></div>${tick}</div>` +
      `<div class="val${isBest ? ' best' : ''}">${fmt(key, m.ms)}${delta}</div></div>`;
  }
  return html + '</div></div>';
}

/** Who has the lowest number in how many of the timed rows. */
function scorecard(results: Results): string {
  const wins: Record<string, number> = { indexeddb: 0, sqlite: 0, rows: 0 };
  let total = 0;
  for (const [, , rows] of SECTIONS) {
    for (const [key] of rows) {
      if (key === 'disk_mb') continue;
      const cells = ORDER.map((name) => [name, results[name]?.[key]?.ms] as const).filter((c): c is [typeof ORDER[number], number] => c[1] !== undefined);
      if (cells.length < ORDER.length) continue;
      total++;
      const best = Math.min(...cells.map((c) => c[1]));
      // A tie at the clock's resolution counts for both.
      for (const [name, ms] of cells) if (ms - best < 0.05) wins[name]++;
    }
  }
  if (total === 0) return '';
  const boot = (name: string): string => {
    const m = results[name]?.boot_paint;
    return m ? fmt('boot_paint', m.ms) : '-';
  };
  return (
    '<div class="tiles">' +
    ORDER.map(
      (name) =>
        `<div class="tile"><div class="who"><i style="background:${COLOR[name]}"></i>${NAMES[name]}</div>` +
        `<div class="big">${wins[name]}<small> of ${total}</small></div><div class="cap">scenarios where it is the fastest</div>` +
        `<div class="line"><span>first rows on screen</span><b>${boot(name)}</b></div></div>`
    ).join('') +
    '</div>'
  );
}

function table(results: Results): string {
  let html = `<table><thead><tr><th>median <small>(p95)</small></th>${ORDER.map((n) => `<th>${NAMES[n]}</th>`).join('')}</tr></thead><tbody>`;
  for (const [title, , rows] of SECTIONS) {
    html += `<tr><th colspan="4" class="group">${esc(title)}</th></tr>`;
    for (const [key, label, sub] of rows) {
      const cells = ORDER.map((name) => results[name]?.[key]);
      if (cells.every((c) => !c)) continue;
      const best = Math.min(...cells.filter(Boolean).map((c) => c!.ms));
      html += `<tr><td>${esc(label)}${sub ? ` <small>(${esc(sub)})</small>` : ''}</td>`;
      for (const cell of cells) {
        if (!cell) html += '<td>-</td>';
        else {
          const p95 = cell.p95 !== undefined && key !== 'disk_mb' ? ` <small>(${fmt(key, cell.p95).replace(/ (ms|s)$/, '')})</small>` : '';
          html += `<td class="${cell.ms === best ? 'best' : ''}">${fmt(key, cell.ms)}${p95}</td>`;
        }
      }
      html += '</tr>';
    }
  }
  return html + '</tbody></table>';
}

export function render(root: HTMLElement, results: Results, baseline: Results | null): void {
  const present = ORDER.filter((name) => results[name] && Object.keys(results[name]).length > 0);
  let html = '';
  if (present.length > 0) {
    html += scorecard(results);
    html += `<div class="legend">${ORDER.map((n) => `<span><i style="background:${COLOR[n]}"></i>${NAMES[n]}</span>`).join('')}<span class="hint"><i class="p95 static"></i>p95</span></div>`;
    html += '<p class="note">Median, lower is better. The bars of one row share a scale; rows do not.</p>';
    for (const [title, about, rows] of SECTIONS) {
      const body = rows
        .map(([key, label, sub]) =>
          barRow(
            key,
            label,
            sub,
            ORDER.map((name) => ({ name: NAMES[name], color: COLOR[name], metric: results[name]?.[key], before: baseline?.[name]?.[key] }))
          )
        )
        .join('');
      if (body) html += `<section><h2>${esc(title)}</h2>${about ? `<p class="about">${esc(about)}</p>` : ''}${body}</section>`;
    }
  }

  const start = results.rows?.start_hint && results.rows?.start_plain;
  if (start) {
    html +=
      '<section><h2>Starting the row store\'s worker</h2><p class="about">From <code>new Worker()</code> on an idle page to the store being open, with and without the V8 compile hint on the worker\'s first line.</p>' +
      barRow('start', 'Worker started, store open', undefined, [
        { name: 'With compile hint', color: 'var(--s3)', metric: results.rows.start_hint, before: baseline?.rows?.start_hint },
        { name: 'Without', color: 'var(--mute)', metric: results.rows.start_plain, before: baseline?.rows?.start_plain },
      ]) +
      barRow('start', 'Of which: opening the store, inside the worker', 'where functions run for the first time', [
        { name: 'With compile hint', color: 'var(--s3)', metric: results.rows.start_hint_inside, before: baseline?.rows?.start_hint_inside },
        { name: 'Without', color: 'var(--mute)', metric: results.rows.start_plain_inside, before: baseline?.rows?.start_plain_inside },
      ]) +
      '<div class="legend small"><span><i style="background:var(--s3)"></i>with compile hint</span><span><i style="background:var(--mute)"></i>without</span></div></section>';
  }

  const tabs = results.tabs;
  if (tabs && Object.keys(tabs).length > 0) {
    html +=
      '<section><h2>Shared tabs: a second tab\'s reads</h2><p class="about">Two real tabs on one row store. The second tab (a follower) reads either through the leader tab\'s worker, or with <code>followerReads: \'direct\'</code> from the file itself.</p>';
    for (const [key, label, sub] of TAB_ROWS) {
      const entry = tabs[key];
      if (!entry) continue;
      html += barRow(key, label, sub, [
        { name: 'Direct (own reader)', color: 'var(--s3)', metric: entry.direct, before: baseline?.tabs?.[key]?.direct },
        { name: 'Through the leader', color: 'var(--mute)', metric: entry.leader, before: baseline?.tabs?.[key]?.leader },
      ]);
    }
    const note = Object.values(tabs).find((entry) => entry.note)?.note;
    html += `<div class="legend small"><span><i style="background:var(--s3)"></i>direct (own reader)</span><span><i style="background:var(--mute)"></i>through the leader</span></div>${note ? `<p class="about">${esc(note)}</p>` : ''}</section>`;
  }

  if (results.codec) {
    const c = results.codec;
    html +=
      '<section><h2>Stored form of a row</h2><p class="about">5,000 game rows, encoded and decoded one at a time, and as the row store ships a window: all rows in one parse.</p>' +
      barRow('codec', 'Encode', undefined, [
        { name: 'JSON text', color: 'var(--s3)', metric: c.json_encode },
        { name: 'CBOR (the SurrealDB SDK\'s codec)', color: 'var(--mute)', metric: c.cbor_encode },
      ]) +
      barRow('codec', 'Decode', undefined, [
        { name: 'JSON text, one parse for all', color: 'var(--s3)', metric: c.json_decode_one_parse },
        { name: 'JSON text, a parse per row', color: 'var(--s3)', metric: c.json_decode },
        { name: 'CBOR', color: 'var(--mute)', metric: c.cbor_decode },
      ]) +
      `<p class="about">On disk: JSON ${c.json_mb.ms.toFixed(1)} MB, CBOR ${c.cbor_mb.ms.toFixed(1)} MB.</p>` +
      '<div class="legend small"><span><i style="background:var(--s3)"></i>JSON text</span><span><i style="background:var(--mute)"></i>CBOR</span></div></section>';
  }
  if (present.length > 0) html += `<details><summary>The same numbers as a table</summary>${table(results)}</details>`;
  root.innerHTML = html || '<p class="note">No results yet. Press Run.</p>';
}

/** One floating tooltip for every bar. */
export function installTooltip(root: HTMLElement, tip: HTMLElement): void {
  root.addEventListener('mousemove', (ev) => {
    const bar = (ev.target as HTMLElement).closest?.('.bar') as HTMLElement | null;
    if (!bar?.dataset.tip) {
      tip.style.display = 'none';
      return;
    }
    const [name, color, value, rank, before] = bar.dataset.tip.split('|');
    tip.innerHTML =
      `<b><i style="background:${color}"></i>${esc(name)}</b><div>${esc(value)}</div>` +
      (rank ? `<div>${esc(rank)}</div>` : '') +
      (before ? `<div>${esc(before)}</div>` : '');
    tip.style.display = 'block';
    const x = Math.min(ev.pageX + 14, document.documentElement.clientWidth + window.scrollX - tip.offsetWidth - 8);
    tip.style.left = `${Math.max(8, x)}px`;
    tip.style.top = `${ev.pageY + 16}px`;
  });
  root.addEventListener('mouseleave', () => {
    tip.style.display = 'none';
  });
}
