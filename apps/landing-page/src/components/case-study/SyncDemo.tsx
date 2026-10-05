import type React from 'react';
import { useCallback, useEffect, useReducer, useRef, useState } from 'react';

/**
 * One analysis board, two devices. Alice works through a line on WhitePawn's
 * analysis screen, a move on her laptop, the next on her phone, and both read
 * the same `analysis_board` row in SurrealDB.
 *
 * The devices only ever talk to SurrealDB. sp00ky's sync engine runs behind the
 * database: it takes in the change, works out which live queries the row
 * feeds, and writes that back, so SurrealDB can push the row to the other
 * device. Every move takes four steps, shown under the devices as it happens:
 *   1. local write  - the device's board updates from its own local store
 *   2. SurrealDB    - the write goes up and is stored in the row
 *   3. sp00ky       - the engine behind the database routes the change
 *   4. live query   - the other device receives the row and re-renders
 * Taking the phone offline parks a move at step 2 (the phone's own write waits
 * in its outbox) or step 4 (a laptop move waits for the phone), and
 * reconnecting resumes it.
 *
 * Nothing here talks to a server. It is a re-enactment of the flow, timed
 * slower than the real thing so the eye can follow it.
 */

type Device = 'laptop' | 'phone';
type Board = Record<string, string>;
type Stage = 'local' | 'upload' | 'stored' | 'ingest' | 'route' | 'publish' | 'deliver' | 'done';

interface Move {
  from: string;
  to: string;
  san: string;
}

// Italian Game, Giuoco Piano. No castling or promotion, so a from/to pair is enough.
const MOVES: Move[] = [
  { from: 'e2', to: 'e4', san: 'e4' },
  { from: 'e7', to: 'e5', san: 'e5' },
  { from: 'g1', to: 'f3', san: 'Nf3' },
  { from: 'b8', to: 'c6', san: 'Nc6' },
  { from: 'f1', to: 'c4', san: 'Bc4' },
  { from: 'f8', to: 'c5', san: 'Bc5' },
  { from: 'c2', to: 'c3', san: 'c3' },
  { from: 'g8', to: 'f6', san: 'Nf6' },
  { from: 'd2', to: 'd4', san: 'd4' },
  { from: 'e5', to: 'd4', san: 'exd4' },
  { from: 'c3', to: 'd4', san: 'cxd4' },
  { from: 'c5', to: 'b4', san: 'Bb4+' },
  { from: 'c1', to: 'd2', san: 'Bd2' },
  { from: 'b4', to: 'd2', san: 'Bxd2+' },
  { from: 'b1', to: 'd2', san: 'Nbxd2' },
  { from: 'd7', to: 'd5', san: 'd5' },
];

const FILES = 'abcdefgh';
const BACK_RANK = 'RNBQKBNR';
const GLYPH: Record<string, string> = { K: '♚', Q: '♛', R: '♜', B: '♝', N: '♞', P: '♟' };

function boardAfter(count: number): Board {
  const b: Board = {};
  for (let i = 0; i < 8; i++) {
    const f = FILES[i];
    b[`${f}1`] = `w${BACK_RANK[i]}`;
    b[`${f}2`] = 'wP';
    b[`${f}7`] = 'bP';
    b[`${f}8`] = `b${BACK_RANK[i]}`;
  }
  for (const m of MOVES.slice(0, count)) {
    b[m.to] = b[m.from];
    delete b[m.from];
  }
  return b;
}

/** Moves alternate: even plies are added on the laptop, odd plies on the phone. */
const owner = (ply: number): Device => (ply % 2 === 0 ? 'laptop' : 'phone');
const other = (d: Device): Device => (d === 'laptop' ? 'phone' : 'laptop');
const NAME: Record<Device, string> = { laptop: 'laptop', phone: 'phone' };

interface Flight {
  ply: number;
  from: Device;
  stage: Stage;
  /** Why the move is parked: the phone's own write is offline, or the phone cannot receive. */
  wait: 'outbox' | 'offline' | null;
  doneAt: number;
}

interface State {
  /** Plies stored in the row. */
  server: number;
  /** Plies each board shows. */
  seen: Record<Device, number>;
  phoneOnline: boolean;
  flight: Flight | null;
  /** Bumps on every store so the row can flash. */
  writes: number;
}

const initialState = (): State => ({
  server: 0,
  seen: { laptop: 0, phone: 0 },
  phoneOnline: true,
  flight: null,
  writes: 0,
});

// Slower than reality on purpose, so each step can be read.
const STEP_MS = 700;
const HOP_MS = 550;
const TURN_GAP_MS = 1200;

export default function SyncDemo() {
  const state = useRef<State>(initialState());
  const [, render] = useReducer((n: number) => n + 1, 0);
  const timers = useRef<number[]>([]);
  const root = useRef<HTMLDivElement>(null);
  // Autoplays while on screen; the plug on the phone's wire is the only control.
  const [playing, setPlaying] = useState(false);

  const commit = useCallback((mutate: (s: State) => void) => {
    mutate(state.current);
    render();
  }, []);
  const later = useCallback((ms: number, fn: () => void) => {
    timers.current.push(window.setTimeout(fn, ms));
  }, []);
  const setStage = useCallback(
    (stage: Stage, wait: Flight['wait'] = null) =>
      commit((s) => {
        if (!s.flight) return;
        s.flight = { ...s.flight, stage, wait, doneAt: stage === 'done' ? Date.now() : 0 };
      }),
    [commit],
  );

  const deliver = useCallback(() => {
    const f = state.current.flight;
    if (!f) return;
    const to = other(f.from);
    if (to === 'phone' && !state.current.phoneOnline) {
      setStage('deliver', 'offline');
      return;
    }
    setStage('deliver');
    later(HOP_MS, () => {
      commit((st) => {
        st.seen[to] = st.server;
      });
      setStage('done');
    });
  }, [commit, later, setStage]);

  // Step 3: the engine behind the database takes the change in, routes it and
  // writes the result back to SurrealDB.
  const route = useCallback(() => {
    setStage('ingest');
    later(HOP_MS, () => {
      setStage('route');
      later(STEP_MS, () => {
        setStage('publish');
        later(HOP_MS, deliver);
      });
    });
  }, [deliver, later, setStage]);

  // Step 2: upload, and SurrealDB stores the row.
  const upload = useCallback(() => {
    setStage('upload');
    later(HOP_MS, () => {
      commit((s) => {
        if (!s.flight) return;
        s.server = Math.max(s.server, s.flight.ply + 1);
        s.writes += 1;
      });
      setStage('stored');
      later(STEP_MS, route);
    });
  }, [commit, later, route, setStage]);

  /** The device that adds the next move, if its board is caught up and nothing is in flight. */
  const nextMover = (): Device | null => {
    const s = state.current;
    if (s.flight && s.flight.stage !== 'done') return null;
    for (const d of ['laptop', 'phone'] as Device[]) {
      const ply = s.seen[d];
      if (ply < MOVES.length && owner(ply) === d) return d;
    }
    return null;
  };

  // Step 1: the local write.
  const play = useCallback(() => {
    const device = nextMover();
    if (!device) return;
    const ply = state.current.seen[device];
    commit((s) => {
      s.seen[device] = ply + 1;
      s.flight = { ply, from: device, stage: 'local', wait: null, doneAt: 0 };
    });
    later(STEP_MS, () => {
      if (device === 'phone' && !state.current.phoneOnline) {
        setStage('upload', 'outbox');
        return;
      }
      upload();
    });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [commit, later, setStage, upload]);

  const setPhoneOnline = (online: boolean) => {
    commit((s) => {
      s.phoneOnline = online;
    });
    const f = state.current.flight;
    if (!online || !f) return;
    if (f.wait === 'outbox') upload();
    else if (f.wait === 'offline') deliver();
  };

  const reset = useCallback(() => {
    timers.current.forEach(clearTimeout);
    timers.current = [];
    state.current = initialState();
    render();
  }, []);

  useEffect(() => {
    const el = root.current;
    if (!el) return;
    const io = new IntersectionObserver(([entry]) => setPlaying(entry.isIntersecting), { threshold: 0.25 });
    io.observe(el);
    return () => io.disconnect();
  }, []);

  // Autoplay: the next move a beat after the last one settled; restart a finished line.
  useEffect(() => {
    if (!playing) return;
    const id = window.setInterval(() => {
      const s = state.current;
      const f = s.flight;
      if (f && (f.stage !== 'done' || Date.now() - f.doneAt < TURN_GAP_MS)) return;
      if (s.seen.laptop === MOVES.length && s.seen.phone === MOVES.length) {
        if (f && Date.now() - f.doneAt > TURN_GAP_MS * 2.5) reset();
        return;
      }
      play();
    }, 250);
    return () => window.clearInterval(id);
  }, [playing, play, reset]);

  useEffect(() => () => timers.current.forEach(clearTimeout), []);

  const s = state.current;
  const f = s.flight;
  const stored = MOVES.slice(0, s.server);
  const stamp = f ? `${f.ply}-${f.stage}` : '';
  const moving = f && !f.wait ? f.stage : null;
  const sideOf = (d: Device) => (d === 'laptop' ? 'left' : 'right');

  const plug = (
    <button
      type="button"
      className={`sd-plug${s.phoneOnline ? '' : ' is-off'}`}
      onClick={() => setPhoneOnline(!s.phoneOnline)}
      aria-pressed={!s.phoneOnline}
      aria-label={s.phoneOnline ? 'Disconnect the phone' : 'Reconnect the phone'}
      title={s.phoneOnline ? 'Disconnect the phone' : 'Reconnect the phone'}
    >
      {s.phoneOnline ? (
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
          <path d="M9 17H7A5 5 0 0 1 7 7h2" />
          <path d="M15 7h2a5 5 0 1 1 0 10h-2" />
          <line x1="8" x2="16" y1="12" y2="12" />
        </svg>
      ) : (
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
          <path d="M9 17H7A5 5 0 0 1 7 7" />
          <path d="M15 7h2a5 5 0 0 1 4 8" />
          <line x1="8" x2="12" y1="12" y2="12" />
          <line x1="2" x2="22" y1="2" y2="22" />
        </svg>
      )}
      <span className="sd-plug-label">{s.phoneOnline ? 'disconnect' : 'reconnect'}</span>
    </button>
  );

  // Which wire carries a dot right now, and which way it travels.
  const dot = (wire: 'left' | 'right' | 'down'): 'toward' | 'away' | null => {
    if (!f || !moving) return null;
    if (moving === 'upload' && sideOf(f.from) === wire) return 'toward';
    if (moving === 'deliver' && sideOf(other(f.from)) === wire) return 'away';
    if (wire === 'down' && moving === 'ingest') return 'away';
    if (wire === 'down' && moving === 'publish') return 'toward';
    return null;
  };

  const dbActive = moving === 'stored' || moving === 'ingest' || moving === 'publish';
  const engineActive = moving === 'ingest' || moving === 'route' || moving === 'publish';

  return (
    <div className="sd" ref={root}>
      <div className="sd-stage">
        <DeviceFrame kind="laptop" ply={s.seen.laptop} status={statusFor('laptop', s)} />
        <div className="sd-middle">
        <Wire wire="left" dir={dot('left')} stamp={stamp} />

        <div className="sd-stack">
        <div className={`sd-db${dbActive ? ' is-active' : ''}${moving === 'stored' ? ' is-write' : ''}`}>
          <div className="sd-card-head">
            <img src="/surrealdb-logo.png" alt="" width="14" height="16" />
            <span>SurrealDB</span>
          </div>
          <div className="sd-row" key={s.writes}>
            <span className="sd-rid">analysis_board:AB_x7Kq</span>
            <dl>
              <dt>moves</dt>
              <dd>
                {stored.length === 0 ? (
                  <span className="sd-muted">empty</span>
                ) : (
                  <>
                    {stored.length > 3 && <span className="sd-muted">… </span>}
                    {stored.slice(-3).map((m, i, arr) => (
                      <span key={m.san + i} className={i === arr.length - 1 ? 'sd-newest' : ''}>
                        {m.san}
                        {i < arr.length - 1 ? ' ' : ''}
                      </span>
                    ))}
                  </>
                )}
              </dd>
              <dt>ply</dt>
              <dd>{s.server}</dd>
            </dl>
          </div>
        </div>

        <div className="sd-behind">
          <Wire wire="down" dir={dot('down')} stamp={stamp} />
          <div className={`sd-engine${engineActive ? ' is-active' : ''}`}>
            <img src="/footer-mark-00.svg" alt="" width="20" height="13" />
            <span>
              <strong>sp00ky sync engine</strong>
              <em>{engineText(f)}</em>
            </span>
          </div>
          <span className="sd-behind-label">runs behind your database</span>
        </div>
        </div>

        <Wire wire="right" dir={dot('right')} stamp={stamp} dim={!s.phoneOnline}>
          {plug}
        </Wire>
        </div>

        <DeviceFrame
          kind="phone"
          ply={s.seen.phone}
          status={statusFor('phone', s)}
          offline={!s.phoneOnline}
          outbox={f?.wait === 'outbox'}
          plug={plug}
        />
      </div>

      <ol className="sd-steps" aria-label="What happens to the move">
        {stepsFor(f).map((step, i) => (
          <li key={step.title} className={`is-${step.state}`}>
            <span className="sd-step-n">{i + 1}</span>
            <span className="sd-step-body">
              <strong>{step.title}</strong>
              <span>{step.text}</span>
            </span>
          </li>
        ))}
      </ol>

    </div>
  );
}

type StepState = 'idle' | 'active' | 'done' | 'wait';

function stepsFor(f: Flight | null): { title: string; text: string; state: StepState }[] {
  const from = f ? NAME[f.from] : 'device';
  const to = f ? NAME[other(f.from)] : 'other device';
  const san = f ? MOVES[f.ply].san : 'the move';
  // Stage -> step: upload and stored are step 2, ingest/route/publish step 3.
  const STEP: Record<Stage, number> = {
    local: 0,
    upload: 1,
    stored: 1,
    ingest: 2,
    route: 2,
    publish: 2,
    deliver: 3,
    done: 4,
  };
  const current = f ? STEP[f.stage] : -1;
  const stateOf = (i: number): StepState => {
    if (current < 0) return 'idle';
    if (i < current || current === 4) return 'done';
    if (i > current) return 'idle';
    return f?.wait ? 'wait' : 'active';
  };
  return [
    {
      title: 'Local write',
      text: `The ${from} shows ${san} at once, from its local store.`,
      state: stateOf(0),
    },
    {
      title: 'SurrealDB',
      text:
        f?.wait === 'outbox'
          ? 'No signal: the write waits in the phone’s outbox.'
          : 'The write is stored in the analysis_board row.',
      state: stateOf(1),
    },
    {
      title: 'sp00ky, behind it',
      text: `Finds the screens this row feeds, here the ${to}, and hands them back to SurrealDB.`,
      state: stateOf(2),
    },
    {
      title: 'Live query',
      text:
        f?.wait === 'offline'
          ? 'The phone is offline. It catches up when it reconnects.'
          : `SurrealDB pushes the row and the ${to} shows ${san}.`,
      state: stateOf(3),
    },
  ];
}

function engineText(f: Flight | null): string {
  if (!f) return 'idle';
  if (f.stage === 'ingest') return 'reading the change';
  if (f.stage === 'route') return 'matching live queries';
  if (f.stage === 'publish') return `routing it to the ${NAME[other(f.from)]}`;
  return 'idle';
}

function statusFor(device: Device, s: State): string {
  if (device === 'phone' && !s.phoneOnline) return 'Offline';
  const f = s.flight;
  if (f && f.from === device && f.stage !== 'done' && !(f.stage === 'deliver')) return 'Saving';
  if (s.seen[device] < s.server || (f && other(f.from) === device && f.stage !== 'done')) return 'Updating';
  return 'Up to date';
}

function Wire({
  wire,
  dir,
  stamp,
  dim = false,
  children,
}: {
  wire: 'left' | 'right' | 'down';
  dir: 'toward' | 'away' | null;
  stamp: string;
  dim?: boolean;
  children?: React.ReactNode;
}) {
  // "toward" travels to the database card, "away" from it.
  const cls =
    dir === null
      ? ''
      : wire === 'left'
        ? dir === 'toward'
          ? 'fwd'
          : 'rev'
        : wire === 'right'
          ? dir === 'toward'
            ? 'rev'
            : 'fwd'
          : dir === 'away'
            ? 'down'
            : 'up';
  return (
    <div className={`sd-wire sd-wire-${wire}${dim ? ' is-dim' : ''}`}>
      {dir && <span key={stamp} className={`sd-dot ${cls}`} aria-hidden="true" />}
      {children}
    </div>
  );
}

function DeviceFrame({
  kind,
  ply,
  status,
  offline = false,
  outbox = false,
  plug,
}: {
  kind: Device;
  ply: number;
  status: string;
  offline?: boolean;
  outbox?: boolean;
  /** Phones only: the connection plug, shown here when the wires are hidden. */
  plug?: React.ReactNode;
}) {
  const board = boardAfter(ply);
  const last = ply > 0 ? MOVES[ply - 1] : null;
  const shown = MOVES.slice(0, ply);
  const tone = status === 'Offline' ? 'off' : status === 'Up to date' ? 'ok' : 'busy';
  const bar = (
    <div className="sd-bar">
      <span className="sd-title">Analysis</span>
      <span className={`sd-status is-${tone}`}>{status}</span>
    </div>
  );
  const grid = (
    <div
      className="sd-board"
      role="img"
      aria-label={`The ${kind}'s board after ${ply} moves${last ? `, last move ${last.san}` : ''}`}
    >
      {[8, 7, 6, 5, 4, 3, 2, 1].map((r) =>
        [...FILES].map((f) => {
          const sq = `${f}${r}`;
          const piece = board[sq];
          // a1 is a dark square: file index 0 + rank 1 is odd.
          const dark = (FILES.indexOf(f) + r) % 2 === 1;
          const hl = last && (last.from === sq || last.to === sq);
          return (
            <span key={sq} className={`sd-sq${dark ? ' dark' : ''}${hl ? ' hl' : ''}`}>
              {piece && (
                <span className={`sd-pc ${piece[0] === 'w' ? 'white' : 'black'}`}>
                  {GLYPH[piece[1]]}
                  {'\uFE0E'}
                </span>
              )}
            </span>
          );
        }),
      )}
    </div>
  );

  // The laptop is landscape: board left, the analysis panel with the line right.
  // The phone is portrait: board on top, the newest moves under it.
  const pairs: { n: number; w: string; b?: string }[] = [];
  for (let i = 0; i < shown.length; i += 2) pairs.push({ n: i / 2 + 1, w: shown[i].san, b: shown[i + 1]?.san });

  return (
    <figure className={`sd-device sd-${kind}${offline ? ' is-offline' : ''}`}>
      <div className="sd-screen">
        {kind === 'laptop' ? (
          <>
            {grid}
            <div className="sd-side">
              {bar}
              <ol className="sd-moves" aria-hidden="true">
                {pairs.length === 0 && <li className="sd-moves-empty">Start position</li>}
                {pairs.slice(-6).map((p) => (
                  <li key={p.n}>
                    <span className="n">{p.n}.</span>
                    <span className={!p.b ? 'is-new' : ''}>{p.w}</span>
                    {p.b && <span className="is-new">{p.b}</span>}
                  </li>
                ))}
              </ol>
            </div>
          </>
        ) : (
          <>
            {bar}
            {grid}
            <div className="sd-line" aria-hidden="true">
              {shown.length === 0 ? (
                <span className="sd-line-empty">Start position</span>
              ) : (
                shown.slice(-3).map((m, i, arr) => (
                  <span key={`${ply}-${i}`} className={i === arr.length - 1 ? 'is-new' : ''}>
                    {m.san}
                  </span>
                ))
              )}
            </div>
          </>
        )}
        {outbox && <span className="sd-outbox">1 write in outbox</span>}
      </div>
      {kind === 'laptop' && <div className="sd-laptop-base" aria-hidden="true" />}
      <figcaption>
        Alice&rsquo;s {kind}
        {plug && <span className="sd-plug-mobile">{plug}</span>}
      </figcaption>
    </figure>
  );
}
