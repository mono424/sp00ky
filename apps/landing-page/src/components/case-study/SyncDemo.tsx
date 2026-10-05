import { useCallback, useEffect, useReducer, useRef, useState } from 'react';

/**
 * Two players, two platforms, one row. Alice plays white in the web app, Bob
 * plays black on his phone, and both boards read the same `game` row.
 *
 * Every move takes the same four steps, shown under the devices as it happens:
 *   1. local write  - the mover's board updates from its own local store
 *   2. upload       - the write goes up and SurrealDB commits it to the row
 *   3. sync engine  - sp00ky finds the live queries that row feeds
 *   4. live query   - the other device receives the row and re-renders
 * Taking Bob's phone offline stops a move at step 2 (his write waits in the
 * outbox) or step 4 (Alice's move waits for him), and reconnecting resumes it.
 *
 * Nothing here talks to a server. It is a re-enactment of the flow, timed
 * slower than the real thing so the eye can follow it.
 */

type Device = 'web' | 'phone';
type Board = Record<string, string>;
type Stage = 'local' | 'upload' | 'commit' | 'route' | 'deliver' | 'done';

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

/** White (even plies) is played on the web, black (odd plies) on the phone. */
const owner = (ply: number): Device => (ply % 2 === 0 ? 'web' : 'phone');
const other = (d: Device): Device => (d === 'web' ? 'phone' : 'web');
const PLAYER: Record<Device, string> = { web: 'Alice', phone: 'Bob' };

interface Flight {
  ply: number;
  from: Device;
  stage: Stage;
  /** Why the move is parked: Bob's own write is offline, or Bob cannot receive. */
  wait: 'outbox' | 'offline' | null;
  doneAt: number;
}

interface State {
  /** Plies committed to the row in SurrealDB. */
  server: number;
  /** Plies each board shows. */
  seen: Record<Device, number>;
  phoneOnline: boolean;
  flight: Flight | null;
  /** Bumps on every commit so the row can flash. */
  commits: number;
}

const initialState = (): State => ({
  server: 0,
  seen: { web: 0, phone: 0 },
  phoneOnline: true,
  flight: null,
  commits: 0,
});

// Slower than reality on purpose, so each step can be read.
const STEP_MS = 750;
const HOP_MS = 600;
const TURN_GAP_MS = 1100;

export default function SyncDemo() {
  const state = useRef<State>(initialState());
  const [, render] = useReducer((n: number) => n + 1, 0);
  const timers = useRef<number[]>([]);
  const [playing, setPlaying] = useState(false);
  const [reducedMotion, setReducedMotion] = useState(false);

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

  // Steps 3 and 4: the row is committed; route it and deliver it.
  const routeAndDeliver = useCallback(() => {
    setStage('route');
    later(STEP_MS, () => {
      const s = state.current;
      const f = s.flight;
      if (!f) return;
      const to = other(f.from);
      if (to === 'phone' && !s.phoneOnline) {
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
    });
  }, [commit, later, setStage]);

  // Step 2: upload, then SurrealDB commits.
  const upload = useCallback(() => {
    setStage('upload');
    later(HOP_MS, () => {
      commit((s) => {
        if (!s.flight) return;
        s.server = Math.max(s.server, s.flight.ply + 1);
        s.commits += 1;
      });
      setStage('commit');
      later(STEP_MS, routeAndDeliver);
    });
  }, [commit, later, routeAndDeliver, setStage]);

  /** The device whose turn it is and whose board is caught up, if no move is in flight. */
  const nextMover = (): Device | null => {
    const s = state.current;
    if (s.flight && s.flight.stage !== 'done') return null;
    for (const d of ['web', 'phone'] as Device[]) {
      const ply = s.seen[d];
      if (ply < MOVES.length && owner(ply) === d) return d;
    }
    return null;
  };

  // Step 1: the mover's local write.
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
    else if (f.wait === 'offline') {
      setStage('deliver');
      later(HOP_MS, () => {
        commit((st) => {
          st.seen.phone = st.server;
        });
        setStage('done');
      });
    }
  };

  const reset = useCallback(() => {
    timers.current.forEach(clearTimeout);
    timers.current = [];
    state.current = initialState();
    render();
  }, []);

  useEffect(() => {
    const mq = window.matchMedia('(prefers-reduced-motion: reduce)');
    setReducedMotion(mq.matches);
    setPlaying(!mq.matches);
  }, []);

  // Autoplay: the next move a beat after the last one settled; restart a finished game.
  useEffect(() => {
    if (!playing) return;
    const id = window.setInterval(() => {
      const s = state.current;
      const f = s.flight;
      if (f && (f.stage !== 'done' || Date.now() - f.doneAt < TURN_GAP_MS)) return;
      if (s.seen.web === MOVES.length && s.seen.phone === MOVES.length) {
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
  const mover = nextMover();
  const committed = MOVES.slice(0, s.server);

  // Which wire carries a dot right now, and which way.
  const leg: { side: 'left' | 'right'; dir: 'in' | 'out' } | null =
    f && !f.wait
      ? f.stage === 'upload'
        ? { side: f.from === 'web' ? 'left' : 'right', dir: 'in' }
        : f.stage === 'deliver'
          ? { side: other(f.from) === 'web' ? 'left' : 'right', dir: 'out' }
          : null
      : null;

  return (
    <div className="sd">
      <div className="sd-stage">
        <DeviceFrame kind="web" ply={s.seen.web} status={statusFor('web', s, mover)} />
        <Wire side="left" leg={leg} stamp={f ? `${f.ply}-${f.stage}` : ''} />
        <div className={`sd-db${f?.stage === 'commit' ? ' is-commit' : ''}${f?.stage === 'route' ? ' is-route' : ''}`}>
          <div className="sd-db-head">
            <img src="/surrealdb-logo.png" alt="" width="14" height="16" />
            SurrealDB
          </div>
          <div className="sd-row" key={s.commits}>
            <span className="sd-rid">game:alice_bob</span>
            <dl>
              <dt>moves</dt>
              <dd>
                {committed.length === 0 ? (
                  <span className="sd-muted">none yet</span>
                ) : (
                  <>
                    {committed.length > 3 && <span className="sd-muted">{committed.length - 3} more, </span>}
                    {committed.slice(-3).map((m, i, arr) => (
                      <span key={m.san + i} className={i === arr.length - 1 ? 'sd-newest' : ''}>
                        {m.san}
                        {i < arr.length - 1 ? ' ' : ''}
                      </span>
                    ))}
                  </>
                )}
              </dd>
              <dt>to move</dt>
              <dd>{s.server >= MOVES.length ? 'game saved' : owner(s.server) === 'web' ? 'white' : 'black'}</dd>
            </dl>
          </div>
          <div className="sd-engine">
            <img src="/footer-mark-00.svg" alt="" width="18" height="12" />
            <span>
              <strong>sp00ky sync engine</strong>
              <em>{engineText(f)}</em>
            </span>
          </div>
        </div>
        <Wire side="right" leg={leg} stamp={f ? `${f.ply}-${f.stage}` : ''} dim={!s.phoneOnline} />
        <DeviceFrame
          kind="phone"
          ply={s.seen.phone}
          status={statusFor('phone', s, mover)}
          offline={!s.phoneOnline}
          outbox={f?.wait === 'outbox' ? 1 : 0}
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

      <div className="sd-controls">
        <button type="button" className="sd-btn" onClick={() => setPlaying((p) => !p)} aria-pressed={playing}>
          {playing ? 'Pause' : 'Play'}
        </button>
        <button type="button" className="sd-btn" onClick={() => play()} disabled={!mover}>
          Next move
        </button>
        <button
          type="button"
          className={`sd-btn sd-btn-signal${s.phoneOnline ? '' : ' is-off'}`}
          onClick={() => setPhoneOnline(!s.phoneOnline)}
          aria-pressed={!s.phoneOnline}
        >
          {s.phoneOnline ? "Take Bob's phone offline" : "Bring Bob's phone back online"}
        </button>
        <button type="button" className="sd-btn sd-btn-quiet" onClick={reset}>
          Restart
        </button>
        {reducedMotion && !playing && (
          <span className="sd-hint">Autoplay is off because your system asks for reduced motion.</span>
        )}
      </div>
    </div>
  );
}

type StepState = 'idle' | 'active' | 'done' | 'wait';

function stepsFor(f: Flight | null): { title: string; text: string; state: StepState }[] {
  const from = f ? PLAYER[f.from] : 'The mover';
  const to = f ? PLAYER[other(f.from)] : 'the other player';
  const san = f ? MOVES[f.ply].san : 'a move';
  const order: Stage[] = ['local', 'upload', 'commit', 'route', 'deliver', 'done'];
  const at = f ? order.indexOf(f.stage) : -1;
  // Stage index -> step index: upload and commit are both step 2.
  const stepOf = (stage: number) => [0, 1, 1, 2, 3, 4][stage];
  const current = at < 0 ? -1 : stepOf(at);
  const stateOf = (i: number): StepState => {
    if (current < 0) return 'idle';
    if (i < current || current === 4) return 'done';
    if (i > current) return 'idle';
    return f?.wait ? 'wait' : 'active';
  };
  return [
    {
      title: 'Local write',
      text: `${from}'s board shows ${san} at once, from the local store.`,
      state: stateOf(0),
    },
    {
      title: 'Upload',
      text:
        f?.wait === 'outbox'
          ? `No signal: the write waits in ${from}'s outbox.`
          : at >= 2
            ? 'SurrealDB committed it to the game row.'
            : 'The write goes up to SurrealDB.',
      state: stateOf(1),
    },
    {
      title: 'Sync engine',
      text: `sp00ky finds the live queries this row feeds: ${to}'s game screen.`,
      state: stateOf(2),
    },
    {
      title: 'Live query',
      text:
        f?.wait === 'offline'
          ? `${to}'s phone is offline. It catches up when it reconnects.`
          : `${f ? to : 'The other player'}'s board re-renders with ${san}.`,
      state: stateOf(3),
    },
  ];
}

function engineText(f: Flight | null): string {
  if (!f) return 'waiting for a change';
  const to = PLAYER[other(f.from)];
  if (f.stage === 'route') return 'matching live queries';
  if (f.stage === 'deliver') return f.wait ? `${to} offline, holding` : `pushing to ${to}`;
  if (f.stage === 'done') return `${to} is up to date`;
  return 'waiting for a change';
}

function statusFor(device: Device, s: State, mover: Device | null): string {
  if (s.seen[device] >= MOVES.length && s.server >= MOVES.length) return 'Game saved';
  if (device === 'phone' && !s.phoneOnline) return 'Offline';
  if (mover === device) return 'Your move';
  return `${PLAYER[other(device)]}'s move`;
}

function Wire({
  side,
  leg,
  stamp,
  dim = false,
}: {
  side: 'left' | 'right';
  leg: { side: 'left' | 'right'; dir: 'in' | 'out' } | null;
  stamp: string;
  dim?: boolean;
}) {
  // "in" travels toward the database in the middle, "out" away from it.
  const active = leg && leg.side === side ? leg : null;
  const forward = active ? (side === 'left' ? active.dir === 'in' : active.dir === 'out') : false;
  return (
    <div className={`sd-wire${dim ? ' is-dim' : ''}`} aria-hidden="true">
      {active && <span key={stamp} className={`sd-dot ${forward ? 'fwd' : 'rev'}`} />}
    </div>
  );
}

function DeviceFrame({
  kind,
  ply,
  status,
  offline = false,
  outbox = 0,
}: {
  kind: Device;
  ply: number;
  status: string;
  offline?: boolean;
  outbox?: number;
}) {
  const board = boardAfter(ply);
  const last = ply > 0 ? MOVES[ply - 1] : null;
  const flipped = kind === 'phone';
  const ranks = flipped ? [1, 2, 3, 4, 5, 6, 7, 8] : [8, 7, 6, 5, 4, 3, 2, 1];
  const files = flipped ? [...FILES].reverse() : [...FILES];
  const pairs: string[] = [];
  for (let i = 0; i < ply; i += 2) {
    pairs.push(`${i / 2 + 1}. ${MOVES[i].san}${MOVES[i + 1] && i + 1 < ply ? ` ${MOVES[i + 1].san}` : ''}`);
  }

  return (
    <figure className={`sd-device sd-${kind}${offline ? ' is-offline' : ''}`}>
      <div className="sd-screen">
        <div className="sd-bar">
          <span className="sd-players">{kind === 'web' ? 'Alice vs Bob' : 'Bob vs Alice'}</span>
          <span className={`sd-status${offline ? ' is-off' : ''}`}>{status}</span>
        </div>
        <div className="sd-body">
          <div
            className="sd-board"
            role="img"
            aria-label={`${kind === 'web' ? "Alice's" : "Bob's"} board after ${ply} moves${last ? `, last move ${last.san}` : ''}`}
          >
            {ranks.map((r) =>
              files.map((f) => {
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
                        {'︎'}
                      </span>
                    )}
                  </span>
                );
              }),
            )}
          </div>
          {kind === 'web' && (
            <ol className="sd-movelist" aria-hidden="true">
              {pairs.slice(-6).map((p) => (
                <li key={p}>{p}</li>
              ))}
            </ol>
          )}
        </div>
        {outbox > 0 && <span className="sd-outbox">{outbox} write in outbox</span>}
      </div>
      {kind === 'web' && <div className="sd-laptop-base" aria-hidden="true" />}
      <figcaption>{kind === 'web' ? 'Alice, web app' : 'Bob, iPhone app'}</figcaption>
    </figure>
  );
}
