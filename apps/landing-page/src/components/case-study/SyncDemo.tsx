import { useCallback, useEffect, useReducer, useRef, useState } from 'react';

/**
 * Two players, two platforms, one game row. Alice plays white in the web app,
 * Bob plays black on his phone. Every move is a local write that lands on the
 * mover's board instantly, goes up to SurrealDB, and comes back down to the
 * other device through its live query. Taking Bob offline shows the other half
 * of local-first: his move still lands on his board, waits in the outbox, and
 * flushes the moment he reconnects.
 *
 * Nothing here talks to a server. It is a faithful re-enactment of the flow,
 * timed slower than the real thing so the eye can follow it.
 */

type Device = 'web' | 'phone';
type Board = Record<string, string>;

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

function startBoard(): Board {
  const b: Board = {};
  for (let i = 0; i < 8; i++) {
    const f = FILES[i];
    b[`${f}1`] = `w${BACK_RANK[i]}`;
    b[`${f}2`] = 'wP';
    b[`${f}7`] = 'bP';
    b[`${f}8`] = `b${BACK_RANK[i]}`;
  }
  return b;
}

function boardAfter(count: number): Board {
  const b = startBoard();
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

type Leg = 'web-up' | 'db-phone' | 'phone-up' | 'db-web';

interface LogLine {
  id: number;
  who: string;
  what: string;
  tone: 'local' | 'db' | 'live' | 'queue';
}

interface State {
  /** Plies committed to the database. */
  server: number;
  /** Plies each device's live query has rendered. */
  seen: Record<Device, number>;
  /** Phone writes sitting in the outbox. */
  queued: number;
  phoneOnline: boolean;
  pulse: { id: number; leg: Leg } | null;
  log: LogLine[];
  seq: number;
}

const initialState = (): State => ({
  server: 0,
  seen: { web: 0, phone: 0 },
  queued: 0,
  phoneOnline: true,
  pulse: null,
  log: [],
  seq: 0,
});

// Slower than reality on purpose, so the round trip is visible.
const HOP_MS = 480;
const TURN_MS = 1900;

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

  const log = (s: State, who: string, what: string, tone: LogLine['tone']) => {
    s.seq += 1;
    s.log = [...s.log, { id: s.seq, who, what, tone }].slice(-5);
  };

  const pulse = (leg: Leg) =>
    commit((s) => {
      s.seq += 1;
      s.pulse = { id: s.seq, leg };
    });

  const deliver = useCallback(
    (device: Device) => {
      commit((s) => {
        if (s.server <= s.seen[device]) return;
        const newest = MOVES[s.server - 1];
        s.seen[device] = s.server;
        log(s, `${PLAYER[device]}'s ${device === 'web' ? 'browser' : 'phone'}`, `live query re-rendered: ${newest.san}`, 'live');
      });
    },
    [commit],
  );

  const upload = useCallback(
    (from: Device, upTo: number, flushed = 0) => {
      pulse(from === 'web' ? 'web-up' : 'phone-up');
      later(HOP_MS, () => {
        commit((s) => {
          s.server = Math.max(s.server, upTo);
          log(
            s,
            'SurrealDB',
            flushed > 0 ? `committed ${flushed} flushed write${flushed > 1 ? 's' : ''}` : 'committed, SSP notified subscribers',
            'db',
          );
        });
        const to = other(from);
        if (to === 'phone' && !state.current.phoneOnline) return;
        pulse(to === 'phone' ? 'db-phone' : 'db-web');
        later(HOP_MS, () => deliver(to));
      });
    },
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [commit, deliver, later],
  );

  /** The device whose turn it is and whose board is caught up, if any. */
  const nextMover = (): Device | null => {
    const s = state.current;
    for (const d of ['web', 'phone'] as Device[]) {
      const ply = s.seen[d];
      if (ply < MOVES.length && owner(ply) === d) return d;
    }
    return null;
  };

  const play = useCallback(() => {
    const device = nextMover();
    if (!device) return false;
    const ply = state.current.seen[device];
    const move = MOVES[ply];
    const offline = device === 'phone' && !state.current.phoneOnline;
    commit((s) => {
      s.seen[device] = ply + 1;
      log(s, PLAYER[device], `plays ${move.san}, on screen in 1 ms`, 'local');
      if (offline) {
        s.queued += 1;
        log(s, 'Outbox', `${s.queued} write${s.queued > 1 ? 's' : ''} waiting for a connection`, 'queue');
      }
    });
    if (!offline) upload(device, ply + 1);
    return true;
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [commit, upload]);

  const setPhoneOnline = (online: boolean) => {
    commit((s) => {
      s.phoneOnline = online;
      log(s, 'Bob', online ? 'back online' : 'loses signal', online ? 'live' : 'queue');
    });
    if (!online) return;
    const s = state.current;
    if (s.queued > 0) {
      const flushed = s.queued;
      commit((st) => {
        st.queued = 0;
      });
      upload('phone', s.seen.phone, flushed);
    } else if (s.server > s.seen.phone) {
      pulse('db-phone');
      later(HOP_MS, () => deliver('phone'));
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

  // Autoplay: one ply per turn; restart a finished, settled game after a beat.
  useEffect(() => {
    if (!playing) return;
    const id = window.setInterval(() => {
      const s = state.current;
      const settled = s.server === MOVES.length && s.seen.web === MOVES.length && s.seen.phone === MOVES.length;
      if (settled) {
        reset();
        return;
      }
      play();
    }, TURN_MS);
    return () => window.clearInterval(id);
  }, [playing, play, reset]);

  useEffect(() => () => timers.current.forEach(clearTimeout), []);

  const s = state.current;
  const mover = nextMover();
  const offlineHint = !s.phoneOnline ? offlineHintFor(s) : null;

  return (
    <div className="sd">
      <div className="sd-stage">
        <DeviceFrame
          kind="web"
          label="Alice, WhitePawn web (SolidJS)"
          ply={s.seen.web}
          status={statusFor('web', s, mover)}
        />
        <Wire legs={['web-up', 'db-web']} pulse={s.pulse} dim={false} />
        <div className="sd-db" aria-hidden="true">
          <span className="sd-db-mark">
            <img src="/footer-mark-00.svg" alt="" width="22" height="15" />
          </span>
          <span className="sd-db-label">SurrealDB + sp00ky</span>
          <span className="sd-db-count">{s.server} / {MOVES.length} plies</span>
        </div>
        <Wire legs={['db-phone', 'phone-up']} pulse={s.pulse} dim={!s.phoneOnline} />
        <DeviceFrame
          kind="phone"
          label="Bob, WhitePawn iOS (Flutter)"
          ply={s.seen.phone}
          status={statusFor('phone', s, mover)}
          offline={!s.phoneOnline}
          queued={s.queued}
        />
      </div>

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
          {s.phoneOnline ? "Take Bob's phone offline" : "Bring Bob's phone back"}
        </button>
        <button type="button" className="sd-btn sd-btn-quiet" onClick={reset}>
          Restart
        </button>
        {offlineHint && <span className="sd-hint">{offlineHint}</span>}
        {reducedMotion && !playing && <span className="sd-hint">Autoplay is off because your system asks for reduced motion.</span>}
      </div>

      <ol className="sd-log" aria-label="What sp00ky did">
        {s.log.length === 0 && <li className="sd-log-empty">Press Play or Next move to start the game.</li>}
        {s.log.map((l) => (
          <li key={l.id} className={`sd-log-line tone-${l.tone}`}>
            <span className="sd-log-who">{l.who}</span>
            <span className="sd-log-what">{l.what}</span>
          </li>
        ))}
      </ol>
    </div>
  );
}

function offlineHintFor(s: State): string {
  if (s.queued > 0) return "Bob's move is in his phone's outbox. Bring the phone back to flush it.";
  if (s.server > s.seen.phone) {
    return `Bob's phone missed Alice's ${MOVES[s.server - 1].san}. Bring it back and it catches up by itself.`;
  }
  return 'Bob is offline, but his next move still lands on his board at once.';
}

function statusFor(device: Device, s: State, mover: Device | null): string {
  if (s.seen[device] >= MOVES.length) return 'Game saved';
  if (device === 'phone' && !s.phoneOnline) {
    return s.queued > 0 ? `Offline, ${s.queued} move queued` : 'Offline, still playable';
  }
  if (mover === device) return 'Your move';
  return `Waiting for ${PLAYER[other(device)]}`;
}

function Wire({ legs, pulse, dim }: { legs: [Leg, Leg]; pulse: State['pulse']; dim: boolean }) {
  // legs[0] travels left-to-right (top-to-bottom on phones), legs[1] the other way.
  const active = pulse && legs.includes(pulse.leg) ? pulse : null;
  const forward = active?.leg === 'web-up' || active?.leg === 'db-phone';
  return (
    <div className={`sd-wire${dim ? ' is-dim' : ''}`} aria-hidden="true">
      {active && <span key={active.id} className={`sd-dot ${forward ? 'fwd' : 'rev'}`} />}
    </div>
  );
}

function DeviceFrame({
  kind,
  label,
  ply,
  status,
  offline = false,
  queued = 0,
}: {
  kind: Device;
  label: string;
  ply: number;
  status: string;
  offline?: boolean;
  queued?: number;
}) {
  const board = boardAfter(ply);
  const last = ply > 0 ? MOVES[ply - 1] : null;
  const flipped = kind === 'phone';
  const ranks = flipped ? [1, 2, 3, 4, 5, 6, 7, 8] : [8, 7, 6, 5, 4, 3, 2, 1];
  const files = flipped ? [...FILES].reverse() : [...FILES];

  return (
    <figure className={`sd-device sd-${kind}${offline ? ' is-offline' : ''}`}>
      <div className="sd-screen">
        <div className="sd-bar">
          <span className="sd-players">
            {kind === 'web' ? 'Alice vs Bob' : 'Bob vs Alice'}
          </span>
          <span className={`sd-status${queued > 0 ? ' is-queued' : ''}`}>{status}</span>
        </div>
        <div className="sd-board" role="img" aria-label={`Board after ${ply} plies${last ? `, last move ${last.san}` : ''}`}>
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
        <div className="sd-moves" aria-hidden="true">
          {MOVES.slice(0, ply)
            .slice(-4)
            .map((m, i, arr) => (
              <span key={`${ply}-${i}`} className={i === arr.length - 1 ? 'is-new' : ''}>
                {m.san}
              </span>
            ))}
        </div>
      </div>
      {kind === 'web' && <div className="sd-laptop-base" aria-hidden="true" />}
      <figcaption>{label}</figcaption>
    </figure>
  );
}
