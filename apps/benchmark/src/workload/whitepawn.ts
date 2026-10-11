import { Buffer } from 'node:buffer';
import { connectSurreal, type SurrealHandle } from '../stack/surrealdb.js';
import { withTimeout } from '../util/wait.js';

export type WhitepawnProfile = 'smoke' | 'standard' | 'large';
export interface WhitepawnCase {
  name: string;
  rows: number;
  pgnPayload: boolean;
}
export interface WhitepawnGame {
  id: string;
  owner: string;
  database: string;
  white: string;
  black: string;
  date: string;
  result: string;
  eco: string;
  source: string;
  avatar: string;
  sort_index: number;
  status: string;
  speed: string;
  pgn?: string;
}
export interface GameWindowOptions {
  filtered?: boolean;
  collection?: string;
}
export interface WhitepawnSeedCounts {
  games: number;
  collections: number;
  players: number;
  mainCount: number;
  /** UTF-8 JSON bytes of all synthetic record bodies, not database disk usage. */
  serializedBytes: number;
  /** UTF-8 bytes of the actual INSERT statements sent to SurrealDB. */
  sqlBytes: number;
  batches: number;
}

export const WHITEPAWN_PAGE = 50;
export const WHITEPAWN_BATCH = 500;
export const WHITEPAWN_PLAYERS = 256;
const BASE_SECONDS = Date.UTC(2026, 0, 1) / 1000;
const RESULTS = ['1-0', '0-1', '1/2-1/2'] as const;
const SPEEDS = ['blitz', 'rapid', 'bullet', 'classical'] as const;
const SOURCES = ['lichess', 'chesscom', 'pgn'] as const;

function integer(value: number, name: string, minimum = 0): void {
  if (!Number.isSafeInteger(value) || value < minimum) {
    throw new Error(`${name} must be a safe integer >= ${minimum}`);
  }
}

export function presetCases(profile: WhitepawnProfile): WhitepawnCase[] {
  const profiles: Record<WhitepawnProfile, WhitepawnCase[]> = {
    smoke: [
      { name: 'metadata-10000', rows: 10_000, pgnPayload: false },
      { name: 'pgn-10000', rows: 10_000, pgnPayload: true },
    ],
    standard: [
      { name: 'metadata-100000', rows: 100_000, pgnPayload: false },
      { name: 'metadata-1000000', rows: 1_000_000, pgnPayload: false },
      { name: 'pgn-1000000', rows: 1_000_000, pgnPayload: true },
    ],
    large: [
      { name: 'metadata-100000', rows: 100_000, pgnPayload: false },
      { name: 'metadata-1000000', rows: 1_000_000, pgnPayload: false },
      { name: 'pgn-1000000', rows: 1_000_000, pgnPayload: true },
      { name: 'metadata-10000000', rows: 10_000_000, pgnPayload: false },
    ],
  };
  const cases = profiles[profile];
  if (!cases) throw new Error(`Unknown WhitePawn profile: ${profile}`);
  return cases;
}

export function gameId(index: number): string {
  integer(index, 'index', 1);
  return `game:g${String(index).padStart(9, '0')}`;
}

function collectionFor(index: number): string {
  return index % 5 === 0
    ? `game_database:c${String(((Math.floor(index / 5) - 1) % 19) + 1).padStart(2, '0')}`
    : 'game_database:main';
}

function playerId(index: number): string {
  return `player_name:p${String(index % WHITEPAWN_PLAYERS).padStart(3, '0')}`;
}

function pgnFor(index: number): string {
  // Deterministic PGN-like text with per-row tags and varied comments. Avoid a
  // shared padding string, which would hide compression and serialization cost.
  const moves = [
    'e4 e5',
    'Nf3 Nc6',
    'Bb5 a6',
    'Ba4 Nf6',
    'O-O Be7',
    'Re1 b5',
    'Bb3 d6',
    'c3 O-O',
    'h3 Nb8',
    'd4 Nbd7',
    'Nbd2 Bb7',
    'Bc2 Re8',
  ];
  let text = `[Event "Synthetic library ${index}"]\n[Site "Benchmark"]\n[White "Player ${index % WHITEPAWN_PLAYERS}"]\n[Black "Player ${(index * 17 + 1) % WHITEPAWN_PLAYERS}"]\n[Result "${RESULTS[index % 3]}"]\n\n`;
  for (let move = 0; text.length < 1024 + (index % 257); move++) {
    const score = ((index * 97 + move * 31) % 1201) - 600;
    text += `${move + 1}. ${moves[(index + move) % moves.length]} {position ${index.toString(36)}_${move.toString(36)} eval ${score}} `;
  }
  return text + RESULTS[index % 3];
}

/** One-based ids; every fifth game belongs to one of nineteen small collections. */
export function gameRecord(index: number, rows: number, pgnPayload = false): WhitepawnGame {
  integer(index, 'index', 1);
  integer(rows, 'rows', 1);
  const database = collectionFor(index);
  // Four games share a timestamp, making the id tiebreaker observable.
  const seconds = BASE_SECONDS - Math.floor((index - 1) / 4);
  const record: WhitepawnGame = {
    id: gameId(index),
    owner: database === 'game_database:main' ? 'owner-main' : `owner-${database.split(':')[1]}`,
    database,
    white: playerId(index),
    black: playerId(index * 17 + 1),
    date: new Date(seconds * 1000).toISOString(),
    result: RESULTS[index % RESULTS.length],
    eco: `${String.fromCharCode(65 + (index % 5))}${String(index % 100).padStart(2, '0')}`,
    source: SOURCES[index % SOURCES.length],
    avatar: `https://example.invalid/avatar/${index % WHITEPAWN_PLAYERS}`,
    sort_index: -seconds,
    status: index % 97 === 0 ? 'Recording' : 'Finished',
    speed: SPEEDS[Math.floor((index - 1) / 3) % SPEEDS.length],
  };
  if (pgnPayload) record.pgn = pgnFor(index);
  return record;
}

function json(value: string): string {
  return JSON.stringify(value);
}

export function gameSql(record: WhitepawnGame): string {
  return `{ id: ${record.id}, owner: ${json(record.owner)}, database: ${record.database}, white: ${record.white}, black: ${record.black}, date: d${json(record.date)}, result: ${json(record.result)}, eco: ${json(record.eco)}, source: ${json(record.source)}, avatar: ${json(record.avatar)}, sort_index: ${record.sort_index}, status: ${json(record.status)}, speed: ${json(record.speed)}${record.pgn === undefined ? '' : `, pgn: ${json(record.pgn)}`} }`;
}

export function insertGamesSql(
  start: number,
  count: number,
  rows: number,
  pgnPayload = false
): { sql: string; serializedBytes: number; mainCount: number } {
  integer(start, 'start', 1);
  integer(count, 'count', 1);
  if (count > WHITEPAWN_BATCH)
    throw new Error(`A game batch cannot exceed ${WHITEPAWN_BATCH} rows`);
  const values: string[] = [];
  let serializedBytes = 0;
  let mainCount = 0;
  for (let index = start; index < start + count; index++) {
    const record = gameRecord(index, rows, pgnPayload);
    values.push(gameSql(record));
    serializedBytes += Buffer.byteLength(JSON.stringify(record));
    if (record.database === 'game_database:main') mainCount++;
  }
  return { sql: `INSERT INTO game [${values.join(',')}];`, serializedBytes, mainCount };
}

export async function seedWhitepawn(
  handle: SurrealHandle,
  rows: number,
  pgnPayload = false,
  onProgress?: (seeded: number, total: number) => void,
  signal?: AbortSignal
): Promise<WhitepawnSeedCounts> {
  integer(rows, 'rows', 1);
  signal?.throwIfAborted();
  const counts: WhitepawnSeedCounts = {
    games: rows,
    collections: 20,
    players: WHITEPAWN_PLAYERS,
    mainCount: 0,
    serializedBytes: 0,
    sqlBytes: 0,
    batches: 0,
  };
  const db = await connectSurreal(handle);
  try {
    const collections = Array.from({ length: 20 }, (_, index) => {
      const key = index === 0 ? 'main' : `c${String(index).padStart(2, '0')}`;
      return { id: `game_database:${key}`, owner: `owner-${key}`, name: `Library ${key}` };
    });
    const players = Array.from({ length: WHITEPAWN_PLAYERS }, (_, index) => ({
      id: playerId(index),
      owner: 'owner-main',
      name: `Synthetic Player ${String(index).padStart(3, '0')}`,
    }));
    for (const [table, records] of [
      ['game_database', collections],
      ['player_name', players],
    ] as const) {
      signal?.throwIfAborted();
      const sql = `INSERT INTO ${table} [${records.map((record) => `{ id: ${record.id}, owner: ${json(record.owner)}, name: ${json(record.name)} }`).join(',')}];`;
      await withTimeout(Promise.resolve(db.query(sql)), 60_000, 'seeding', signal);
      counts.serializedBytes += records.reduce(
        (bytes, record) => bytes + Buffer.byteLength(JSON.stringify(record)),
        0
      );
      counts.sqlBytes += Buffer.byteLength(sql);
      counts.batches++;
    }
    for (let start = 1; start <= rows; start += WHITEPAWN_BATCH) {
      signal?.throwIfAborted();
      const count = Math.min(WHITEPAWN_BATCH, rows - start + 1);
      const batch = insertGamesSql(start, count, rows, pgnPayload);
      await withTimeout(Promise.resolve(db.query(batch.sql)), 60_000, 'seeding', signal);
      counts.serializedBytes += batch.serializedBytes;
      counts.sqlBytes += Buffer.byteLength(batch.sql);
      counts.mainCount += batch.mainCount;
      counts.batches++;
      onProgress?.(start + count - 1, rows);
    }
    return counts;
  } finally {
    await withTimeout(db.close(), 1_000, 'seeding connection close').catch(() => {});
  }
}

function collectionOption(collection = 'game_database:main'): string {
  if (!/^game_database:(main|c(?:0[1-9]|1[0-9]))$/.test(collection)) {
    throw new Error(`Unknown synthetic collection: ${collection}`);
  }
  return collection;
}

export function gameWindowQuery(offset: number, opts: GameWindowOptions = {}): string {
  integer(offset, 'offset');
  collectionOption(opts.collection);
  const filter = opts.filtered ? ' AND result = "1-0" AND speed = "blitz"' : '';
  // Match the client's parameterized query shape. A bare record literal is
  // interpreted as a join candidate by the SSP's historical SQL converter.
  return `SELECT id, white, black, date, result, eco, source, avatar, sort_index, status, database, (SELECT id, name FROM player_name WHERE id=$parent.white LIMIT 1)[0] AS white, (SELECT id, name FROM player_name WHERE id=$parent.black LIMIT 1)[0] AS black FROM game WHERE database = $database${filter} ORDER BY sort_index ASC, id ASC LIMIT ${WHITEPAWN_PAGE} START ${offset};`;
}

/** HTTP registration values; source reads bind database as an SDK RecordId. */
export function gameWindowParams(opts: GameWindowOptions = {}): { database: string } {
  return { database: collectionOption(opts.collection) };
}

/** Bounded-memory independent oracle for the initial immutable synthetic dataset. */
export function expectedGameIds(
  rows: number,
  offset: number,
  opts: GameWindowOptions = {}
): string[] {
  integer(rows, 'rows', 1);
  integer(offset, 'offset');
  const collection = collectionOption(opts.collection);
  const ids: string[] = [];
  let matched = 0;
  for (let index = 1; index <= rows && ids.length < WHITEPAWN_PAGE; index++) {
    if (collectionFor(index) !== collection) continue;
    if (opts.filtered && (index % 3 !== 0 || Math.floor((index - 1) / 3) % 4 !== 0)) continue;
    if (matched++ >= offset) ids.push(gameId(index));
  }
  return ids;
}

/** Scroll through shallow, middle and deep windows, then revisit early windows. */
export function scrollOffsets(rows: number, steps = 100): number[] {
  integer(rows, 'rows', 1);
  integer(steps, 'steps', 1);
  const mainCount = rows - Math.floor(rows / 5);
  const windows = Math.ceil(mainCount / WHITEPAWN_PAGE);
  return Array.from({ length: steps }, (_, index) => {
    if (index === steps - 1) return 0;
    const progress = index / steps;
    let window: number;
    if (progress < 0.35) window = index;
    else if (progress < 0.7) window = Math.floor(windows / 2) + index - Math.floor(steps * 0.35);
    else if (progress < 0.9) window = windows - 1 - (index - Math.floor(steps * 0.7));
    else window = index - Math.floor(steps * 0.9);
    return Math.max(0, window % windows) * WHITEPAWN_PAGE;
  });
}
