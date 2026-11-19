/**
 * The benchmark's dataset: games, their comments and the users who play and
 * write them. Shaped like a real app's rows (WhitePawn's game list): about a
 * kilobyte per game, most of it one long text field no list ever shows, a few
 * record links, a version. Generated from a seed, so every backend and every
 * run sees the same bytes.
 */
export type Row = Record<string, unknown>;

export interface Dataset {
  users: Row[];
  games: Row[];
  comments: Row[];
}

export function rng(seed: number): (n: number) => number {
  let s = seed | 0;
  return (n) => {
    s = (Math.imul(s, 1103515245) + 12345) & 0x7fffffff;
    return s % n;
  };
}

const ALPHABET = '0123456789abcdefghijklmnopqrstuvwxyz';

/** A 20-character id in the alphabet SurrealDB's random ids use. */
function randomId(rnd: (n: number) => number): string {
  let out = '';
  for (let i = 0; i < 20; i++) out += ALPHABET[rnd(36)];
  return out;
}

const OPENINGS = ['Sicilian Defense', 'Ruy Lopez', 'Queen’s Gambit', 'French Defense', 'Caro-Kann', 'King’s Indian', 'English Opening', 'Italian Game'];
const ECO = ['B90', 'C65', 'D37', 'C11', 'B12', 'E60', 'A20', 'C50', 'B33', 'D85', 'A45', 'C42', 'B01', 'E12', 'A04', 'C88', 'D02', 'B22', 'E32', 'A10'];
const NAMES = ['Müller', 'Fall', 'Nakamura', 'Carlsen', 'Polgár', 'Ding', 'Firouzja', 'Abdusattorov', 'Hou', 'Kosteniuk'];
const RESULTS = ['1-0', '0-1', '1/2-1/2'];
const SAN = ['e4', 'c5', 'Nf3', 'd6', 'd4', 'cxd4', 'Nxd4', 'Nf6', 'Nc3', 'a6', 'Be3', 'e5', 'Nb3', 'Be6', 'f3', 'Be7', 'Qd2', 'O-O', 'O-O-O', 'Nbd7', 'g4', 'b5', 'g5', 'b4', 'Ne2', 'Ne8', 'f4', 'a5', 'f5', 'a4'];

function pgn(rnd: (n: number) => number, moves: number): string {
  let out = '';
  for (let m = 1; m <= moves; m++) out += `${m}. ${SAN[rnd(SAN.length)]} ${SAN[rnd(SAN.length)]} `;
  return out;
}

export function generate(games: number, comments: number, users = 500): Dataset {
  const rnd = rng(20261117);
  const userRows: Row[] = [];
  for (let i = 0; i < users; i++) {
    userRows.push({
      id: `user:${randomId(rnd)}`,
      _00_rv: 1,
      name: `${NAMES[rnd(NAMES.length)]}, ${String.fromCharCode(65 + rnd(26))}.`,
      elo: 1200 + rnd(1600),
      country: ['DE', 'SN', 'US', 'NO', 'HU', 'CN', 'FR', 'UZ'][rnd(8)],
    });
  }
  const gameRows: Row[] = [];
  for (let i = 0; i < games; i++) {
    const white = userRows[rnd(users)];
    const black = userRows[rnd(users)];
    const moves = 30 + rnd(40);
    gameRows.push({
      id: `game:${randomId(rnd)}`,
      _00_rv: 1 + rnd(9),
      white: white.id,
      black: black.id,
      white_name: white.name,
      black_name: black.name,
      white_elo: white.elo,
      black_elo: black.elo,
      result: RESULTS[rnd(3)],
      opening: OPENINGS[rnd(OPENINGS.length)],
      eco: ECO[rnd(ECO.length)],
      event: `Rated blitz game #${i}`,
      site: 'https://lichess.org/' + randomId(rnd).slice(0, 8),
      played_at: new Date(1700000000000 + i * 61_000).toISOString(),
      created_ms: 1700000000000 + rnd(30_000_000) * 1000,
      time_control: ['180+0', '180+2', '300+0', '600+5'][rnd(4)],
      moves,
      rated: rnd(5) !== 0,
      favorite: rnd(12) === 0,
      tags: ['blitz', `opening-${rnd(7)}`, rnd(2) ? 'rated' : 'casual'],
      collection: `collection:${String(i % 40).padStart(4, '0')}`,
      pgn: pgn(rnd, moves),
    });
  }
  const commentRows: Row[] = [];
  for (let i = 0; i < comments; i++) {
    commentRows.push({
      id: `comment:${randomId(rnd)}`,
      _00_rv: 1,
      game: gameRows[rnd(games)].id,
      author: userRows[rnd(users)].id,
      rank: rnd(1000),
      hidden: rnd(10) === 0,
      text: `Move ${rnd(60)} was the turning point: ${SAN[rnd(SAN.length)]} instead of ${SAN[rnd(SAN.length)]}.`,
      created_ms: 1700000000000 + i * 1000,
    });
  }
  return { users: userRows, games: gameRows, comments: commentRows };
}

/** `n` distinct ids of `rows`, picked from a seed. */
export function pick(rows: Row[], n: number, seed: number): string[] {
  const rnd = rng(seed);
  const out = new Set<string>();
  while (out.size < Math.min(n, rows.length)) out.add(String(rows[rnd(rows.length)].id));
  return [...out];
}
