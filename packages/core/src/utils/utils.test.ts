import { describe, it, expect } from 'vitest';
import { RecordId } from 'surrealdb';
import {
  compareRecordIds,
  encodeRecordId,
  extractIdPart,
  extractTablePart,
  parseRecordIdString,
  generateId,
  parseDuration,
} from './index';
import type { QueryTimeToLive } from '../types';

describe('compareRecordIds', () => {
  it('returns true for equal strings', () => {
    expect(compareRecordIds('user:123', 'user:123')).toBe(true);
  });

  it('returns false for different strings', () => {
    expect(compareRecordIds('user:123', 'user:456')).toBe(false);
  });

  it('returns true for equal RecordIds', () => {
    const a = new RecordId('user', '123');
    const b = new RecordId('user', '123');
    expect(compareRecordIds(a, b)).toBe(true);
  });

  it('returns false for different RecordIds', () => {
    const a = new RecordId('user', '123');
    const b = new RecordId('user', '456');
    expect(compareRecordIds(a, b)).toBe(false);
  });

  it('returns true for RecordId matching equivalent string', () => {
    const rid = new RecordId('user', '123');
    expect(compareRecordIds(rid, 'user:123')).toBe(true);
  });

  it('returns true for string matching equivalent RecordId', () => {
    const rid = new RecordId('user', '123');
    expect(compareRecordIds('user:123', rid)).toBe(true);
  });

  it('returns false for mismatched RecordId and string', () => {
    const rid = new RecordId('user', '123');
    expect(compareRecordIds(rid, 'post:123')).toBe(false);
  });
});

describe('encodeRecordId', () => {
  it('encodes a RecordId to table:id format', () => {
    const rid = new RecordId('user', 'abc');
    expect(encodeRecordId(rid)).toBe('user:abc');
  });

  it('encodes a RecordId with numeric-like id', () => {
    const rid = new RecordId('post', '42');
    expect(encodeRecordId(rid)).toBe('post:42');
  });
});

describe('extractIdPart', () => {
  it('extracts id from a string', () => {
    expect(extractIdPart('user:123')).toBe('123');
  });

  it('extracts id from a string with colons in the id', () => {
    expect(extractIdPart('table:some:complex:id')).toBe('some:complex:id');
  });

  it('extracts string id from a RecordId', () => {
    const rid = new RecordId('user', 'abc');
    expect(extractIdPart(rid)).toBe('abc');
  });

  it('extracts numeric id from a RecordId as string', () => {
    const rid = new RecordId('user', 42 as any);
    expect(extractIdPart(rid)).toBe('42');
  });
});

describe('extractTablePart', () => {
  it('extracts table from a string', () => {
    expect(extractTablePart('user:123')).toBe('user');
  });

  it('extracts table from a string with colons in the id', () => {
    expect(extractTablePart('table:some:complex:id')).toBe('table');
  });

  it('extracts table from a RecordId', () => {
    const rid = new RecordId('post', 'abc');
    expect(extractTablePart(rid)).toBe('post');
  });
});

describe('parseRecordIdString', () => {
  it('parses standard table:id format', () => {
    const rid = parseRecordIdString('user:123');
    expect(rid).toBeInstanceOf(RecordId);
    expect(rid.table.toString()).toBe('user');
    expect(rid.id).toBe('123');
  });

  it('parses id containing colons', () => {
    const rid = parseRecordIdString('table:part1:part2');
    expect(rid.table.toString()).toBe('table');
    expect(rid.id).toBe('part1:part2');
  });
});

describe('generateId', () => {
  it('returns a 32-character hex string', () => {
    const id = generateId();
    expect(id).toMatch(/^[0-9a-f]{32}$/);
  });

  it('generates unique ids across calls', () => {
    const ids = new Set(Array.from({ length: 100 }, () => generateId()));
    expect(ids.size).toBe(100);
  });
});

describe('parseDuration', () => {
  it('parses seconds', () => {
    expect(parseDuration('30s' as QueryTimeToLive)).toBe(30000);
  });

  it('parses minutes', () => {
    expect(parseDuration('5m')).toBe(300000);
  });

  it('parses hours', () => {
    expect(parseDuration('1h')).toBe(3600000);
  });

  it('returns default for invalid string', () => {
    expect(parseDuration('invalid' as any)).toBe(600000);
  });

  it('returns default for unknown unit', () => {
    expect(parseDuration('10d' as any)).toBe(600000);
  });

  it('returns default for non-string non-bigint input', () => {
    expect(parseDuration(undefined as any)).toBe(600000);
  });

  it('handles bigint input', () => {
    expect(parseDuration(5000n as any)).toBe(5000);
  });
});
