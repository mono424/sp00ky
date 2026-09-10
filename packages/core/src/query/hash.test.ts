import { describe, expect, it } from 'vitest';
import { queryHashInput, viewKeyInput } from './hash';

describe('hash inputs (golden against the previous DataModule formulas)', () => {
  const input = { surql: 'SELECT * FROM t WHERE a = $a', params: { a: 1, b: 'x' } };
  it('salted key is JSON.stringify({surql, params, sessionId}) in that key order', () => {
    expect(queryHashInput(input, 'sess')).toBe(JSON.stringify({ ...input, sessionId: 'sess' }));
    expect(queryHashInput(input, null)).toBe(JSON.stringify({ ...input, sessionId: null }));
  });
  it('view key is the same input minus the salt', () => {
    expect(viewKeyInput(input)).toBe(JSON.stringify(input));
    expect(viewKeyInput(input)).not.toBe(queryHashInput(input, 'sess'));
  });
});
