import { describe, expect, it } from 'vitest';
import { backoffMs, RETRY_BASE_MS, RETRY_MAX_MS } from './constants';

describe('backoffMs', () => {
  it('doubles from the base and caps at the max', () => {
    expect(backoffMs(0)).toBe(RETRY_BASE_MS);
    expect(backoffMs(1)).toBe(RETRY_BASE_MS * 2);
    expect(backoffMs(3)).toBe(RETRY_BASE_MS * 8);
    expect(backoffMs(20)).toBe(RETRY_MAX_MS);
  });
  it('clamps negative and huge attempts', () => {
    expect(backoffMs(-5)).toBe(RETRY_BASE_MS);
    expect(backoffMs(10_000)).toBe(RETRY_MAX_MS);
  });
  it('honours custom base and max', () => {
    expect(backoffMs(2, 100, 250)).toBe(250);
    expect(backoffMs(1, 100, 250)).toBe(200);
  });
});
