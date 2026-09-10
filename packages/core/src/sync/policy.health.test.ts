import { describe, expect, it } from 'vitest';
import { nextHealth, selfHealDelayMs } from './policy';
import { initialHealth } from '../state/client-state';
import { sameHealth } from '../state/reducers';

const start = { health: initialHealth('connected'), consecutiveFailures: 0, hasSyncedOnce: false };

describe('nextHealth', () => {
  it('disabled reporting never changes anything', () => {
    const out = nextHealth(start, false, new Error('x'), 0);
    expect(out).toMatchObject({ ...start, degradedNow: false, recoveredNow: false });
  });
  it('first success latches everConnected and hasSyncedOnce', () => {
    const out = nextHealth(start, true, undefined, 3);
    expect(out.health.everConnected).toBe(true);
    expect(out.hasSyncedOnce).toBe(true);
    // A second success moves nothing, so the runtime must not re-notify.
    const again = nextHealth(out, true, undefined, 3);
    expect(sameHealth(again.health, out.health)).toBe(true);
  });
  it('failures accumulate, degrade at the threshold once, recover on success', () => {
    let s = nextHealth(start, false, new Error('socket closed'), 3);
    expect(s.health.status).toBe('healthy');
    expect(s.consecutiveFailures).toBe(1);
    expect(s.health.kind).toBe('network');
    s = nextHealth(s, false, 'permission denied', 3);
    expect(s.health.kind).toBe('application');
    s = nextHealth(s, false, new Error('timeout'), 3);
    expect(s.health.status).toBe('degraded');
    expect(s.degradedNow).toBe(true);
    expect(s.health.consecutiveFailures).toBe(3);
    s = nextHealth(s, false, new Error('timeout'), 3);
    expect(s.degradedNow).toBe(false);
    expect(s.consecutiveFailures).toBe(4);
    const r = nextHealth(s, true, undefined, 3);
    expect(r.health.status).toBe('healthy');
    expect(r.recoveredNow).toBe(true);
    expect(r.health.kind).toBeUndefined();
    expect(r.consecutiveFailures).toBe(0);
  });
  it('a success after sub-threshold failures resets without a recovery flag', () => {
    const s = nextHealth(start, false, new Error('x'), 3);
    const r = nextHealth(s, true, undefined, 3);
    expect(r.recoveredNow).toBe(false);
    expect(r.consecutiveFailures).toBe(0);
  });
});

describe('selfHealDelayMs', () => {
  it('doubles from 2s and caps at 30s', () => {
    expect(selfHealDelayMs(0)).toBe(2000);
    expect(selfHealDelayMs(2)).toBe(8000);
    expect(selfHealDelayMs(10)).toBe(30000);
    expect(selfHealDelayMs(100)).toBe(30000);
  });
});
