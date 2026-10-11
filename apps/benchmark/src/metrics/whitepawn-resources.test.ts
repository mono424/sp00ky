import assert from 'node:assert/strict';
import { test } from 'node:test';
import { estimateResources, resourceLimit } from './whitepawn-resources.js';

const machine = {
  diskFreeBytes: 30 * 1024 ** 3,
  hostMemoryBytes: 36 * 1024 ** 3,
  hostFreeMemoryBytes: 8 * 1024 ** 3,
  dockerMemoryBytes: 16 * 1024 ** 3,
  dockerVersion: 'test',
};

test('10M compact tier is recorded as resource limited on a disk-constrained host', () => {
  assert.match(resourceLimit(machine, estimateResources(10_000_000, false))!, /disk/);
  assert.equal(resourceLimit(machine, estimateResources(100_000, false)), undefined);
});

test('estimates use a smaller measured case of the same payload type', () => {
  const observation = {
    rows: 100_000,
    pgnPayload: false,
    diskBytes: 1e8,
    hostPeakBytes: 2e8,
    dockerPeakBytes: 3e8,
    basis: 'observed',
  };
  const projected = estimateResources(1_000_000, false, [observation]);
  assert.equal(projected.diskBytes, 1.25e9);
  assert.equal(projected.hostPeakBytes, 2.5e9);
  assert.match(projected.basis, /observed/);
  assert.match(estimateResources(1_000_000, true, [observation]).basis, /compact-derived PGN/);
});

test('database and native processes share the physical host memory budget', () => {
  const estimate = {
    rows: 1_000_000,
    pgnPayload: false,
    diskBytes: 1024 ** 3,
    hostPeakBytes: 25 * 1024 ** 3,
    dockerPeakBytes: 5 * 1024 ** 3,
    basis: 'test',
  };
  assert.match(resourceLimit(machine, estimate)!, /host RAM/);
  assert.equal(resourceLimit(machine, { ...estimate, hostPeakBytes: 20 * 1024 ** 3 }), undefined);
});

test('scaling includes the additional rows loaded during each workload', () => {
  const observation = {
    rows: 100_000,
    loadedRows: 133_000,
    pgnPayload: false,
    diskBytes: 133_000,
    hostPeakBytes: 266_000,
    dockerPeakBytes: 399_000,
    basis: 'observed',
  };
  const projected = estimateResources(1_000_000, false, [observation], 33_000);
  assert.equal(projected.loadedRows, 1_033_000);
  assert.equal(projected.diskBytes, 1_033_000 * 1.25);
  assert.equal(projected.hostPeakBytes, 1_033_000 * 2 * 1.25);
});

test('PGN fallback carries measured compact costs and payload premiums into host limits', () => {
  const compact = {
    rows: 100_000,
    loadedRows: 133_000,
    pgnPayload: false,
    diskBytes: 0.986e9,
    hostPeakBytes: 2.5e9,
    dockerPeakBytes: 1.4e9,
    basis: 'observed',
  };
  const loadedRows = 1_033_000;
  const scale = (loadedRows / compact.loadedRows) * 1.25;
  const otherCompactCases = [
    { ...compact, rows: 50_000, loadedRows: 83_000, hostPeakBytes: 9e9 },
    { ...compact, rows: 2_000_000, loadedRows: 2_033_000, hostPeakBytes: 9e9 },
  ];
  const metadata = estimateResources(1_000_000, false, [compact], 33_000);
  const pgn = estimateResources(1_000_000, true, [...otherCompactCases, compact], 33_000);
  assert.equal(pgn.loadedRows, loadedRows);
  assert.equal(pgn.diskBytes, compact.diskBytes * scale + loadedRows * 4096);
  assert.equal(pgn.hostPeakBytes, compact.hostPeakBytes * scale + loadedRows * 1500);
  assert.equal(pgn.dockerPeakBytes, compact.dockerPeakBytes * scale + loadedRows * 750);
  assert.match(pgn.basis, /compact-derived PGN.*100000-row compact/);
  assert.match(resourceLimit(machine, metadata)!, /host RAM/);
  assert.match(resourceLimit(machine, pgn)!, /host RAM/);
});

test('compact-derived PGN estimates preserve initial floors and prefer a same-payload peer', () => {
  const compact = {
    rows: 100_000,
    pgnPayload: false,
    diskBytes: 1,
    hostPeakBytes: 1,
    dockerPeakBytes: 1,
    basis: 'observed',
  };
  const initial = estimateResources(1_000_000, true);
  const floored = estimateResources(1_000_000, true, [compact]);
  assert.equal(floored.diskBytes, initial.diskBytes);
  assert.equal(floored.hostPeakBytes, initial.hostPeakBytes);
  assert.equal(floored.dockerPeakBytes, initial.dockerPeakBytes);

  const pgn = { ...compact, pgnPayload: true, diskBytes: 2, hostPeakBytes: 3, dockerPeakBytes: 4 };
  const measured = estimateResources(1_000_000, true, [compact, pgn]);
  assert.equal(measured.diskBytes, 25);
  assert.equal(measured.hostPeakBytes, 37.5);
  assert.equal(measured.dockerPeakBytes, 50);
  assert.match(measured.basis, /^1.25 × observed 100000-row PGN case$/);
});
