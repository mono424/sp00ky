// TEMPORARY benchmark probe (not for commit). A page that sets
// `globalThis.__btrace = []` collects `{ t, ev, ...data }` entries, `t` in
// epoch milliseconds with sub-ms precision so two pages can be compared.
export function btrace(ev: string, data?: Record<string, unknown>): void {
  const g = globalThis as { __btrace?: unknown[] };
  if (!g.__btrace) return;
  g.__btrace.push({ t: performance.timeOrigin + performance.now(), ev, ...data });
}

export function bnow(): number {
  return performance.now();
}
