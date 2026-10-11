export const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

export async function withTimeout<T>(
  operation: Promise<T>,
  timeoutMs: number,
  label: string,
  signal?: AbortSignal
): Promise<T> {
  signal?.throwIfAborted();
  let timer: ReturnType<typeof setTimeout> | undefined;
  let onAbort: (() => void) | undefined;
  const deadline = new Promise<never>((_, reject) => {
    timer = setTimeout(
      () => reject(new Error(`Timed out after ${timeoutMs}ms waiting for ${label}`)),
      timeoutMs
    );
    if (signal) {
      onAbort = () => reject(signal.reason ?? new Error('Aborted'));
      signal.addEventListener('abort', onAbort, { once: true });
    }
  });
  try {
    return await Promise.race([operation, deadline]);
  } finally {
    if (timer) clearTimeout(timer);
    if (onAbort) signal!.removeEventListener('abort', onAbort);
  }
}

export async function waitFor(
  predicate: () => Promise<boolean>,
  opts: { timeoutMs: number; intervalMs?: number; label?: string; signal?: AbortSignal }
): Promise<void> {
  const interval = opts.intervalMs ?? 200;
  const deadline = Date.now() + opts.timeoutMs;
  let lastErr: unknown;
  while (Date.now() < deadline) {
    opts.signal?.throwIfAborted();
    try {
      if (
        await withTimeout(
          predicate(),
          Math.max(1, deadline - Date.now()),
          opts.label ?? 'condition',
          opts.signal
        )
      )
        return;
    } catch (e) {
      opts.signal?.throwIfAborted();
      lastErr = e;
    }
    await sleep(Math.max(0, Math.min(interval, deadline - Date.now())));
  }
  throw new Error(
    `Timed out after ${opts.timeoutMs}ms waiting for ${opts.label ?? 'condition'}` +
      (lastErr ? `: ${(lastErr as Error).message ?? lastErr}` : '')
  );
}

export async function waitForHttp(
  url: string,
  opts: {
    timeoutMs: number;
    intervalMs?: number;
    expectStatus?: number;
    label?: string;
    signal?: AbortSignal;
  } = {
    timeoutMs: 30_000,
  }
) {
  const expect = opts.expectStatus ?? 200;
  const deadline = Date.now() + opts.timeoutMs;
  await waitFor(
    async () => {
      const timeout = AbortSignal.timeout(Math.max(1, Math.min(1000, deadline - Date.now())));
      const signal = opts.signal ? AbortSignal.any([timeout, opts.signal]) : timeout;
      const r = await fetch(url, { signal });
      await r.body?.cancel();
      return r.status === expect;
    },
    {
      timeoutMs: opts.timeoutMs,
      intervalMs: opts.intervalMs ?? 250,
      label: opts.label ?? `HTTP ${expect} on ${url}`,
      signal: opts.signal,
    }
  );
}
