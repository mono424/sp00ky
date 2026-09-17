import { createSignal } from 'solid-js';

export interface RunInHostPageOptions<T> {
  onSuccess?: (result: T) => void;
  onError?: (error: any) => void;
}

/**
 * Custom hook to run code in the host page context using chrome.devtools.inspectedWindow.eval
 * This is a safer alternative to directly calling eval and handles the callback pattern reactively
 *
 * @param frameUrl - URL of the iframe to evaluate in; undefined targets the main
 *   document. Chrome's eval addresses a frame by URL (there is no frameId
 *   option), so two iframes sharing a URL are indistinguishable here and the
 *   first match wins. Every eval in the panel flows through this hook, which is
 *   what makes "inspect that iframe instead" a single switch rather than a
 *   change at ~10 call sites.
 */
export function useRunInHostPage(
  frameUrl?: () => string | undefined,
  onFrameLost?: (frameUrl: string) => void
) {
  const [isRunning, setIsRunning] = createSignal(false);
  const [error, setError] = createSignal<any>(null);

  /**
   * Execute code in the inspected page
   * @param code - JavaScript code to execute in the host page
   * @param options - Success and error callbacks
   */
  const run = <T = any>(code: string, options?: RunInHostPageOptions<T>): void => {
    setIsRunning(true);
    setError(null);

    const url = frameUrl?.();

    const handle = (result: T, isException: any) => {
      setIsRunning(false);

      if (isException) {
        // `E_NOTFOUND` means the frameURL matched no frame — the iframe
        // navigated (a route change rewrites its URL in place) rather than the
        // page throwing. Reported separately so the panel can re-address the
        // frame instead of showing a page error, or worse, quietly failing
        // every call from here on.
        if (url && isException?.code === 'E_NOTFOUND') {
          onFrameLost?.(url);
        }
        setError(isException);
        options?.onError?.(isException);
      } else {
        options?.onSuccess?.(result);
      }
    };

    if (url) {
      chrome.devtools.inspectedWindow.eval(code, { frameURL: url }, handle);
    } else {
      chrome.devtools.inspectedWindow.eval(code, handle);
    }
  };

  /**
   * Get the Sp00ky state from the host page
   */
  const getSp00kyState = (
    onSuccess: (state: any) => void,
    onError?: (error: any) => void
  ): void => {
    run(`window.__00__ ? window.__00__.getState() : null`, {
      onSuccess,
      onError,
    });
  };

  /**
   * Rows of one active query, pulled on demand (the pushed state carries only
   * counts and capped ids).
   */
  const getQueryRows = (
    queryHash: number,
    onSuccess: (rows: { data?: unknown; localArray?: unknown; remoteArray?: unknown } | null) => void,
    onError?: (error: any) => void
  ): void => {
    run(
      `(async function() {
        try {
          if (window.__00__ && window.__00__.getQueryRows) {
            return { success: true, rows: await window.__00__.getQueryRows(${Number(queryHash)}) };
          }
          return { success: false, error: 'Sp00ky not found' };
        } catch (error) {
          return { success: false, error: error instanceof Error ? error.message : String(error) };
        }
      })()`,
      {
        onSuccess: (result: any) => {
          if (result?.success) onSuccess(result.rows ?? null);
          else onError?.(result?.error ?? 'unknown');
        },
        onError,
      }
    );
  };

  /**
   * Get table data from the host page
   */
  const getTableData = (
    tableName: string,
    onSuccess: (data: Record<string, unknown>[]) => void,
    onError?: (error: any) => void
  ): void => {
    run(
      `(async function() {
        try {
          if (window.__00__ && window.__00__.getTableData) {
            const data = await window.__00__.getTableData("${tableName}");
            window.postMessage({
              type: 'SP00KY_TABLE_DATA_RESPONSE',
              source: 'sp00ky-devtools-page',
              tableName: "${tableName}",
              data: data
            }, '*');
            return { success: true, count: data?.length || 0 };
          }
          return { success: false, error: 'Sp00ky not found' };
        } catch (error) {
          return { success: false, error: error instanceof Error ? error.message : String(error) };
        }
      })()`,
      { onSuccess, onError }
    );
  };

  /**
   * Clear events history in the host page
   */
  const clearHistory = (
    onSuccess?: (result: { success: boolean }) => void,
    onError?: (error: any) => void
  ): void => {
    run(
      `(function() {
        if (window.__00__ && window.__00__.clearHistory) {
          window.__00__.clearHistory();
          return { success: true };
        }
        return { success: false };
      })()`,
      { onSuccess, onError }
    );
  };

  /**
   * Check if Sp00ky is available on the page
   */
  const checkSp00kyAvailable = (onSuccess: (available: boolean) => void): void => {
    // An eval that cannot run (frame gone, page navigating) is a "no client
    // reachable" answer, not a reason to leave the caller hanging: without this
    // the connection dot froze on its last value and the refresh spinner ran
    // until its watchdog.
    run(`!!window.__00__`, { onSuccess, onError: () => onSuccess(false) });
  };

  /**
   * Update a table row
   */
  const updateTableRow = (
    tableName: string,
    recordId: string,
    updates: Record<string, unknown>,
    onSuccess: (result: { success: boolean; error?: string }) => void,
    onError?: (error: any) => void
  ): void => {
    const updatesJson = JSON.stringify(updates).replace(/\\/g, '\\\\').replace(/"/g, '\\"');
    run(
      `(async function() {
        try {
          if (window.__00__ && window.__00__.updateTableRow) {
            const updates = JSON.parse("${updatesJson}");
            const result = await window.__00__.updateTableRow("${tableName}", "${recordId}", updates);
            return result;
          }
          return { success: false, error: 'Sp00ky not found' };
        } catch (error) {
          return { success: false, error: error instanceof Error ? error.message : String(error) };
        }
      })()`,
      { onSuccess, onError }
    );
  };

  /**
   * Delete a table row
   */
  const deleteTableRow = (
    tableName: string,
    recordId: string,
    onSuccess: (result: { success: boolean; error?: string }) => void,
    onError?: (error: any) => void
  ): void => {
    run(
      `(async function() {
        try {
          if (window.__00__ && window.__00__.deleteTableRow) {
            const result = await window.__00__.deleteTableRow("${tableName}", "${recordId}");
            return result;
          }
          return { success: false, error: 'Sp00ky not found' };
        } catch (error) {
          return { success: false, error: error instanceof Error ? error.message : String(error) };
        }
      })()`,
      { onSuccess, onError }
    );
  };

  /**
   * Run a query in the host page
   */
  const runQuery = (
    query: string,
    target: 'local' | 'remote',
    requestId: string,
    onSuccess: (result: { success: boolean; data?: any; error?: string }) => void,
    onError?: (error: any) => void
  ): void => {
    // Escape query for eval
    const escapedQuery = query.replace(/\\/g, '\\\\').replace(/"/g, '\\"').replace(/\n/g, ' ');

    // We strictly use eval to DISPATCH THE EVENT.
    // The actual execution happens in page-script.ts (which listens to this event).
    // This avoids all async/await serialization issues in eval.
    run(
      `(function() {
        try {
            window.dispatchEvent(new CustomEvent('SP00KY_RUN_QUERY', {
                detail: {
                    requestId: '${requestId}',
                    query: "${escapedQuery}",
                    target: "${target}"
                }
            }));
            return { success: true, started: true };
        } catch (error) {
            var msg = error instanceof Error ? error.message : String(error);
            return { success: false, error: msg || 'Unknown caught error in eval dispatch' };
        }
      })()`,
      { onSuccess, onError }
    );
  };

  /**
   * Storage diagnostics / persist request. Like `runQuery`, eval only
   * DISPATCHES the event; page-script.ts awaits the async work and posts a
   * SP00KY_STORAGE_INFO_RESPONSE correlated by requestId.
   */
  const storageOp = (
    op: 'info' | 'persist',
    requestId: string,
    options: { tableCounts?: boolean } | undefined,
    onSuccess: (result: { success: boolean; error?: string }) => void,
    onError?: (error: any) => void
  ): void => {
    // No user strings involved: op is a literal, options is a plain flag object.
    run(
      `(function() {
        try {
            window.dispatchEvent(new CustomEvent('SP00KY_STORAGE_OP', {
                detail: {
                    requestId: '${requestId}',
                    op: '${op}',
                    options: ${JSON.stringify(options ?? null)}
                }
            }));
            return { success: true, started: true };
        } catch (error) {
            var msg = error instanceof Error ? error.message : String(error);
            return { success: false, error: msg || 'Unknown caught error in eval dispatch' };
        }
      })()`,
      { onSuccess, onError }
    );
  };

  /**
   * Dispatch a request/response op into the page (Access tab: feature flags,
   * impersonation). Same eval-dispatch-only contract as `storageOp`:
   * page-script.ts awaits the async work and posts a response correlated by
   * requestId.
   *
   * `args` is JSON-serialized rather than interpolated. It carries strings
   * that come from the database (flag keys, user record ids, a typed reason),
   * so string concatenation here would be an eval injection. `eventName` and
   * `op` are fixed by the callers below.
   */
  const pageOp = (
    eventName: 'SP00KY_FLAG_OP' | 'SP00KY_IMPERSONATE_OP',
    op: string,
    requestId: string,
    args: Record<string, unknown> | undefined,
    onSuccess: (result: { success: boolean; error?: string }) => void,
    onError?: (error: any) => void
  ): void => {
    run(
      `(function() {
        try {
            window.dispatchEvent(new CustomEvent(${JSON.stringify(eventName)}, {
                detail: {
                    requestId: ${JSON.stringify(requestId)},
                    op: ${JSON.stringify(op)},
                    args: ${JSON.stringify(args ?? null)}
                }
            }));
            return { success: true, started: true };
        } catch (error) {
            var msg = error instanceof Error ? error.message : String(error);
            return { success: false, error: msg || 'Unknown caught error in eval dispatch' };
        }
      })()`,
      { onSuccess, onError }
    );
  };

  const flagOp = (
    op: 'list' | 'setEnabled' | 'setUserVariant' | 'setOverride' | 'clearOverrides',
    requestId: string,
    args: Record<string, unknown> | undefined,
    onSuccess: (result: { success: boolean; error?: string }) => void,
    onError?: (error: any) => void
  ): void => pageOp('SP00KY_FLAG_OP', op, requestId, args, onSuccess, onError);

  const impersonateOp = (
    op: 'status' | 'listUsers' | 'start' | 'stop',
    requestId: string,
    args: Record<string, unknown> | undefined,
    onSuccess: (result: { success: boolean; error?: string }) => void,
    onError?: (error: any) => void
  ): void => pageOp('SP00KY_IMPERSONATE_OP', op, requestId, args, onSuccess, onError);

  return {
    run,
    getSp00kyState,
    getTableData,
    getQueryRows,
    runQuery,
    storageOp,
    flagOp,
    impersonateOp,
    updateTableRow,
    deleteTableRow,
    clearHistory,
    checkSp00kyAvailable,
    isRunning,
    error,
  };
}
