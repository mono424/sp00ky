// This script runs in the page context and has access to window.__00__
(function () {
  let isInitialized = false;

  // Hook into Sp00ky if it exists
  function checkForSp00ky() {
    if ((window as any).__00__ && !isInitialized) {
      console.log('Sp00ky detected by DevTools');
      isInitialized = true;

      const sp00ky = (window as any).__00__;

      // Send initial detection message with full state
      window.postMessage(
        {
          type: 'SP00KY_DETECTED',
          source: 'sp00ky-devtools-page',
          data: {
            version: sp00ky.version,
            detected: true,
            state: sp00ky.getState ? sp00ky.getState() : null,
          },
        },
        '*'
      );

      // Wake the client's devtools bridge. It stays dormant (no per-event
      // serialization or state broadcasts) until it receives this handshake, so
      // prod pays nothing when no panel is attached. Reaching this point means a
      // consumer IS present, so opt in to the live SP00KY_STATE_CHANGED push channel.
      window.postMessage(
        { type: 'SP00KY_DEVTOOLS_CONNECT', source: 'sp00ky-devtools-page' },
        '*'
      );

      return true;
    }
    return false;
  }

  // Listen for GET_STATE requests
  window.addEventListener('message', (event) => {
    if (event.source !== window) return;
    if (event.data.type === 'GET_STATE' && event.data.source === 'sp00ky-devtools-content') {
      if ((window as any).__00__) {
        const sp00ky = (window as any).__00__;
        window.postMessage(
          {
            type: 'SP00KY_STATE_CHANGED',
            source: 'sp00ky-devtools-page',
            state: sp00ky.getState ? sp00ky.getState() : null,
          },
          '*'
        );
      }
    }
  });

  // Listen for execution requests from content script
  window.addEventListener('SP00KY_RUN_QUERY', async (event: any) => {
    const { requestId, query, target } = event.detail;

    if (!(window as any).__00__?.runQuery) {
      window.postMessage(
        {
          type: 'SP00KY_QUERY_RESPONSE',
          source: 'sp00ky-devtools-page',
          requestId,
          success: false,
          error: 'Sp00ky not found or runQuery not supported',
        },
        '*'
      );
      return;
    }

    try {
      const result = await (window as any).__00__.runQuery(query, target);
      window.postMessage(
        {
          type: 'SP00KY_QUERY_RESPONSE',
          source: 'sp00ky-devtools-page',
          requestId,
          success: result.success,
          data: result.data,
          error: result.error,
        },
        '*'
      );
    } catch (err: any) {
      window.postMessage(
        {
          type: 'SP00KY_QUERY_RESPONSE',
          source: 'sp00ky-devtools-page',
          requestId,
          success: false,
          error: err.message || String(err),
        },
        '*'
      );
    }
  });

  // Storage diagnostics + persist request (Storage tab). One listener with an
  // `op` discriminator instead of two copy-paste blocks.
  window.addEventListener('SP00KY_STORAGE_OP', async (event: any) => {
    const { requestId, op, options } = event.detail;
    const sp00ky = (window as any).__00__;

    const respond = (payload: { success: boolean; data?: any; error?: string }) => {
      window.postMessage(
        {
          type: 'SP00KY_STORAGE_INFO_RESPONSE',
          source: 'sp00ky-devtools-page',
          requestId,
          ...payload,
        },
        '*'
      );
    };

    const method = op === 'persist' ? sp00ky?.requestPersistentStorage : sp00ky?.getStorageInfo;
    if (!method) {
      respond({
        success: false,
        error:
          op === 'persist'
            ? 'requestPersistentStorage not supported by this core version'
            : 'getStorageInfo not supported by this core version',
      });
      return;
    }

    try {
      const data = op === 'persist' ? await sp00ky.requestPersistentStorage() : await sp00ky.getStorageInfo(options);
      respond({ success: true, data });
    } catch (err: any) {
      respond({ success: false, error: err?.message || String(err) });
    }
  });

  // Feature flags (Access tab). Same shape as SP00KY_STORAGE_OP: one listener,
  // an `op` discriminator, and `args` passed as a JSON object rather than
  // concatenated into the eval string — flag keys and user record ids come
  // from the database, so they must never become code.
  window.addEventListener('SP00KY_FLAG_OP', async (event: any) => {
    const { requestId, op, args } = event.detail;
    const sp00ky = (window as any).__00__;

    const respond = (payload: { success: boolean; data?: any; error?: string }) => {
      window.postMessage(
        {
          type: 'SP00KY_FLAG_RESPONSE',
          source: 'sp00ky-devtools-page',
          requestId,
          ...payload,
        },
        '*'
      );
    };

    const methodByOp: Record<string, string> = {
      list: 'getFlags',
      setEnabled: 'setFlagEnabled',
      setUserVariant: 'setFlagUserVariant',
      setOverride: 'setLocalFlagOverride',
      clearOverrides: 'clearLocalFlagOverrides',
    };
    const name = methodByOp[op];
    // Version skew: a panel newer than the page's core. Without this the
    // promise on the panel side hangs until its timeout with no explanation.
    if (!name || typeof sp00ky?.[name] !== 'function') {
      respond({
        success: false,
        error: `${name || op} not supported by this core version`,
      });
      return;
    }

    try {
      const a = args || {};
      let data;
      switch (op) {
        case 'setEnabled':
          data = await sp00ky.setFlagEnabled(a.key, a.enabled);
          break;
        case 'setUserVariant':
          data = await sp00ky.setFlagUserVariant(a.key, a.variant, a.remove, a.userId);
          break;
        case 'setOverride':
          data = await sp00ky.setLocalFlagOverride(a.key, a.variant, a.payload);
          break;
        case 'clearOverrides':
          data = await sp00ky.clearLocalFlagOverrides();
          break;
        default:
          data = await sp00ky.getFlags();
      }
      respond({ success: true, data });
    } catch (err: any) {
      respond({ success: false, error: err?.message || String(err) });
    }
  });

  // Impersonation (Access tab). Same shape as SP00KY_FLAG_OP. The page's core
  // answers `{ success, error, ... }` itself; authorization happens in
  // SurrealDB, so this only relays.
  window.addEventListener('SP00KY_IMPERSONATE_OP', async (event: any) => {
    const { requestId, op, args } = event.detail;
    const sp00ky = (window as any).__00__;
    const respond = (payload: { success: boolean; data?: any; error?: string }) => {
      window.postMessage(
        { type: 'SP00KY_IMPERSONATE_RESPONSE', source: 'sp00ky-devtools-page', requestId, ...payload },
        '*'
      );
    };
    if (typeof sp00ky?.impersonationOp !== 'function') {
      respond({ success: false, error: 'impersonation is not supported by this core version' });
      return;
    }
    try {
      respond({ success: true, data: await sp00ky.impersonationOp(op, args || {}) });
    } catch (err: any) {
      respond({ success: false, error: err?.message || String(err) });
    }
  });

  // Try immediately, then fast retries, then long-tail fallback
  if (!checkForSp00ky()) {
    // Fast retries for normal case
    setTimeout(checkForSp00ky, 100);
    setTimeout(checkForSp00ky, 500);
    setTimeout(checkForSp00ky, 1000);
    setTimeout(checkForSp00ky, 2000);

    // Long-tail fallback: check every 3s for up to 30s
    let longPollCount = 0;
    const longPoll = setInterval(() => {
      if (checkForSp00ky() || ++longPollCount >= 10) {
        clearInterval(longPoll);
      }
    }, 3000);
  }

  // Rows of one view, on demand: the pushed state carries counts + capped ids
  // only (see core's DevToolsService.getActiveQueries).
  window.addEventListener('SP00KY_GET_QUERY_ROWS', async (event: any) => {
    const { requestId, queryHash } = event.detail;
    const sp00ky = (window as any).__00__;
    const respond = (body: Record<string, unknown>) =>
      window.postMessage(
        { type: 'SP00KY_BRIDGE_RESPONSE', source: 'sp00ky-devtools-page', requestId, ...body },
        '*'
      );
    if (!sp00ky?.getQueryRows) {
      respond({ success: false, error: 'Sp00ky not found or getQueryRows not supported' });
      return;
    }
    try {
      respond({ success: true, data: await sp00ky.getQueryRows(Number(queryHash)) });
    } catch (err: any) {
      respond({ success: false, error: err?.message || String(err) });
    }
  });

  // Listen for GET_TABLE_DATA requests from content script
  window.addEventListener('SP00KY_GET_TABLE_DATA', async (event: any) => {
    const { requestId, tableName } = event.detail;
    const sp00ky = (window as any).__00__;

    if (!sp00ky?.getTableData) {
      window.postMessage(
        {
          type: 'SP00KY_BRIDGE_RESPONSE',
          source: 'sp00ky-devtools-page',
          requestId,
          success: false,
          error: 'Sp00ky not found or getTableData not supported',
        },
        '*'
      );
      return;
    }

    try {
      const data = await sp00ky.getTableData(tableName);
      window.postMessage(
        {
          type: 'SP00KY_BRIDGE_RESPONSE',
          source: 'sp00ky-devtools-page',
          requestId,
          success: true,
          data,
        },
        '*'
      );
    } catch (err: any) {
      window.postMessage(
        {
          type: 'SP00KY_BRIDGE_RESPONSE',
          source: 'sp00ky-devtools-page',
          requestId,
          success: false,
          error: err.message || String(err),
        },
        '*'
      );
    }
  });

  // Listen for UPDATE_TABLE_ROW requests from content script
  window.addEventListener('SP00KY_UPDATE_TABLE_ROW', async (event: any) => {
    const { requestId, tableName, recordId, updates } = event.detail;
    const sp00ky = (window as any).__00__;

    if (!sp00ky?.updateTableRow) {
      window.postMessage(
        {
          type: 'SP00KY_BRIDGE_RESPONSE',
          source: 'sp00ky-devtools-page',
          requestId,
          success: false,
          error: 'Sp00ky not found or updateTableRow not supported',
        },
        '*'
      );
      return;
    }

    try {
      const result = await sp00ky.updateTableRow(tableName, recordId, updates);
      window.postMessage(
        {
          type: 'SP00KY_BRIDGE_RESPONSE',
          source: 'sp00ky-devtools-page',
          requestId,
          success: result.success !== false,
          data: result,
          error: result.error,
        },
        '*'
      );
    } catch (err: any) {
      window.postMessage(
        {
          type: 'SP00KY_BRIDGE_RESPONSE',
          source: 'sp00ky-devtools-page',
          requestId,
          success: false,
          error: err.message || String(err),
        },
        '*'
      );
    }
  });

  // Listen for DELETE_TABLE_ROW requests from content script
  window.addEventListener('SP00KY_DELETE_TABLE_ROW', async (event: any) => {
    const { requestId, tableName, recordId } = event.detail;
    const sp00ky = (window as any).__00__;

    if (!sp00ky?.deleteTableRow) {
      window.postMessage(
        {
          type: 'SP00KY_BRIDGE_RESPONSE',
          source: 'sp00ky-devtools-page',
          requestId,
          success: false,
          error: 'Sp00ky not found or deleteTableRow not supported',
        },
        '*'
      );
      return;
    }

    try {
      const result = await sp00ky.deleteTableRow(tableName, recordId);
      window.postMessage(
        {
          type: 'SP00KY_BRIDGE_RESPONSE',
          source: 'sp00ky-devtools-page',
          requestId,
          success: result.success !== false,
          data: result,
          error: result.error,
        },
        '*'
      );
    } catch (err: any) {
      window.postMessage(
        {
          type: 'SP00KY_BRIDGE_RESPONSE',
          source: 'sp00ky-devtools-page',
          requestId,
          success: false,
          error: err.message || String(err),
        },
        '*'
      );
    }
  });

  // Listen for CLEAR_HISTORY requests from content script
  window.addEventListener('SP00KY_CLEAR_HISTORY', (event: any) => {
    const { requestId } = event.detail;
    const sp00ky = (window as any).__00__;

    if (!sp00ky?.clearHistory) {
      window.postMessage(
        {
          type: 'SP00KY_BRIDGE_RESPONSE',
          source: 'sp00ky-devtools-page',
          requestId,
          success: false,
          error: 'Sp00ky not found or clearHistory not supported',
        },
        '*'
      );
      return;
    }

    try {
      sp00ky.clearHistory();
      window.postMessage(
        {
          type: 'SP00KY_BRIDGE_RESPONSE',
          source: 'sp00ky-devtools-page',
          requestId,
          success: true,
        },
        '*'
      );
    } catch (err: any) {
      window.postMessage(
        {
          type: 'SP00KY_BRIDGE_RESPONSE',
          source: 'sp00ky-devtools-page',
          requestId,
          success: false,
          error: err.message || String(err),
        },
        '*'
      );
    }
  });

  // Backend version discovery. The panel drives this by eval in the main
  // document, but an out-of-process (cross-origin) iframe cannot be evaluated
  // in at all — `inspectedWindow.eval` addresses frames by URL and only reaches
  // same-process ones — so it has to be reachable over the message channel too.
  window.addEventListener('SP00KY_REFRESH_VERSIONS', async (event: any) => {
    const { requestId } = event.detail ?? {};
    const sp00ky = (window as any).__00__;
    const respond = (payload: { success: boolean; error?: string }) => {
      window.postMessage(
        { type: 'SP00KY_BRIDGE_RESPONSE', source: 'sp00ky-devtools-page', requestId, ...payload },
        '*'
      );
    };

    if (!sp00ky?.refreshVersions) {
      respond({ success: false, error: 'Sp00ky not found or refreshVersions not supported' });
      return;
    }
    try {
      await sp00ky.refreshVersions();
      respond({ success: true });
    } catch (err: any) {
      respond({ success: false, error: err?.message || String(err) });
    }
  });

  // Also listen for custom event in case Sp00ky loads later
  window.addEventListener('sp00ky:init', () => {
    checkForSp00ky();
  });
})();
