// Background service worker for the extension
// Handles communication between content scripts and devtools panels

console.log('Sp00ky DevTools background script loaded');

// Keep track of active connections
const connections = new Map<number, chrome.runtime.Port>();

// --- MCP Bridge WebSocket Client ---

const BRIDGE_PORT = 9315;
let bridgeSocket: WebSocket | null = null;
let bridgeReconnectTimer: ReturnType<typeof setTimeout> | null = null;
let bridgeReconnectDelay = 1000;
const BRIDGE_MAX_RECONNECT_DELAY = 30000;
let mcpEnabled = false;

// Stop retrying (and spamming the console with native "WebSocket connection
// failed" errors) once the MCP bridge has proven unreachable. Retrying resumes
// on an explicit re-enable or when a panel reopens.
let bridgeAttempts = 0;
let bridgeGaveUp = false;
const BRIDGE_MAX_ATTEMPTS = 4;

// Track tabs that have Sp00ky detected. `frameId` is the frame the MCP bridge
// talks to — the main document whenever it has a client, otherwise the first
// iframe that announced one. The bridge is tab-scoped (it has no frame picker),
// so without pinning a frame its requests would fan out to every frame's
// content script and collect several answers to one question.
const sp00kyTabs = new Map<number, { url?: string; title?: string; frameId: number }>();

/**
 * Every FRAME that has announced a Sp00ky client, per tab: `frameId` 0 is the
 * main document, anything else is an iframe. The content script runs in all
 * frames, so one tab can hold several independent clients (an embedded app, a
 * preview iframe) — the panel picks which one it is inspecting.
 *
 * Populated from `SP00KY_DETECTED`, which the page script posts once per frame.
 */
const sp00kyFrames = new Map<number, Map<number, Sp00kyFrameInfo>>();

interface Sp00kyFrameInfo {
  frameId: number;
  url: string;
  /** Client version, so the picker can flag a frame running a different build. */
  version?: string;
}

/**
 * Frame removals waiting to be applied, keyed `tabId:frameId`.
 *
 * `pagehide` says "this document is going away", but on a navigation the NEXT
 * document in the same frame often announces itself first — the two messages
 * come from different scripts and are not ordered. Deleting on arrival
 * therefore threw away an entry that had just been re-added, leaving a frame
 * that was very much alive missing from the list. So a removal is deferred and
 * cancelled by any sign of life from that frame.
 */
const pendingFrameRemovals = new Map<string, ReturnType<typeof setTimeout>>();
const FRAME_REMOVAL_DELAY_MS = 1500;

function cancelFrameRemoval(tabId: number, frameId: number) {
  const key = `${tabId}:${frameId}`;
  const timer = pendingFrameRemovals.get(key);
  if (timer) {
    clearTimeout(timer);
    pendingFrameRemovals.delete(key);
  }
}

function frameListFor(tabId: number): Sp00kyFrameInfo[] {
  const frames = sp00kyFrames.get(tabId);
  if (!frames) return [];
  // Main frame first, then iframes in discovery order.
  return [...frames.values()].sort((a, b) => a.frameId - b.frameId);
}

/** Push the tab's frame list to its panel (no-op when no panel is attached). */
function postFrames(tabId: number) {
  connections.get(tabId)?.postMessage({
    type: 'SP00KY_FRAMES',
    frames: frameListFor(tabId),
  });
}

function connectToBridge() {
  if (!mcpEnabled || bridgeGaveUp) return;
  if (bridgeSocket && bridgeSocket.readyState === WebSocket.OPEN) return;

  bridgeAttempts++;

  try {
    bridgeSocket = new WebSocket(`ws://127.0.0.1:${BRIDGE_PORT}`);
  } catch (err) {
    console.debug('[DevTools Bridge] Failed to create WebSocket:', err);
    scheduleBridgeReconnect();
    return;
  }

  bridgeSocket.addEventListener('open', () => {
    console.log('[DevTools Bridge] Connected to MCP bridge');
    bridgeReconnectDelay = 1000; // Reset backoff
    bridgeAttempts = 0;
    bridgeGaveUp = false;

    // Report connected tabs
    reportTabsToBridge();
    broadcastMcpStatus();
  });

  bridgeSocket.addEventListener('message', (event) => {
    try {
      const msg = JSON.parse(event.data as string);
      handleBridgeRequest(msg);
    } catch (err) {
      console.warn('[DevTools Bridge] Bad message:', err);
    }
  });

  bridgeSocket.addEventListener('close', () => {
    console.debug('[DevTools Bridge] Disconnected from MCP bridge');
    bridgeSocket = null;
    broadcastMcpStatus();
    scheduleBridgeReconnect();
  });

  bridgeSocket.addEventListener('error', () => {
    // The native "WebSocket connection failed" is already logged by the browser;
    // don't add to the noise. onclose fires next and drives the reconnect.
  });
}

function scheduleBridgeReconnect() {
  if (!mcpEnabled) return;
  if (bridgeReconnectTimer) return;
  if (bridgeAttempts >= BRIDGE_MAX_ATTEMPTS) {
    if (!bridgeGaveUp) {
      bridgeGaveUp = true;
      console.info(
        `[DevTools Bridge] MCP bridge unreachable on ws://127.0.0.1:${BRIDGE_PORT} after ${BRIDGE_MAX_ATTEMPTS} attempts — pausing retries. Start the devtools-mcp server, then re-toggle MCP (or reopen the panel) to retry.`
      );
      broadcastMcpStatus();
    }
    return;
  }
  bridgeReconnectTimer = setTimeout(() => {
    bridgeReconnectTimer = null;
    bridgeReconnectDelay = Math.min(bridgeReconnectDelay * 1.5, BRIDGE_MAX_RECONNECT_DELAY);
    connectToBridge();
  }, bridgeReconnectDelay);
}

function disconnectFromBridge() {
  if (bridgeReconnectTimer) {
    clearTimeout(bridgeReconnectTimer);
    bridgeReconnectTimer = null;
  }
  if (bridgeSocket) {
    bridgeSocket.close();
    bridgeSocket = null;
  }
  bridgeReconnectDelay = 1000;
  bridgeAttempts = 0;
  bridgeGaveUp = false;
  broadcastMcpStatus();
}

function setMcpEnabled(enabled: boolean) {
  mcpEnabled = enabled;
  chrome.storage.local.set({ mcpEnabled: enabled });
  if (enabled) {
    bridgeAttempts = 0;
    bridgeGaveUp = false;
    connectToBridge();
  } else {
    disconnectFromBridge();
  }
}

/** Resume connecting after a give-up (e.g. panel reopened / status requested). */
function retryBridgeIfGaveUp() {
  if (mcpEnabled && bridgeGaveUp && (!bridgeSocket || bridgeSocket.readyState !== WebSocket.OPEN)) {
    bridgeAttempts = 0;
    bridgeGaveUp = false;
    connectToBridge();
  }
}

function reportTabsToBridge() {
  if (!bridgeSocket || bridgeSocket.readyState !== WebSocket.OPEN) return;
  const tabs = Array.from(sp00kyTabs.entries()).map(([tabId, info]) => ({
    tabId,
    ...info,
  }));
  bridgeSocket.send(
    JSON.stringify({
      jsonrpc: '2.0',
      method: 'tabsChanged',
      params: { tabs },
    })
  );
}

// Map of pending bridge request IDs to their tab IDs
const pendingBridgeRequests = new Map<string, number>();

async function handleBridgeRequest(msg: any) {
  if (msg.jsonrpc !== '2.0' || !msg.id || !msg.method) return;

  const { id, method, params = {}, tabId: requestedTabId } = msg;

  // Resolve target tab
  let targetTabId: number | undefined = requestedTabId;
  if (targetTabId === undefined) {
    // Use first known sp00ky tab
    const firstTab = sp00kyTabs.keys().next().value;
    targetTabId = firstTab;
  }

  if (targetTabId === undefined) {
    sendBridgeError(id, -32000, 'No Sp00ky tabs connected');
    return;
  }

  // Which frame in that tab the bridge speaks to (see `sp00kyTabs`).
  const targetFrameId = sp00kyTabs.get(targetTabId)?.frameId ?? 0;

  // Handle getState by requesting state from the page
  if (method === 'getState') {
    // Register pending request, then ask content script for state
    pendingBridgeRequests.set(id, targetTabId);
    try {
      await chrome.tabs.sendMessage(
        targetTabId,
        { type: 'GET_SP00KY_STATE' },
        { frameId: targetFrameId }
      );
    } catch (err: any) {
      pendingBridgeRequests.delete(id);
      sendBridgeError(id, -32000, `Failed to contact tab: ${err.message}`);
    }
    return;
  }

  // For methods that go through content script events and wait for a response
  const methodToType: Record<string, string> = {
    runQuery: 'RUN_QUERY',
    getTableData: 'GET_TABLE_DATA',
    getQueryRows: 'GET_QUERY_ROWS',
    updateTableRow: 'UPDATE_TABLE_ROW',
    deleteTableRow: 'DELETE_TABLE_ROW',
    // `{ method: 'mutationOp' | 'logOp', op, args }`, answered by page-script.
    pageOp: 'PAGE_OP',
  };

  const msgType = methodToType[method];
  if (!msgType) {
    sendBridgeError(id, -32601, `Unknown method: ${method}`);
    return;
  }

  // Generate a requestId and track it
  const requestId = `bridge-${id}`;
  pendingBridgeRequests.set(requestId, targetTabId);

  try {
    await chrome.tabs.sendMessage(
      targetTabId,
      { type: msgType, payload: { ...params, requestId } },
      { frameId: targetFrameId }
    );
  } catch (err: any) {
    pendingBridgeRequests.delete(requestId);
    sendBridgeError(id, -32000, `Failed to contact tab: ${err.message}`);
  }
}

function sendBridgeResponse(id: string, result: unknown) {
  if (!bridgeSocket || bridgeSocket.readyState !== WebSocket.OPEN) return;
  bridgeSocket.send(JSON.stringify({ jsonrpc: '2.0', id, result }));
}

function sendBridgeError(id: string, code: number, message: string) {
  if (!bridgeSocket || bridgeSocket.readyState !== WebSocket.OPEN) return;
  bridgeSocket.send(JSON.stringify({ jsonrpc: '2.0', id, error: { code, message } }));
}

// Keepalive ping to prevent service worker termination
setInterval(() => {
  if (bridgeSocket && bridgeSocket.readyState === WebSocket.OPEN) {
    // Send a lightweight ping message (WebSocket API handles pong automatically)
    bridgeSocket.send(JSON.stringify({ jsonrpc: '2.0', method: 'ping', params: {} }));
  }
}, 20_000);

// Load MCP enabled state and connect if enabled
chrome.storage.local.get('mcpEnabled', (result) => {
  mcpEnabled = result.mcpEnabled === true;
  if (mcpEnabled) {
    connectToBridge();
  }
});

function getMcpStatus() {
  return {
    type: 'MCP_STATUS',
    enabled: mcpEnabled,
    connected: bridgeSocket !== null && bridgeSocket.readyState === WebSocket.OPEN,
    port: BRIDGE_PORT,
  };
}

function broadcastMcpStatus() {
  const status = getMcpStatus();
  for (const port of connections.values()) {
    port.postMessage(status);
  }
}

// --- End MCP Bridge ---

// Handle connections from devtools panels
chrome.runtime.onConnect.addListener((port) => {
  console.log('DevTools panel connected');

  let tabId: number | undefined;

  // Listen for messages from the devtools panel
  const messageListener = (message: any) => {
    if (message.name === 'init') {
      tabId = message.tabId;
      if (tabId !== undefined) {
        connections.set(tabId, port);
        // A panel opened after the page loaded missed every SP00KY_DETECTED,
        // so hand it what we already know instead of leaving the frame picker
        // empty until the next navigation.
        postFrames(tabId);
      }
    }

    if (message.type === 'GET_FRAMES') {
      if (tabId !== undefined) postFrames(tabId);
      return;
    }

    // Handle MCP status request from panel — a fresh panel is a good moment to
    // resume retrying if we'd previously given up.
    if (message.type === 'GET_MCP_STATUS') {
      retryBridgeIfGaveUp();
      port.postMessage(getMcpStatus());
      return;
    }

    // Handle MCP enable/disable from panel
    if (message.type === 'SET_MCP_ENABLED') {
      setMcpEnabled(!!message.enabled);
      // Broadcast after a short delay to let connect/disconnect settle
      setTimeout(() => broadcastMcpStatus(), 100);
      return;
    }

    // Forward messages to the content script
    if (tabId) {
      if (message.type === 'RUN_QUERY') {
        console.log('[DevTools Background] Forwarding RUN_QUERY to tab', tabId);
      }
      // Address ONE frame when the panel names it. Without this the message
      // reaches every frame's content script (the script runs in all of them),
      // so an inspected iframe and the main document would both answer a single
      // state request and the panel would render whichever landed last.
      const sent =
        typeof message.frameId === 'number'
          ? chrome.tabs.sendMessage(tabId, message, { frameId: message.frameId })
          : chrome.tabs.sendMessage(tabId, message);
      sent.catch((error: unknown) => {
        // Ignore errors if content script is not ready or tab is closed
        console.warn('Failed to send message to content script:', error);
      });
    } else {
      console.warn('[DevTools Background] Dropping message, no tabId for port', message);
    }
  };

  port.onMessage.addListener(messageListener);

  port.onDisconnect.addListener(() => {
    console.log('DevTools panel disconnected');
    if (tabId) {
      connections.delete(tabId);
    }
  });
});

// Handle messages from content scripts
chrome.runtime.onMessage.addListener((message, sender) => {
  const senderTabId = sender.tab?.id;
  // 0 for the main document. Older Chrome without frameId on a sender falls
  // back to 0, i.e. "treat it as the main frame" — the pre-iframe behavior.
  const senderFrameId = sender.frameId ?? 0;

  // Track Sp00ky tabs
  if (message.type === 'SP00KY_DETECTED' && senderTabId) {
    // The MCP bridge is tab-scoped, so it keeps tracking the TAB; the main
    // frame wins the entry, an iframe only fills it while no main-frame client
    // has announced itself.
    const known = sp00kyTabs.get(senderTabId);
    if (senderFrameId === 0 || !known) {
      sp00kyTabs.set(senderTabId, {
        url: sender.tab?.url,
        title: sender.tab?.title,
        frameId: senderFrameId,
      });
      reportTabsToBridge();
    }

    // The frame just spoke, so any queued removal for it is stale.
    cancelFrameRemoval(senderTabId, senderFrameId);

    let frames = sp00kyFrames.get(senderTabId);
    if (!frames) {
      frames = new Map();
      sp00kyFrames.set(senderTabId, frames);
    }
    frames.set(senderFrameId, {
      frameId: senderFrameId,
      url: sender.url ?? sender.tab?.url ?? '',
      version: message.data?.version,
    });
    postFrames(senderTabId);
  }

  // A frame this panel can be pointed at changed its URL (SPA route change).
  // The URL is how `inspectedWindow.eval` addresses a frame, so a stale one
  // breaks every eval — keep the registry current and re-publish it.
  if (message.type === 'SP00KY_FRAME_URL' && senderTabId && message.url) {
    cancelFrameRemoval(senderTabId, senderFrameId);
    const frames = sp00kyFrames.get(senderTabId);
    if (frames) {
      const existing = frames.get(senderFrameId);
      // Re-registers a bfcache-restored frame as well as re-addressing a routed
      // one, so `pageshow` does not need its own branch here.
      frames.set(senderFrameId, { ...existing, frameId: senderFrameId, url: message.url });
      postFrames(senderTabId);
    }
    return;
  }

  // The frame's document is being torn down (navigation, or the host page
  // dropped the iframe). Forget it — a frame that cannot be evaluated in must
  // not stay in the picker.
  if (message.type === 'SP00KY_FRAME_GONE' && senderTabId) {
    const tabId = senderTabId;
    const frameId = senderFrameId;
    const key = `${tabId}:${frameId}`;
    if (pendingFrameRemovals.has(key)) return;
    pendingFrameRemovals.set(
      key,
      setTimeout(() => {
        pendingFrameRemovals.delete(key);
        const frames = sp00kyFrames.get(tabId);
        if (frames?.delete(frameId)) postFrames(tabId);
      }, FRAME_REMOVAL_DELAY_MS)
    );
    return;
  }

  // A new top-level document. THIS is a page reload — not `tabs.onUpdated`,
  // which reports "loading" for subframe navigations too. Every frame of the
  // previous page is gone; the new page's frames announce themselves.
  if (message.type === 'SP00KY_MAIN_DOCUMENT' && senderTabId) {
    sp00kyFrames.delete(senderTabId);
    postFrames(senderTabId);
    connections.get(senderTabId)?.postMessage({ type: 'PAGE_RELOADED' });
    return;
  }

  // Handle bridge responses (from page-script via content script)
  if (message.type === 'SP00KY_BRIDGE_RESPONSE' && message.requestId) {
    const bridgeId = message.requestId.replace('bridge-', '');
    pendingBridgeRequests.delete(message.requestId);

    if (message.success) {
      sendBridgeResponse(bridgeId, message.data ?? { success: true });
    } else {
      sendBridgeError(bridgeId, -32000, message.error || 'Unknown error');
    }
    // Don't return early - let it also forward to devtools panel if connected
  }

  // Handle query responses for bridge
  if (message.type === 'SP00KY_QUERY_RESPONSE' && message.requestId?.startsWith('bridge-')) {
    const bridgeId = message.requestId.replace('bridge-', '');
    pendingBridgeRequests.delete(message.requestId);

    if (message.success) {
      sendBridgeResponse(bridgeId, { success: true, data: message.data });
    } else {
      sendBridgeError(bridgeId, -32000, message.error || 'Query failed');
    }
  }

  // Handle state responses for bridge getState requests
  if (message.type === 'SP00KY_STATE_CHANGED' && senderTabId) {
    // Check if any pending bridge getState requests match this tab
    for (const [reqId, reqTabId] of pendingBridgeRequests) {
      if (reqTabId === senderTabId && !reqId.startsWith('bridge-')) {
        pendingBridgeRequests.delete(reqId);
        sendBridgeResponse(reqId, message.state);
      }
    }
  }

  // Anything arriving from a frame proves that frame is alive, whatever the
  // registry currently believes. Re-adding it here makes the list self-healing:
  // no ordering of detect/teardown messages can leave a frame that is actively
  // pushing state missing from the picker.
  if (senderTabId && message.source === 'sp00ky-devtools-page') {
    cancelFrameRemoval(senderTabId, senderFrameId);
    const frames = sp00kyFrames.get(senderTabId);
    if (frames && !frames.has(senderFrameId)) {
      frames.set(senderFrameId, { frameId: senderFrameId, url: sender.url ?? '' });
      postFrames(senderTabId);
    }
  }

  // Forward state updates to the appropriate devtools panel
  if (senderTabId) {
    if (connections.has(senderTabId)) {
      const port = connections.get(senderTabId);
      console.log(
        '[DevTools Background] Forwarding content message to panel. Type:',
        message.type,
        'Tab:',
        senderTabId
      );
      // Stamp the origin frame: with the content script in every frame, the
      // panel has to drop pushes from frames it is not inspecting.
      port?.postMessage({ ...message, frameId: senderFrameId, frameUrl: sender.url });
    } else {
      console.log(
        '[DevTools Background] NO CONNECTION found for tab',
        senderTabId,
        'Active connections:',
        Array.from(connections.keys())
      );
    }
  } else {
    console.warn('[DevTools Background] Message from unknown sender (no tab id)', sender);
  }
});

// Page lifecycle is driven by the content script's `SP00KY_MAIN_DOCUMENT` and
// per-frame `SP00KY_FRAME_GONE` instead of `chrome.tabs.onUpdated`. The tab's
// status is a whole-TAB signal: a subframe navigating flips it to "loading" and
// back to "complete", so using it meant an embedded app navigating looked
// exactly like the top page reloading — which wiped the frame registry and
// dropped the iframe the panel was inspecting.

// Clean up sp00ky tabs when tabs are closed
chrome.tabs.onRemoved.addListener((tabId) => {
  sp00kyFrames.delete(tabId);
  if (sp00kyTabs.has(tabId)) {
    sp00kyTabs.delete(tabId);
    reportTabsToBridge();
  }
});
