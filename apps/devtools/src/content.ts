// Content script - injects a script into the page to access Sp00ky
// and communicates with the background script

console.log('Sp00ky DevTools content script loaded');

// Inject a script into the page context to access window.__00__
// Using external file to avoid CSP violations with inline scripts
function injectPageScript() {
  const script = document.createElement('script');
  script.src = chrome.runtime.getURL('page-script.js');
  script.addEventListener('load', function () {
    // Remove script tag after execution to keep DOM clean
    try {
      script.remove();
    } catch (e) {
      // Ignore errors if script is already removed
      console.warn('[DevTools] Script removal failed:', e);
    }
  });
  script.addEventListener('error', function (error) {
    console.error('[DevTools] Failed to load page-script.js:', error);
  });
  (document.head || document.documentElement).appendChild(script);
}

/** Send to the background, tolerating a reloaded/invalidated extension. */
function sendToBackground(message: Record<string, unknown>) {
  try {
    chrome.runtime.sendMessage(message).catch((error) => {
      // Silently ignore "Extension context invalidated" errors (happens during dev reloads)
      if (!error.message?.includes('Extension context invalidated')) {
        console.warn('[DevTools] Failed to send message to background:', error);
      }
    });
  } catch (_error) {
    // Extension was reloaded, runtime is no longer available
    // This is normal during development, silently ignore
  }
}

/**
 * Report this frame's URL whenever it changes.
 *
 * The panel evaluates inside an iframe with `inspectedWindow.eval`'s
 * `frameURL` option — Chrome offers no frameId there — so the URL IS the
 * address. A client-side route change (`history.pushState`) rewrites it
 * without reloading the document, so nothing re-announces the frame and the
 * panel keeps targeting a URL that no longer matches anything: every eval then
 * fails with E_NOTFOUND and the panel looks disconnected.
 *
 * Polled, because `pushState` raises no event and this content script lives in
 * the isolated world where patching the page's History API is not possible.
 * Only frames that actually host a client poll, and the check is a string
 * compare.
 */
let lastReportedUrl = location.href;
let urlWatcher: ReturnType<typeof setInterval> | undefined;

function reportUrlIfChanged() {
  if (location.href === lastReportedUrl) return;
  lastReportedUrl = location.href;
  sendToBackground({
    type: 'SP00KY_FRAME_URL',
    source: 'sp00ky-devtools-content',
    url: lastReportedUrl,
  });
}

function watchFrameUrl() {
  if (urlWatcher) return;
  window.addEventListener('popstate', reportUrlIfChanged);
  window.addEventListener('hashchange', reportUrlIfChanged);
  urlWatcher = setInterval(reportUrlIfChanged, 1000);

  // This document is going away (the frame navigated, or the host page tore the
  // iframe down). Say so, or the picker keeps offering a frame that can no
  // longer be evaluated in.
  window.addEventListener('pagehide', () => {
    sendToBackground({ type: 'SP00KY_FRAME_GONE', source: 'sp00ky-devtools-content' });
  });
  // Restored from the back/forward cache: same document, still a client.
  window.addEventListener('pageshow', (event) => {
    if ((event as PageTransitionEvent).persisted) {
      lastReportedUrl = location.href;
      sendToBackground({
        type: 'SP00KY_FRAME_URL',
        source: 'sp00ky-devtools-content',
        url: lastReportedUrl,
      });
    }
  });
}

// Listen for messages from the injected script
window.addEventListener('message', (event) => {
  // Only accept messages from the same window
  if (event.source !== window) return;

  // Only handle messages from our injected script
  if (event.data.source !== 'sp00ky-devtools-page') return;

  // Debug logging - Log EVERYTHING to debug connection
  console.log('[DevTools Content Script] Forwarding message:', event.data.type);

  // This frame hosts a client, so its URL is now an address the panel needs
  // kept current.
  if (event.data.type === 'SP00KY_DETECTED') watchFrameUrl();

  // Forward to background script with all relevant data
  sendToBackground({ ...event.data });
});

// Listen for messages from the background script/devtools
chrome.runtime.onMessage.addListener((message, _sender, _sendResponse) => {
  if (message.type === 'GET_SP00KY_STATE') {
    // Request state from the page
    window.postMessage(
      {
        type: 'GET_STATE',
        source: 'sp00ky-devtools-content',
      },
      '*'
    );
  } else if (message.type === 'RUN_QUERY') {
    // Dispatch event for page-script.ts to handle
    window.dispatchEvent(
      new CustomEvent('SP00KY_RUN_QUERY', {
        detail: message.payload,
      })
    );
  } else if (message.type === 'GET_QUERY_ROWS') {
    window.dispatchEvent(
      new CustomEvent('SP00KY_GET_QUERY_ROWS', {
        detail: message.payload,
      })
    );
  } else if (message.type === 'GET_TABLE_DATA') {
    window.dispatchEvent(
      new CustomEvent('SP00KY_GET_TABLE_DATA', {
        detail: message.payload,
      })
    );
  } else if (message.type === 'UPDATE_TABLE_ROW') {
    window.dispatchEvent(
      new CustomEvent('SP00KY_UPDATE_TABLE_ROW', {
        detail: message.payload,
      })
    );
  } else if (message.type === 'DELETE_TABLE_ROW') {
    window.dispatchEvent(
      new CustomEvent('SP00KY_DELETE_TABLE_ROW', {
        detail: message.payload,
      })
    );
  } else if (message.type === 'CLEAR_HISTORY') {
    window.dispatchEvent(
      new CustomEvent('SP00KY_CLEAR_HISTORY', {
        detail: message.payload,
      })
    );
  } else if (message.type === 'STORAGE_OP') {
    window.dispatchEvent(new CustomEvent('SP00KY_STORAGE_OP', { detail: message.payload }));
  } else if (message.type === 'FLAG_OP') {
    window.dispatchEvent(new CustomEvent('SP00KY_FLAG_OP', { detail: message.payload }));
  } else if (message.type === 'IMPERSONATE_OP') {
    window.dispatchEvent(new CustomEvent('SP00KY_IMPERSONATE_OP', { detail: message.payload }));
  } else if (message.type === 'REFRESH_VERSIONS') {
    window.dispatchEvent(new CustomEvent('SP00KY_REFRESH_VERSIONS', { detail: message.payload }));
  }
  // Return true to indicate we may send a response asynchronously
  return true;
});

// A fresh MAIN document: everything the panel knew about this tab's frames
// belongs to the previous page. Announced from here rather than inferred from
// `chrome.tabs.onUpdated`, whose `status: "loading"` also fires when a SUBFRAME
// navigates — treating that as "the page reloaded" wiped the frame registry
// (and so the iframe you were inspecting) every time an embedded app navigated.
if (window.top === window) {
  sendToBackground({ type: 'SP00KY_MAIN_DOCUMENT', source: 'sp00ky-devtools-content' });
}

// Inject the page script
injectPageScript();
