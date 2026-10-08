import React, { useState } from 'react';

const BROWSER_CSS = `
.browser-window {
  width: 100%;
  height: 550px;
  background: #1e1e1e;
  border-radius: 8px;
  box-shadow: 0 20px 50px rgba(0,0,0,0.5);
  display: flex;
  flex-direction: column;
  overflow: hidden;
  border: 1px solid #3e3e42;
  font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
}

.browser-header {
  background: #2d2d2d;
  padding: 8px 12px;
  display: flex;
  align-items: center;
  gap: 16px;
  border-bottom: 1px solid #1a1a1a;
}

.window-controls {
  display: flex;
  gap: 8px;
}

.control-dot {
  width: 12px;
  height: 12px;
  border-radius: 50%;
}
.control-dot.red { background: #ff5f56; }
.control-dot.yellow { background: #ffbd2e; }
.control-dot.green { background: #27c93f; }

.url-bar {
  flex: 1;
  background: #1a1a1a;
  border-radius: 4px;
  padding: 4px 12px;
  font-size: 12px;
  color: #999;
  display: flex;
  align-items: center;
  gap: 8px;
  border: 1px solid #3e3e42;
}

.url-bar:hover {
  background: #252525;
  border-color: #555;
  color: #ccc;
}

.browser-body {
  flex: 1;
  display: flex;
  flex-direction: column;
  position: relative;
  overflow: hidden;
}

.app-viewport {
  flex: 1;
  background: #111;
  position: relative;
  overflow: hidden;
  display: flex;
  align-items: center;
  justify-content: center;
}

.app-placeholder {
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  color: #444;
}

.devtools-container {
  height: 320px;
  border-top: 1px solid #3e3e42;
  display: flex;
  flex-direction: column;
}

@media (max-width: 767px) {
  .browser-window { height: 320px; }
  .devtools-container { height: 200px; }
  .app-viewport { display: none; }
  .browser-header { padding: 6px 10px; }
  .url-bar { font-size: 10px; padding: 3px 8px; }
  .control-dot { width: 8px; height: 8px; }
}
`;

// Exact copy of devtools.css content, modified to scope to .devtools-root
const DEVTOOLS_CSS = `
/* Chrome DevTools Design System - Material Design 3 */
.devtools-root {
  /* Base colors - Dark theme (default) */
  --sys-color-cdt-base-container: #1e1e1e;
  --sys-color-cdt-base-container-high: #252526;
  --sys-color-cdt-base-container-highest: #2d2d30;

  /* Surface colors */
  --sys-color-surface: #252526;
  --sys-color-surface-container: #2d2d30;
  --sys-color-surface-container-low: #1e1e1e;
  --sys-color-surface-container-high: #3e3e42;
  --sys-color-surface-container-highest: #454545;

  /* On-surface colors */
  --sys-color-on-surface: #cccccc;
  --sys-color-on-surface-subtle: #858585;
  --sys-color-on-surface-variant: #9cdcfe;

  /* Primary colors */
  --sys-color-primary: #4fc3f7;
  --sys-color-primary-bright: #61dafb;
  --sys-color-primary-container: rgba(79, 195, 247, 0.1);
  --sys-color-on-primary: #ffffff;

  /* State colors */
  --sys-color-state-hover: rgba(255, 255, 255, 0.06);
  --sys-color-state-hover-on-subtle: rgba(255, 255, 255, 0.06);
  --sys-color-state-focus: rgba(255, 255, 255, 0.12);
  --sys-color-state-pressed: rgba(255, 255, 255, 0.18);
  --sys-color-state-selected: rgba(79, 195, 247, 0.15);
  --sys-color-state-disabled: rgba(255, 255, 255, 0.03);

  /* Divider and border */
  --sys-color-divider: #3e3e42;
  --sys-color-outline: #3e3e42;
  --sys-color-outline-variant: #2d2d30;

  /* Status colors */
  --sys-color-state-on: #4caf50;
  --sys-color-state-off: #858585;
  --sys-color-error: #f48fb1;
  --sys-color-warning: #ffb74d;
  --sys-color-info: #64b5f6;

  /* Text colors */
  --sys-color-text-primary: #cccccc;
  --sys-color-text-secondary: #858585;
  --sys-color-text-disabled: #5a5a5a;

  /* Typography */
  --sys-typescale-body-font:
    'Segoe UI', system-ui, -apple-system, 'SF Pro Display', 'Roboto', sans-serif;
  --sys-typescale-body-size: 12px;
  --sys-typescale-body-weight: 400;
  --sys-typescale-body-line-height: 18px;

  --sys-typescale-label-font:
    'Segoe UI', system-ui, -apple-system, 'SF Pro Display', 'Roboto', sans-serif;
  --sys-typescale-label-size: 11px;
  --sys-typescale-label-weight: 500;
  --sys-typescale-label-line-height: 16px;

  --sys-typescale-title-font:
    'Segoe UI', system-ui, -apple-system, 'SF Pro Display', 'Roboto', sans-serif;
  --sys-typescale-title-size: 13px;
  --sys-typescale-title-weight: 500;
  --sys-typescale-title-line-height: 20px;

  --sys-typescale-monospace-font: 'Consolas', 'Monaco', 'Courier New', monospace;
  --sys-typescale-monospace-size: 11px;
  --sys-typescale-monospace-line-height: 16px;

  /* Spacing */
  --sys-spacing-1: 4px;
  --sys-spacing-2: 8px;
  --sys-spacing-3: 12px;
  --sys-spacing-4: 16px;
  --sys-spacing-5: 20px;
  --sys-spacing-6: 24px;

  /* Border radius */
  --sys-radius-xs: 2px;
  --sys-radius-sm: 4px;
  --sys-radius-md: 6px;
  --sys-radius-lg: 8px;

  /* Shadows */
  --sys-shadow-1: 0 1px 2px rgba(0, 0, 0, 0.3);
  --sys-shadow-2: 0 2px 4px rgba(0, 0, 0, 0.3);
  --sys-shadow-3: 0 4px 8px rgba(0, 0, 0, 0.3);

  /* Transitions */
  --sys-transition-fast: 100ms cubic-bezier(0.4, 0, 0.2, 1);
  --sys-transition-base: 200ms cubic-bezier(0.4, 0, 0.2, 1);
  --sys-transition-slow: 300ms cubic-bezier(0.4, 0, 0.2, 1);

  font-family: var(--sys-typescale-body-font);
  font-size: var(--sys-typescale-body-size);
  font-weight: var(--sys-typescale-body-weight);
  line-height: var(--sys-typescale-body-line-height);
  background: var(--sys-color-cdt-base-container);
  color: var(--sys-color-on-surface);
  height: 100%;
  width: 100%;
  display: flex;
  flex-direction: column;
  overflow: hidden;
  text-align: left;
}

.devtools-root * {
  margin: 0;
  padding: 0;
  box-sizing: border-box;
}

/* Header/Toolbar */
.devtools-root .tabs {
  display: flex;
  background: var(--sys-color-surface);
  border-bottom: 1px solid var(--sys-color-divider);
  flex-shrink: 0;
}

.devtools-root .toolbar-group {
  display: flex;
  align-items: center;
  gap: var(--sys-spacing-1);
}

.devtools-root .toolbar-group + .toolbar-group {
  margin-left: var(--sys-spacing-2);
  padding-left: var(--sys-spacing-2);
  border-left: 1px solid var(--sys-color-divider);
}

.devtools-root .toolbar-group-right {
  display: flex;
  align-items: center;
  gap: var(--sys-spacing-1);
  margin-left: auto;
  flex-grow: 1;
  justify-content: flex-end;
  padding-right: var(--sys-spacing-2);
}

/* Buttons */
.devtools-root .btn {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  padding: var(--sys-spacing-1) var(--sys-spacing-2);
  min-height: 20px;
  background: transparent;
  color: var(--sys-color-on-surface);
  border: none;
  border-radius: var(--sys-radius-xs);
  cursor: pointer;
  font-family: var(--sys-typescale-label-font);
  font-size: var(--sys-typescale-label-size);
  font-weight: var(--sys-typescale-label-weight);
  line-height: var(--sys-typescale-label-line-height);
  transition: background-color var(--sys-transition-fast);
  user-select: none;
}

.devtools-root .btn:hover {
  background: var(--sys-color-state-hover);
}

/* Status indicator */
.devtools-root .status-indicator {
  display: inline-flex;
  align-items: center;
  gap: var(--sys-spacing-1);
  padding: var(--sys-spacing-1) var(--sys-spacing-2);
  font-size: var(--sys-typescale-label-size);
  line-height: var(--sys-typescale-label-line-height);
}

.devtools-root .status-dot {
  width: 8px;
  height: 8px;
  border-radius: 50%;
  flex-shrink: 0;
}

.devtools-root .status-dot.active {
  background: var(--sys-color-state-on);
  box-shadow: 0 0 4px var(--sys-color-state-on);
}

/* Tabs */
.devtools-root .tab-btn {
  position: relative;
  padding: var(--sys-spacing-1) var(--sys-spacing-2);
  background: transparent;
  color: var(--sys-color-on-surface-subtle);
  border: none;
  border-bottom: 2px solid transparent;
  cursor: pointer;
  font-family: var(--sys-typescale-label-font);
  font-size: var(--sys-typescale-label-size);
  font-weight: var(--sys-typescale-label-weight);
  line-height: var(--sys-typescale-label-line-height);
  transition: color var(--sys-transition-fast), background-color var(--sys-transition-fast);
  user-select: none;
}

.devtools-root .tab-btn:hover {
  background: var(--sys-color-state-hover);
  color: var(--sys-color-on-surface);
}

.devtools-root .tab-btn.active {
  color: var(--sys-color-primary-bright);
  border-bottom-color: var(--sys-color-primary);
}

/* Content area */
.devtools-root .content {
  flex: 1;
  overflow: hidden;
  display: flex;
  flex-direction: column;
  background: var(--sys-color-cdt-base-container);
}

.devtools-root .tab-content {
  display: none;
  flex: 1;
  overflow: hidden;
  flex-direction: column;
}

.devtools-root .tab-content.active {
  display: flex;
}

/* Pane header (Database table list) */
.devtools-root .pane-header {
  padding: 6px 8px;
  border-bottom: 1px solid var(--sys-color-divider);
  flex-shrink: 0;
  min-height: 24px;
  display: flex;
  align-items: center;
}

.devtools-root .pane-header h2 {
  font-family: var(--sys-typescale-label-font);
  font-size: var(--sys-typescale-label-size);
  font-weight: var(--sys-typescale-label-weight);
  line-height: var(--sys-typescale-label-line-height);
  color: var(--sys-color-on-surface);
  margin: 0;
  text-transform: uppercase;
  letter-spacing: 0.5px;
}

/* Toolbar icon buttons and heartbeat badge */
.devtools-root .icon-btn {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  width: 22px;
  height: 22px;
  padding: 0;
  background: transparent;
  border: none;
  border-radius: 50%;
  color: var(--sys-color-on-surface-subtle);
  cursor: pointer;
}
.devtools-root .icon-btn:hover {
  background: var(--sys-color-state-hover);
  color: var(--sys-color-on-surface);
}
.devtools-root .hb-badge {
  font-family: var(--sys-typescale-monospace-font);
  font-size: 10px;
  color: var(--sys-color-primary);
  padding: 0 4px;
}

/* Filter row (Mutations / Logs) */
.devtools-root .sim-filters {
  display: flex;
  align-items: center;
  gap: 6px;
  padding: 4px 8px;
  border-bottom: 1px solid var(--sys-color-divider);
  background: var(--sys-color-surface);
  overflow-x: auto;
  white-space: nowrap;
  flex-shrink: 0;
}
.devtools-root .filter-chip {
  flex-shrink: 0;
  display: inline-flex;
  align-items: baseline;
  gap: 5px;
  padding: 1px 8px;
  border-radius: 12px;
  background: var(--sys-color-cdt-base-container);
  border: 1px solid var(--sys-color-divider);
  font-family: var(--sys-typescale-label-font);
  font-size: 11px;
  font-weight: 500;
  color: var(--sys-color-on-surface-subtle);
  cursor: pointer;
}
.devtools-root .filter-chip.active {
  background: var(--sys-color-primary-container);
  border-color: var(--sys-color-primary);
  color: var(--sys-color-primary);
}
.devtools-root .filter-chip.danger {
  color: var(--sys-color-error);
}
.devtools-root .filter-chip.danger.active {
  background: rgba(244, 143, 177, 0.18);
  border-color: var(--sys-color-error);
}
.devtools-root .filter-chip.warn.active {
  background: rgba(255, 183, 77, 0.12);
  border-color: var(--sys-color-warning);
  color: var(--sys-color-warning);
}
.devtools-root .chip-count {
  font-variant-numeric: tabular-nums;
  opacity: 0.7;
}

/* Mutations */
.devtools-root .op-badge {
  font-size: 10px;
  font-weight: 600;
  letter-spacing: 0.3px;
  text-transform: uppercase;
}
.devtools-root .op-create { color: var(--sys-color-state-on); }
.devtools-root .op-update { color: var(--sys-color-info); }
.devtools-root .op-delete { color: var(--sys-color-error); }
.devtools-root .status-pill {
  display: inline-flex;
  align-items: center;
  gap: 5px;
  padding: 1px 7px 1px 6px;
  border-radius: 999px;
  font-family: var(--sys-typescale-label-font);
  font-size: 10px;
  font-weight: 500;
  line-height: 15px;
  text-transform: capitalize;
}
.devtools-root .status-pill svg {
  width: 10px;
  height: 10px;
  fill: none;
  stroke: currentColor;
  stroke-width: 2.6;
  stroke-linecap: round;
  stroke-linejoin: round;
  flex: none;
}
.devtools-root .pill-pending { color: var(--sys-color-info); background: rgba(33, 150, 243, 0.12); }
.devtools-root .pill-retrying { color: var(--sys-color-warning); background: rgba(255, 167, 38, 0.12); }
.devtools-root .pill-synced { color: var(--sys-color-state-on); background: rgba(76, 175, 80, 0.12); }
.devtools-root .pill-rolled-back { color: var(--sys-color-error); background: rgba(244, 67, 54, 0.12); }
.devtools-root .num { text-align: right; font-variant-numeric: tabular-nums; }
.devtools-root th.num { font-family: var(--sys-typescale-label-font); }
.devtools-root .sim-statusbar {
  display: flex;
  gap: 10px;
  padding: 3px 10px;
  border-top: 1px solid var(--sys-color-divider);
  background: var(--sys-color-surface);
  font-size: 10px;
  color: var(--sys-color-on-surface-subtle);
  white-space: nowrap;
  overflow: hidden;
  flex-shrink: 0;
}
.devtools-root .sim-statusbar .warn { color: var(--sys-color-warning); }

/* Logs */
.devtools-root .log-list {
  flex: 1;
  overflow: auto;
  font-family: var(--sys-typescale-monospace-font);
  font-size: var(--sys-typescale-monospace-size);
  line-height: var(--sys-typescale-monospace-line-height);
}
.devtools-root .log-row {
  display: flex;
  align-items: baseline;
  gap: 8px;
  padding: 2px 8px;
  border-bottom: 1px solid var(--sys-color-outline-variant);
  white-space: nowrap;
}
.devtools-root .log-time { color: var(--sys-color-on-surface-subtle); flex: none; }
.devtools-root .log-level {
  flex: none;
  width: 38px;
  font-size: 9px;
  font-weight: 600;
  text-transform: uppercase;
  color: var(--sys-color-on-surface-subtle);
}
.devtools-root .log-category { flex: none; color: var(--sys-color-on-surface-variant); }
.devtools-root .log-msg { color: var(--sys-color-on-surface); overflow: hidden; text-overflow: ellipsis; }
.devtools-root .log-info .log-level { color: var(--sys-color-info); }
.devtools-root .log-debug .log-msg { color: var(--sys-color-on-surface-subtle); }
.devtools-root .log-warn { background: rgba(255, 183, 77, 0.09); }
.devtools-root .log-warn .log-level, .devtools-root .log-warn .log-msg { color: var(--sys-color-warning); }
.devtools-root .log-error { background: rgba(244, 143, 177, 0.09); }
.devtools-root .log-error .log-level, .devtools-root .log-error .log-msg { color: var(--sys-color-error); }

/* Queries tab */
.devtools-root .queries-container {
  display: grid;
  grid-template-columns: 280px 1fr;
  height: 100%;
  overflow: hidden;
}

.devtools-root .queries-list {
  border-right: 1px solid var(--sys-color-divider);
  overflow: hidden;
  background: var(--sys-color-surface);
  display: flex;
  flex-direction: column;
}

.devtools-root .queries-list-content {
  flex: 1;
  overflow-y: auto;
  padding: 0;
  margin: 0;
}

.devtools-root .queries-header {
  padding: 6px 8px;
  border-bottom: 1px solid var(--sys-color-divider);
  background: var(--sys-color-surface);
  min-height: 24px;
  display: flex;
  align-items: center;
}

.devtools-root .queries-header h2 {
    font-family: var(--sys-typescale-label-font);
    font-size: var(--sys-typescale-label-size);
    font-weight: var(--sys-typescale-label-weight);
    line-height: var(--sys-typescale-label-line-height);
    color: var(--sys-color-on-surface);
    margin: 0;
    text-transform: uppercase;
    letter-spacing: 0.5px;
  }

.devtools-root .query-item {
  padding: 4px 8px;
  cursor: pointer;
  border: 2px solid transparent;
  transition: background-color var(--sys-transition-fast);
  list-style: none;
  background: transparent;
}

.devtools-root .query-item:hover:not(.selected) {
  background: var(--sys-color-state-hover-on-subtle) !important;
}

.devtools-root .query-item.selected {
  background: var(--sys-color-tonal-container, rgba(79, 195, 247, 0.2)) !important;
}

.devtools-root .query-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: var(--sys-spacing-1);
}

.devtools-root .query-hash {
  font-family: var(--sys-typescale-label-font);
  font-size: var(--sys-typescale-label-size);
  font-weight: var(--sys-typescale-label-weight);
  color: var(--sys-color-on-surface);
}

.devtools-root .query-status {
  padding: 2px var(--sys-spacing-1);
  border-radius: var(--sys-radius-xs);
  font-family: var(--sys-typescale-label-font);
  font-size: 9px;
  font-weight: 600;
  text-transform: uppercase;
  letter-spacing: 0.5px;
  line-height: 12px;
}

.devtools-root .status-active {
  background: rgba(76, 175, 80, 0.2);
  color: var(--sys-color-state-on);
}

.devtools-root .status-idle {
    background: rgba(133, 133, 133, 0.2);
    color: #858585;
}

.devtools-root .query-meta {
  font-family: var(--sys-typescale-monospace-font);
  font-size: var(--sys-typescale-monospace-size);
  color: var(--sys-color-on-surface-subtle);
  margin-top: var(--sys-spacing-1);
}

.devtools-root .query-preview {
  font-family: var(--sys-typescale-monospace-font);
  font-size: 10px;
  color: var(--sys-color-on-surface-subtle);
  margin-top: var(--sys-spacing-1);
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.devtools-root .query-detail {
  overflow-y: auto;
  background: var(--sys-color-cdt-base-container);
  padding: 8px;
}

.devtools-root .detail-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding-bottom: 8px;
  margin-bottom: 8px;
  border-bottom: 1px solid var(--sys-color-divider);
}

.devtools-root .detail-header h3 {
  font-family: var(--sys-typescale-title-font);
  font-size: var(--sys-typescale-title-size);
  font-weight: var(--sys-typescale-title-weight);
  color: var(--sys-color-primary-bright);
}

.devtools-root .detail-section {
  margin-bottom: 12px;
}

.devtools-root .detail-label {
  font-family: var(--sys-typescale-label-font);
  font-size: 10px;
  font-weight: var(--sys-typescale-label-weight);
  color: var(--sys-color-on-surface-subtle);
  text-transform: uppercase;
  letter-spacing: 0.5px;
  margin-bottom: 4px;
}

.devtools-root .detail-value {
  font-family: var(--sys-typescale-body-font);
  font-size: var(--sys-typescale-body-size);
  color: var(--sys-color-on-surface);
}

.devtools-root .detail-value.mono {
  font-family: var(--sys-typescale-monospace-font);
  font-size: var(--sys-typescale-monospace-size);
  color: var(--sys-color-primary);
}

.devtools-root .query-code {
  background: var(--sys-color-cdt-base-container);
  border-radius: var(--sys-radius-xs);
  padding: 6px 8px;
  margin: 0;
  overflow-x: auto;
  font-family: var(--sys-typescale-monospace-font);
  font-size: var(--sys-typescale-monospace-size);
  line-height: var(--sys-typescale-monospace-line-height);
  color: var(--sys-color-on-surface);
  border: 1px solid var(--sys-color-divider);
  white-space: pre-wrap;
  word-wrap: break-word;
}

/* Database tab */
.devtools-root .database-container {
  display: grid;
  grid-template-columns: 200px 1fr;
  height: 100%;
  overflow: hidden;
}

.devtools-root .database-tables {
  border-right: 1px solid var(--sys-color-divider);
  overflow: hidden;
  background: var(--sys-color-surface);
  display: flex;
  flex-direction: column;
}

.devtools-root .tables-list {
  flex: 1;
  overflow-y: auto;
}

.devtools-root .table-item {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 6px 12px;
  cursor: pointer;
  border-left: 2px solid transparent;
  width: 100%;
}

.devtools-root .table-item:hover {
    background: var(--sys-color-state-hover-on-subtle);
}

.devtools-root .table-item.selected {
    background: var(--sys-color-tonal-container, rgba(79, 195, 247, 0.2));
    border-left-color: var(--sys-color-primary);
}

.devtools-root .table-name {
    font-family: var(--sys-typescale-monospace-font);
    font-size: var(--sys-typescale-monospace-size);
    color: var(--sys-color-on-surface);
}

.devtools-root .table-count {
    font-family: var(--sys-typescale-label-font);
    font-size: 10px;
    background: var(--sys-color-surface-container-high);
    padding: 1px 6px;
    border-radius: 8px;
    color: var(--sys-color-on-surface-subtle);
}

.devtools-root .data-grid {
    flex: 1;
    overflow: auto;
    font-family: var(--sys-typescale-monospace-font);
    font-size: var(--sys-typescale-monospace-size);
}
.devtools-root table {
    width: 100%;
    border-collapse: collapse;
}
.devtools-root th, .devtools-root td {
    padding: 6px 12px;
    border-bottom: 1px solid var(--sys-color-divider);
    text-align: left;
}
.devtools-root th {
    color: var(--sys-color-on-surface-subtle);
    font-weight: normal;
    background: var(--sys-color-surface);
    position: sticky;
    top: 0;
}
.devtools-root td {
    color: var(--sys-color-on-surface);
    white-space: nowrap;
}
.devtools-root tr:hover td {
    background: var(--sys-color-state-hover);
}
.devtools-root .text-primary { color: var(--sys-color-primary); }
.devtools-root .text-string { color: #ce9178; }
.devtools-root .text-number { color: #b5cea8; }
`;

type MutationStatus = 'pending' | 'retrying' | 'synced' | 'rolled-back';

const MOCK_MUTATIONS: Array<{
  recordId: string;
  op: 'create' | 'update' | 'delete';
  status: MutationStatus;
  tries?: number;
  tookMs?: number;
}> = [
  { recordId: 'comment:01j9xq7r', op: 'create', status: 'pending' },
  { recordId: 'thread:p09s1', op: 'update', status: 'retrying', tries: 2 },
  { recordId: 'comment:01j9xpz4', op: 'create', status: 'synced', tookMs: 84 },
  { recordId: 'thread:7d66s', op: 'delete', status: 'rolled-back', tookMs: 212 },
  { recordId: 'thread:8x92m', op: 'update', status: 'synced', tookMs: 61 },
  { recordId: 'user:khadim', op: 'update', status: 'synced', tookMs: 47 },
];

// Stroke glyphs drawn at 10px in the pill's colour, as in the real panel.
const STATUS_ICON: Record<MutationStatus, React.ReactNode> = {
  pending: (
    <>
      <circle cx="12" cy="12" r="9" />
      <path d="M12 7v5l3 2" />
    </>
  ),
  retrying: (
    <>
      <path d="M20 12a8 8 0 1 1-2.34-5.66" />
      <path d="M20 4v5h-5" />
    </>
  ),
  synced: <path d="M5 12.5l4.5 4.5L19 7.5" />,
  'rolled-back': (
    <>
      <path d="M9 14L4 9l5-5" />
      <path d="M4 9h10.5a5.5 5.5 0 0 1 0 11H11" />
    </>
  ),
};

const MOCK_LOGS: Array<{ time: string; level: 'debug' | 'info' | 'warn' | 'error'; category: string; msg: string }> = [
  { time: '10:42:01.152', level: 'info', category: 'LocalDatabaseService::connect', msg: 'Connected to local database' },
  { time: '10:42:01.340', level: 'info', category: 'StreamProcessorService::init', msg: 'Initialized successfully' },
  { time: '10:42:01.512', level: 'info', category: 'RemoteDatabaseService::connect', msg: 'Connected to remote database' },
  { time: '10:42:05.112', level: 'debug', category: 'local', msg: 'SELECT * FROM thread ORDER BY created_at DESC LIMIT 10' },
  { time: '10:42:05.161', level: 'debug', category: 'query', msg: 'Query membership known' },
  { time: '10:42:06.020', level: 'debug', category: 'DevToolsService::onStreamUpdate', msg: 'StreamUpdate' },
  { time: '10:42:09.731', level: 'error', category: 'saga', msg: 'push rejected a mutation' },
  { time: '10:42:09.742', level: 'warn', category: 'saga', msg: 'circuit revert applied' },
  { time: '10:42:14.400', level: 'warn', category: 'saga', msg: 'push interrupted, retrying with backoff' },
  { time: '10:42:18.902', level: 'debug', category: 'saga', msg: 'drain' },
];

const MOCK_QUERIES = [
  {
    queryHash: '0x8f2a9c',
    status: 'active',
    createdAt: 1715011325112,
    lastUpdate: 1715011335112,
    updateCount: 12,
    dataSize: 1024,
    query: 'SELECT * FROM thread ORDER BY created_at DESC',
    variables: { limit: 10 },
    data: [{ id: 'thread:1', title: 'Why Rust?' }],
  },
  {
    queryHash: '0x99b1d4',
    status: 'idle',
    createdAt: 1715011326000,
    lastUpdate: 1715011330000,
    updateCount: 5,
    dataSize: 256,
    query: 'SELECT count() FROM notifications',
    variables: {},
    data: { count: 3 },
  },
];

const MOCK_TABLES = [
  { name: 'user', count: 12 },
  { name: 'thread', count: 45 },
  { name: 'comment', count: 182 },
];
const MOCK_THREAD_DATA = [
  {
    id: 'thread:8x92m',
    title: 'Why Rust is the future',
    author: 'user:khadim',
    created: '2024-05-01',
  },
  {
    id: 'thread:p09s1',
    title: 'Local-first architecture',
    author: 'user:sarah',
    created: '2024-05-02',
  },
  {
    id: 'thread:7d66s',
    title: 'sp00ky vs ElectricSQL',
    author: 'user:alex',
    created: '2024-05-03',
  },
];

export const DevToolsSimulation = () => {
  const [activeTab, setActiveTab] = useState('queries');
  const [selectedQueryHash, setSelectedQueryHash] = useState('0x8f2a9c');
  const [touchStart, setTouchStart] = useState(0);
  const [touchEnd, setTouchEnd] = useState(0);

  // The tabs that tell the sync story; the real panel adds Timing, Storage,
  // Access, Stack and MCP.
  const tabs = ['queries', 'mutations', 'logs', 'database'];
  const selectedQuery = MOCK_QUERIES.find((q) => q.queryHash === selectedQueryHash);

  // Handle swipe gestures
  const handleTouchStart = (e: React.TouchEvent) => {
    setTouchStart(e.targetTouches[0].clientX);
  };

  const handleTouchMove = (e: React.TouchEvent) => {
    setTouchEnd(e.targetTouches[0].clientX);
  };

  const handleTouchEnd = () => {
    if (!touchStart || !touchEnd) return;

    const distance = touchStart - touchEnd;
    const isLeftSwipe = distance > 50;
    const isRightSwipe = distance < -50;

    const currentIndex = tabs.indexOf(activeTab);

    if (isLeftSwipe && currentIndex < tabs.length - 1) {
      setActiveTab(tabs[currentIndex + 1]);
    }

    if (isRightSwipe && currentIndex > 0) {
      setActiveTab(tabs[currentIndex - 1]);
    }

    setTouchStart(0);
    setTouchEnd(0);
  };

  // Handle mouse/trackpad swipe
  const [mouseStart, setMouseStart] = useState(0);
  const [isDragging, setIsDragging] = useState(false);

  const handlePointerDown = (e: React.PointerEvent) => {
    setMouseStart(e.clientX);
    setIsDragging(true);
  };

  const handlePointerMove = (_e: React.PointerEvent) => {
    if (!isDragging) return;
  };

  const handlePointerUp = (e: React.PointerEvent) => {
    if (!isDragging) return;

    const distance = mouseStart - e.clientX;
    const isLeftSwipe = distance > 80;
    const isRightSwipe = distance < -80;

    const currentIndex = tabs.indexOf(activeTab);

    if (isLeftSwipe && currentIndex < tabs.length - 1) {
      setActiveTab(tabs[currentIndex + 1]);
    }

    if (isRightSwipe && currentIndex > 0) {
      setActiveTab(tabs[currentIndex - 1]);
    }

    setIsDragging(false);
  };

  return (
    <>
      <style>{BROWSER_CSS}</style>
      <style>{DEVTOOLS_CSS}</style>

      <div className="browser-window">
        {/* Browser Header */}
        <div className="browser-header">
          <div className="window-controls">
            <div className="control-dot red"></div>
            <div className="control-dot yellow"></div>
            <div className="control-dot green"></div>
          </div>
          <div className="url-bar">
            <svg
              width="12"
              height="12"
              viewBox="0 0 24 24"
              fill="none"
              stroke="currentColor"
              strokeWidth="2"
              strokeLinecap="round"
              strokeLinejoin="round"
            >
              <rect x="3" y="11" width="18" height="11" rx="2" ry="2"></rect>
              <path d="M7 11V7a5 5 0 0 1 10 0v4"></path>
            </svg>
            <span>localhost:5173</span>
          </div>
        </div>

        {/* Browser Body split */}
        <div className="browser-body">
          {/* App Viewport */}
          <div className="app-viewport">
            <div className="app-placeholder">
              <svg
                style={{ opacity: 0.05, marginBottom: 20 }}
                width="100"
                height="100"
                viewBox="0 0 24 24"
                fill="none"
                stroke="currentColor"
                strokeWidth="1"
                strokeLinecap="round"
                strokeLinejoin="round"
              >
                <path d="M12 2L2 7l10 5 10-5-10-5z"></path>
                <path d="M2 17l10 5 10-5"></path>
                <path d="M2 12l10 5 10-5"></path>
              </svg>
              <h1 style={{ fontSize: 24, fontWeight: 'bold', marginBottom: 8 }}>My sp00ky App</h1>
              <p style={{ fontSize: 14, opacity: 0.6 }}>Syncing active...</p>
            </div>
          </div>

          {/* DevTools Docked at Bottom */}
          <div className="devtools-container">
            <div className="devtools-root">
              {/* Tabs Component Replicated */}
              <div className="tabs">
                <div className="toolbar-group">
                  <div className="status-indicator">
                    <span className="status-dot active" />
                  </div>
                </div>
                {tabs.map((tab) => (
                  <button
                    key={tab}
                    className={`tab-btn ${activeTab === tab ? 'active' : ''}`}
                    onClick={() => setActiveTab(tab)}
                  >
                    {tab.charAt(0).toUpperCase() + tab.slice(1)}
                  </button>
                ))}
                <div className="toolbar-group-right">
                  <span className="hb-badge" title="End-to-end sync latency">
                    ♥ 61ms
                  </span>
                  <button className="icon-btn" title="Refresh" aria-label="Refresh">
                    <svg viewBox="0 0 24 24" width="15" height="15" fill="currentColor" aria-hidden="true">
                      <path d="M17.65 6.35A7.958 7.958 0 0 0 12 4a8 8 0 1 0 7.75 10h-2.08A6 6 0 1 1 12 6c1.66 0 3.14.69 4.22 1.78L13 11h7V4l-2.35 2.35z" />
                    </svg>
                  </button>
                  {(activeTab === 'mutations' || activeTab === 'logs') && (
                    <button className="icon-btn" title="Clear" aria-label="Clear">
                      <svg viewBox="0 0 24 24" width="15" height="15" fill="currentColor" aria-hidden="true">
                        <path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zM4 12c0-4.42 3.58-8 8-8 1.85 0 3.55.63 4.9 1.69L5.69 16.9A7.902 7.902 0 0 1 4 12zm8 8c-1.85 0-3.55-.63-4.9-1.69L18.31 7.1A7.902 7.902 0 0 1 20 12c0 4.42-3.58 8-8 8z" />
                      </svg>
                    </button>
                  )}
                </div>
              </div>

              {/* Content Area */}
              <div
                className="content"
                onTouchStart={handleTouchStart}
                onTouchMove={handleTouchMove}
                onTouchEnd={handleTouchEnd}
                onPointerDown={handlePointerDown}
                onPointerMove={handlePointerMove}
                onPointerUp={handlePointerUp}
                style={{ touchAction: 'pan-y', cursor: isDragging ? 'grabbing' : 'grab' }}
              >
                {/* QUERIES TAB */}
                <div className={`tab-content ${activeTab === 'queries' ? 'active' : ''}`}>
                  <div className="queries-container">
                    <div className="queries-list">
                      <div className="queries-header">
                        <h2>Active Queries</h2>
                      </div>
                      <div className="queries-list-content">
                        {MOCK_QUERIES.map((q) => (
                          <div
                            key={q.queryHash}
                            className={`query-item ${selectedQueryHash === q.queryHash ? 'selected' : ''}`}
                            onClick={() => setSelectedQueryHash(q.queryHash)}
                          >
                            <div className="query-header">
                              <span className="query-hash">#{q.queryHash}</span>
                              <span className={`query-status status-${q.status}`}>{q.status}</span>
                            </div>
                            <div className="query-meta">
                              Updates: {q.updateCount} | Size: {q.dataSize}B
                            </div>
                            <div className="query-preview">{q.query.substring(0, 30)}...</div>
                          </div>
                        ))}
                      </div>
                    </div>

                    {selectedQuery ? (
                      <div className="query-detail">
                        <div className="detail-header">
                          <h3>Query #{selectedQuery.queryHash}</h3>
                          <span className={`query-status status-${selectedQuery.status}`}>
                            {selectedQuery.status}
                          </span>
                        </div>
                        <div className="detail-section">
                          <div className="detail-label">Created</div>
                          <div className="detail-value">
                            {new Date(selectedQuery.createdAt).toLocaleTimeString()}
                          </div>
                        </div>
                        <div className="detail-section">
                          <div className="detail-label">Update Count</div>
                          <div className="detail-value mono">{selectedQuery.updateCount}</div>
                        </div>
                        <div className="detail-section">
                          <div className="detail-label">Query</div>
                          <pre className="query-code">{selectedQuery.query}</pre>
                        </div>
                        <div className="detail-section">
                          <div className="detail-label">Variables</div>
                          <pre className="query-code">
                            {JSON.stringify(selectedQuery.variables, null, 2)}
                          </pre>
                        </div>
                      </div>
                    ) : (
                      <div className="query-detail"></div>
                    )}
                  </div>
                </div>

                {/* DATABASE TAB */}
                <div className={`tab-content ${activeTab === 'database' ? 'active' : ''}`}>
                  <div className="database-container">
                    <div className="database-tables">
                      <div className="pane-header">
                        <h2>Tables</h2>
                      </div>
                      <div className="tables-list">
                        {MOCK_TABLES.map((t) => (
                          <div
                            key={t.name}
                            className={`table-item ${t.name === 'thread' ? 'selected' : ''}`}
                          >
                            <span className="table-name">{t.name}</span>
                            <span className="table-count">{t.count}</span>
                          </div>
                        ))}
                      </div>
                    </div>
                    <div className="data-grid">
                      <table>
                        <thead>
                          <tr>
                            <th>id</th>
                            <th>title</th>
                            <th>author</th>
                            <th>created</th>
                          </tr>
                        </thead>
                        <tbody>
                          {MOCK_THREAD_DATA.map((row) => (
                            <tr key={row.id}>
                              <td className="text-primary">{row.id}</td>
                              <td className="text-string">"{row.title}"</td>
                              <td className="text-primary">{row.author}</td>
                              <td className="text-number">{row.created}</td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  </div>
                </div>

                {/* MUTATIONS TAB */}
                <div className={`tab-content ${activeTab === 'mutations' ? 'active' : ''}`}>
                  <div className="sim-filters">
                    <span className="filter-chip active">
                      All <span className="chip-count">6</span>
                    </span>
                    <span className="filter-chip">
                      Queued <span className="chip-count">2</span>
                    </span>
                    <span className="filter-chip">
                      Synced <span className="chip-count">3</span>
                    </span>
                    <span className="filter-chip danger">
                      Rolled back <span className="chip-count">1</span>
                    </span>
                    <span className="filter-chip danger">
                      Failed tray <span className="chip-count">1</span>
                    </span>
                  </div>
                  <div className="data-grid">
                    <table>
                      <thead>
                        <tr>
                          <th>Record</th>
                          <th>Op</th>
                          <th>Status</th>
                          <th className="num">Tries</th>
                          <th className="num">Took</th>
                        </tr>
                      </thead>
                      <tbody>
                        {MOCK_MUTATIONS.map((m) => (
                          <tr key={m.recordId + m.op}>
                            <td className="text-primary">{m.recordId}</td>
                            <td>
                              <span className={`op-badge op-${m.op}`}>{m.op}</span>
                            </td>
                            <td>
                              <span className={`status-pill pill-${m.status}`}>
                                <svg viewBox="0 0 24 24" aria-hidden="true">
                                  {STATUS_ICON[m.status]}
                                </svg>
                                {m.status}
                              </span>
                            </td>
                            <td className="num">{m.tries ?? ''}</td>
                            <td className="num">{m.tookMs ? `${m.tookMs}ms` : ''}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                  <div className="sim-statusbar">
                    <span>connected, this tab drains for all tabs</span>
                    <span>2 queued</span>
                    <span className="warn">1 in failed tray</span>
                  </div>
                </div>

                {/* LOGS TAB */}
                <div className={`tab-content ${activeTab === 'logs' ? 'active' : ''}`}>
                  <div className="sim-filters">
                    <span className="filter-chip active">
                      debug <span className="chip-count">4</span>
                    </span>
                    <span className="filter-chip active">
                      info <span className="chip-count">3</span>
                    </span>
                    <span className="filter-chip warn active">
                      warn <span className="chip-count">2</span>
                    </span>
                    <span className="filter-chip danger active">
                      error <span className="chip-count">1</span>
                    </span>
                  </div>
                  <div className="log-list">
                    {MOCK_LOGS.map((l) => (
                      <div key={l.time} className={`log-row log-${l.level}`}>
                        <span className="log-time">{l.time}</span>
                        <span className="log-level">{l.level}</span>
                        <span className="log-category">{l.category}</span>
                        <span className="log-msg">{l.msg}</span>
                      </div>
                    ))}
                  </div>
                  <div className="sim-statusbar">
                    <span>10 lines</span>
                    <span>recording debug+, console prints info+</span>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </>
  );
};

export default DevToolsSimulation;
