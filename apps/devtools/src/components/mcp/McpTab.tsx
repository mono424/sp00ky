import { Show } from 'solid-js';
import { useDevTools } from '../../context/DevToolsContext';

export function McpTab() {
  const { mcpStatus, setMcpEnabled } = useDevTools();

  return (
    <div class="mcp-container">
      <div class="mcp-header">
        <h2>MCP Bridge</h2>
        <div class="mcp-header-controls">
          <Show when={mcpStatus().enabled}>
            <div
              class="mcp-status-badge"
              classList={{ connected: mcpStatus().connected, disconnected: !mcpStatus().connected }}
            >
              <span
                class="status-dot"
                classList={{ active: mcpStatus().connected, inactive: !mcpStatus().connected }}
              />
              {mcpStatus().connected ? 'Connected' : 'Disconnected'}
            </div>
          </Show>
          <label class="mcp-toggle">
            <input
              type="checkbox"
              checked={mcpStatus().enabled}
              onChange={(e) => setMcpEnabled(e.currentTarget.checked)}
            />
            <span class="mcp-toggle-slider" />
          </label>
        </div>
      </div>

      <Show
        when={mcpStatus().enabled}
        fallback={
          <div class="mcp-disabled-notice">
            <h3>MCP is disabled</h3>
            <p>
              Enable MCP to allow AI assistants like Claude to interact with your Sp00ky application through DevTools. Toggle the switch above to start the bridge connection.
            </p>
          </div>
        }
      >
        <div class="mcp-section">
          <h3>What is MCP?</h3>
          <p>
            The <strong>Model Context Protocol (MCP)</strong> allows AI assistants like Claude to
            interact with your Sp00ky application directly through DevTools. When connected, AI can
            inspect state, run queries, browse tables, and debug issues in real-time.
          </p>
        </div>

        <div class="mcp-section">
          <h3>Available Tools</h3>
          <div class="mcp-tools-grid">
            <div class="mcp-tool">
              <div class="mcp-tool-name">get_state</div>
              <div class="mcp-tool-desc">Get the full DevTools state snapshot</div>
            </div>
            <div class="mcp-tool">
              <div class="mcp-tool-name">run_query</div>
              <div class="mcp-tool-desc">Execute SurrealQL against local or remote DB</div>
            </div>
            <div class="mcp-tool">
              <div class="mcp-tool-name">list_tables</div>
              <div class="mcp-tool-desc">List all database tables</div>
            </div>
            <div class="mcp-tool">
              <div class="mcp-tool-name">get_table_data</div>
              <div class="mcp-tool-desc">Fetch all records from a table</div>
            </div>
            <div class="mcp-tool">
              <div class="mcp-tool-name">get_active_queries</div>
              <div class="mcp-tool-desc">Inspect live queries and their data</div>
            </div>
            <div class="mcp-tool">
              <div class="mcp-tool-name">get_mutations</div>
              <div class="mcp-tool-desc">Outbox, write outcomes and the failed tray</div>
            </div>
            <div class="mcp-tool">
              <div class="mcp-tool-name">get_logs</div>
              <div class="mcp-tool-desc">The client's log lines, with a capture level</div>
            </div>
            <div class="mcp-tool">
              <div class="mcp-tool-name">get_auth_state</div>
              <div class="mcp-tool-desc">Check authentication status</div>
            </div>
            <div class="mcp-tool">
              <div class="mcp-tool-name">update_table_row</div>
              <div class="mcp-tool-desc">Modify a record in the database</div>
            </div>
            <div class="mcp-tool">
              <div class="mcp-tool-name">delete_table_row</div>
              <div class="mcp-tool-desc">Remove a record from the database</div>
            </div>
            <div class="mcp-tool">
              <div class="mcp-tool-name">list_connections</div>
              <div class="mcp-tool-desc">List connected browser tabs</div>
            </div>
          </div>
        </div>

        <div class="mcp-section">
          <h3>Setup</h3>
          <p>Add the MCP server to Claude Code:</p>
          <pre class="mcp-code">claude mcp add sp00ky-devtools node apps/devtools-mcp/dist/index.js</pre>
          <p class="mcp-hint">
            The MCP server runs a WebSocket bridge on{' '}
            <code>ws://127.0.0.1:{mcpStatus().port}</code>. This extension connects to it when
            enabled.
          </p>
        </div>

        <div class="mcp-section">
          <h3>Connection Details</h3>
          <div class="mcp-detail-grid">
            <div class="mcp-detail-label">Bridge Port</div>
            <div class="mcp-detail-value">
              <code>{mcpStatus().port}</code>
            </div>
            <div class="mcp-detail-label">Transport</div>
            <div class="mcp-detail-value">
              <code>stdio</code> (Claude Code &harr; MCP Server)
            </div>
            <div class="mcp-detail-label">Bridge Protocol</div>
            <div class="mcp-detail-value">
              <code>WebSocket</code> (MCP Server &harr; Extension)
            </div>
            <div class="mcp-detail-label">Status</div>
            <div class="mcp-detail-value">
              {mcpStatus().connected
                ? 'MCP server is running and connected'
                : 'Waiting for MCP server... Start a Claude Code session in the project directory.'}
            </div>
          </div>
        </div>
      </Show>
    </div>
  );
}
