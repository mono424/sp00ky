// Shared message types for MCP Server <-> Chrome Extension bridge (JSON-RPC 2.0 style)

export interface BridgeRequest {
  jsonrpc: '2.0';
  id: string;
  method: string;
  params: Record<string, unknown>;
  tabId?: number;
}

export interface BridgeResponse {
  jsonrpc: '2.0';
  id: string;
  result?: unknown;
  error?: { code: number; message: string };
}

export interface BridgeNotification {
  jsonrpc: '2.0';
  method: string;
  params: Record<string, unknown>;
}

export type BridgeMessage = BridgeRequest | BridgeResponse | BridgeNotification;

// Methods the MCP server can call on the extension
export const BRIDGE_METHODS = {
  GET_STATE: 'getState',
  RUN_QUERY: 'runQuery',
  GET_TABLE_DATA: 'getTableData',
  GET_QUERY_ROWS: 'getQueryRows',
  UPDATE_TABLE_ROW: 'updateTableRow',
  DELETE_TABLE_ROW: 'deleteTableRow',
  CLEAR_HISTORY: 'clearHistory',
  /** `{ method: 'mutationOp' | 'logOp', op, args }`: the Mutations / Logs dispatchers. */
  PAGE_OP: 'pageOp',
} as const;

export const BRIDGE_PORT = 9315;

export function isBridgeResponse(msg: unknown): msg is BridgeResponse {
  return (
    typeof msg === 'object' &&
    msg !== null &&
    'jsonrpc' in msg &&
    (msg as any).jsonrpc === '2.0' &&
    'id' in msg &&
    ('result' in msg || 'error' in msg)
  );
}

export function isBridgeRequest(msg: unknown): msg is BridgeRequest {
  return (
    typeof msg === 'object' &&
    msg !== null &&
    'jsonrpc' in msg &&
    (msg as any).jsonrpc === '2.0' &&
    'method' in msg &&
    'id' in msg &&
    !('result' in msg) &&
    !('error' in msg)
  );
}

export function isBridgeNotification(msg: unknown): msg is BridgeNotification {
  return (
    typeof msg === 'object' &&
    msg !== null &&
    'jsonrpc' in msg &&
    (msg as any).jsonrpc === '2.0' &&
    'method' in msg &&
    !('id' in msg)
  );
}
