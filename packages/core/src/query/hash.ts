/**
 * Hash inputs for the two query keys. Kept byte-identical to the previous
 * implementation (`DataModule.calculateHash` / `calculateMembershipKey`):
 * the salted key names the remote `_00_query` row, the unsalted one names the
 * durable `_00_view` row, and both are `sha256(JSON.stringify(input))`.
 */
export interface QueryKeyInput {
  surql: string;
  params: Record<string, unknown>;
}

/** Session-salted: two tabs of one user must not share a remote view. */
export const queryHashInput = (input: QueryKeyInput, sessionId: string | null): string =>
  JSON.stringify({ surql: input.surql, params: input.params, sessionId });

/** Session-independent: survives reload and bucket switch. */
export const viewKeyInput = (input: QueryKeyInput): string =>
  JSON.stringify({ surql: input.surql, params: input.params });
