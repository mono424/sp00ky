// Placeholder for a project without Worker backends. On deploy the control
// plane uploads its own `backends.mjs` beside `host.mjs`: one entry per
// backend, `{ id, code, compatibilityDate, compatibilityFlags, ai,
// workflows: [{ binding, class, hostBinding, workflowName }], expose }`, with
// `code` imported from the backend's text module, and `exposeHost` set to the
// project's public host.
export const exposeHost = null;
export default {};
