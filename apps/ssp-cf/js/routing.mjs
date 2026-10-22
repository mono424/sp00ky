// Pure helpers of host.mjs, kept apart so they test without the Workers runtime.

/** The secret holding a backend's env as JSON (vault plus `env` in sp00ky.yml). */
export function envSecretName(name) {
	return "SPKY_BACKEND_ENV_" + name.toUpperCase().replace(/[^A-Z0-9]/g, "_");
}

/** Whether a route pattern (`/mcp`, `/llm/*`, `/.well-known/oauth*`) covers a path. */
export function routeMatches(pattern, pathname) {
	return pattern.endsWith("*") ? pathname.startsWith(pattern.slice(0, -1)) : pathname === pattern;
}

/** The backend that exposes `pathname` publicly, if any. */
export function exposedBackend(table, pathname) {
	for (const [name, backend] of Object.entries(table)) {
		if ((backend.expose ?? []).some((pattern) => routeMatches(pattern, pathname))) return name;
	}
	return null;
}
