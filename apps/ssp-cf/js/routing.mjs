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

/** The line `spky deploy` appends to a backend's bundle per wasm file it imports. */
const WASM_MODULE_LINE = "//# sp00ky-wasm-module ";

function base64Bytes(b64) {
	if (typeof Uint8Array.fromBase64 === "function") return Uint8Array.fromBase64(b64);
	const binary = atob(b64);
	const bytes = new Uint8Array(binary.length);
	for (let i = 0; i < binary.length; i++) bytes[i] = binary.charCodeAt(i);
	return bytes;
}

/**
 * A backend bundle as Worker Loader modules: `main.js`, plus a wasm module
 * per `//# sp00ky-wasm-module <name> <base64>` line after it (the CLI's
 * `append_wasm_modules`). The bundle imports them as `./<name>`, so it gets
 * a compiled `WebAssembly.Module`: a Worker may not compile wasm from bytes.
 */
export function bundleModules(code) {
	const modules = {};
	let end = code.length;
	while (end > 0) {
		const text = code.slice(0, end).trimEnd();
		const start = text.lastIndexOf("\n") + 1;
		const line = text.slice(start);
		if (!line.startsWith(WASM_MODULE_LINE)) break;
		const [name, b64] = line.slice(WASM_MODULE_LINE.length).split(" ");
		modules[name] = { wasm: base64Bytes(b64) };
		end = start;
	}
	return { "main.js": code.slice(0, end), ...modules };
}
