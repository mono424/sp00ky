import { test } from "node:test";
import assert from "node:assert/strict";
import { bundleModules, envSecretName, exposedBackend, routeMatches } from "./routing.mjs";

test("route patterns match exactly or by prefix", () => {
	assert.ok(routeMatches("/mcp", "/mcp"));
	assert.ok(!routeMatches("/mcp", "/mcp/x"));
	assert.ok(routeMatches("/llm/*", "/llm/chat"));
	assert.ok(!routeMatches("/llm/*", "/llm"), "a /* pattern needs the slash");
	assert.ok(routeMatches("/.well-known/oauth*", "/.well-known/oauth-authorization-server"));
});

test("an exposed path names its backend, a job path does not", () => {
	const table = { api: { expose: ["/llm/*", "/health"] }, mail: {} };
	assert.equal(exposedBackend(table, "/llm/stream"), "api");
	assert.equal(exposedBackend(table, "/health"), "api");
	assert.equal(exposedBackend(table, "/ingest"), null);
});

test("a backend's env secret name is uppercased and safe", () => {
	assert.equal(envSecretName("api"), "SPKY_BACKEND_ENV_API");
	assert.equal(envSecretName("game-sync"), "SPKY_BACKEND_ENV_GAME_SYNC");
});

test("a bundle without wasm is main.js alone", () => {
	const code = 'export default {};\n// a trailing comment\n';
	assert.deepEqual(bundleModules(code), { "main.js": code });
});

test("wasm lines after the bundle become wasm modules", () => {
	const b64 = (s) => Buffer.from(s).toString("base64");
	const js = 'import a from "./aa-a.wasm";\nimport b from "./bb-b.wasm";\nexport default {};';
	const code = `${js}\n//# sp00ky-wasm-module aa-a.wasm ${b64("\0asm-a")}\n//# sp00ky-wasm-module bb-b.wasm ${b64("\0asm-b")}\n`;
	const modules = bundleModules(code);
	assert.deepEqual(Object.keys(modules).sort(), ["aa-a.wasm", "bb-b.wasm", "main.js"]);
	assert.equal(modules["main.js"].trimEnd(), js);
	assert.equal(Buffer.from(modules["aa-a.wasm"].wasm).toString(), "\0asm-a");
	assert.equal(Buffer.from(modules["bb-b.wasm"].wasm).toString(), "\0asm-b");
});
