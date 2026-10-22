import { test } from "node:test";
import assert from "node:assert/strict";
import { envSecretName, exposedBackend, routeMatches } from "./routing.mjs";

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
