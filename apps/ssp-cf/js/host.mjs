// The ssp-cf Worker's main module: the project's SSP plus its Worker backends.
//
// A Worker backend is a sp00ky backend (OpenAPI spec, outbox jobs) whose code
// the control plane ships as a text module of this script. It runs as a
// Dynamic Worker loaded through the `LOADER` binding, so it has no script, no
// public URL and no credentials of its own:
//
//   - Jobs: the SSP's JobRunner (Rust, in the Durable Object) posts to
//     `worker://<name>/<path>`; `CfHttp` hands those to `__spkyBackends.fetch`
//     below instead of the network.
//   - `expose` paths: the project host's zone routes send those requests here,
//     and they go straight to the backend (an LLM stream passes through).
//   - Workers AI: `AiBinding` forwards `run()` to this script's `AI` binding,
//     because a Dynamic Worker's env only takes service bindings.
//   - Workflows: `DynamicWorkflow` is the class the Workflows engine runs; it
//     reloads the backend and runs its Workflow class (@cloudflare/dynamic-workflows).
//
// Everything else is the SSP's, exactly as when index.js was the main module.

import { WorkerEntrypoint, env as hostEnv, exports as hostExports } from "cloudflare:workers";
import {
	createDynamicWorkflowEntrypoint,
	DynamicWorkflowBinding,
	wrapWorkflowBinding,
} from "@cloudflare/dynamic-workflows";
import ssp from "./index.js";
import backends, { exposeHost } from "./backends.mjs";
import { bundleModules, envSecretName, exposedBackend } from "./routing.mjs";

export { SspNodeDo } from "./index.js";
// Required by wrapWorkflowBinding: the wrapped bindings are stubs of this class.
export { DynamicWorkflowBinding };

/** Workers AI for a backend: the same `run()` the AI binding has. */
export class AiBinding extends WorkerEntrypoint {
	run(...args) {
		return this.env.AI.run(...args);
	}
}

function backendEnv(name, backend) {
	let vars = {};
	const raw = hostEnv[envSecretName(name)];
	if (raw) {
		try {
			vars = JSON.parse(raw);
		} catch {
			console.error(JSON.stringify({ level: "ERROR", message: "backend env is not JSON", backend: name }));
		}
	}
	if (backend.ai) vars[backend.ai] = hostExports.AiBinding({ props: { backend: name } });
	for (const wf of backend.workflows ?? []) {
		vars[wf.binding] = wrapWorkflowBinding({ backend: name, binding: wf.binding }, { bindingName: wf.hostBinding });
	}
	return vars;
}

/** The backend's Dynamic Worker. The id changes with the bundle hash, so a deploy never reuses old code. */
function load(name) {
	const backend = backends[name];
	if (!backend) return null;
	return hostEnv.LOADER.get(backend.id, () => ({
		compatibilityDate: backend.compatibilityDate,
		compatibilityFlags: backend.compatibilityFlags ?? [],
		mainModule: "main.js",
		modules: bundleModules(backend.code),
		env: backendEnv(name, backend),
	}));
}

export const DynamicWorkflow = createDynamicWorkflowEntrypoint(({ metadata }) => {
	const backend = backends[metadata.backend];
	const wf = backend?.workflows?.find((w) => w.binding === metadata.binding);
	if (!wf) throw new Error(`no Workflow ${metadata.binding} on backend ${metadata.backend}`);
	return load(metadata.backend).getEntrypoint(wf.class);
});

// The JobRunner's way in (see `CfHttp` in src/lib.rs).
globalThis.__spkyBackends = {
	// Always a promise: the Rust side awaits it.
	async fetch(name, request) {
		const worker = load(name);
		if (!worker) return new Response(`no Worker backend named "${name}"`, { status: 404 });
		return worker.getEntrypoint().fetch(request);
	},
};

/** The SSP's own fetch handler, whichever shape worker-build gave it. */
function sspFetch(request, env, ctx) {
	if (typeof ssp === "function" && ssp.prototype?.fetch) return ssp.prototype.fetch.call({ env, ctx }, request);
	return ssp.fetch(request, env, ctx);
}

export default {
	fetch(request, env, ctx) {
		if (exposeHost) {
			const url = new URL(request.url);
			if (url.hostname === exposeHost) {
				const name = exposedBackend(backends, url.pathname);
				if (name) return load(name).getEntrypoint().fetch(request);
			}
		}
		return sspFetch(request, env, ctx);
	},
};
