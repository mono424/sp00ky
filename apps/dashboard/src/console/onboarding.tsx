/**
 * The first project: connect a repository, read it, and hand over the commands
 * that get it running, ticking each one off as it runs. The console creates
 * nothing here: `spky deploy` already signs in, creates the project and opens
 * checkout, so this page only says what to run and watches for the result.
 *
 * Linking the repository so pushes deploy is a click, never a side effect of a
 * URL someone sent: the page's address names the repository.
 *
 * What it says comes from onboardingPlan.ts; this file fetches the facts and
 * draws them.
 */

import { For, Index, Match, Show, Switch, createEffect, createMemo, createSignal, onCleanup, type Accessor } from 'solid-js';
import { useSearchParams } from '@solidjs/router';
import { PageHead } from '../components/Chrome';
import { Terminal } from '../routes/Login';
import { cloud, consoleApi, type GitHubConnections, type GitHubInstallation, type Inspection, type Workspace, type WorkspaceProject } from './cloud';
import { useConsoleChrome, useGo } from './chrome';
import { commit, findings, isLive, plan, type Check, type Facts, type RepoLink, type Step, type StepState } from './onboardingPlan';

type Params = { repo?: string; installation?: string; github?: string; github_error?: string };

/** How often the page looks for what the terminal did. */
const WATCH_MS = 3000;

function message(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}

/** Run every so often while the tab is on screen, and at once on coming back to it. */
function whileVisible(run: (comingBack: boolean) => void | Promise<void>, ms: number) {
  const timer = setInterval(() => {
    if (!document.hidden) void run(false);
  }, ms);
  const onBack = () => {
    if (!document.hidden) void run(true);
  };
  window.addEventListener('focus', onBack);
  document.addEventListener('visibilitychange', onBack);
  onCleanup(() => {
    clearInterval(timer);
    window.removeEventListener('focus', onBack);
    document.removeEventListener('visibilitychange', onBack);
  });
}

/** The first stage's answer before one account has been picked. */
function accountsLine(c: GitHubConnections | undefined): string | undefined {
  const accounts = c?.installations ?? [];
  return accounts.length > 1 ? `${accounts.length} accounts` : accounts[0]?.account;
}

export function GitHubMark() {
  return (
    <svg width="17" height="17" viewBox="0 0 24 24" fill="currentColor" aria-hidden="true">
      <path d="M12 .3C5.4.3 0 5.7 0 12.3c0 5.3 3.4 9.8 8.2 11.4.6.1.8-.3.8-.6v-2c-3.3.7-4-1.6-4-1.6-.5-1.4-1.3-1.8-1.3-1.8-1.1-.7.1-.7.1-.7 1.2.1 1.8 1.2 1.8 1.2 1.1 1.8 2.8 1.3 3.5 1 .1-.8.4-1.3.8-1.6-2.7-.3-5.5-1.3-5.5-5.9 0-1.3.5-2.4 1.2-3.2-.1-.3-.5-1.5.1-3.2 0 0 1-.3 3.3 1.2a11.5 11.5 0 0 1 6 0c2.3-1.5 3.3-1.2 3.3-1.2.6 1.7.2 2.9.1 3.2.8.8 1.2 1.9 1.2 3.2 0 4.6-2.8 5.6-5.5 5.9.4.4.8 1.1.8 2.2v3.3c0 .3.2.7.8.6A12 12 0 0 0 24 12.3C24 5.7 18.6.3 12 .3z" />
    </svg>
  );
}

export function Onboarding(props: { workspace: Workspace }) {
  const [params, setParams] = useSearchParams<Params>();
  // Only the accounts count as news (each answer also carries a new install
  // link, which has an hour to live), and a poll that fails keeps what it had.
  const [connections, setConnections] = createSignal<GitHubConnections | undefined>(undefined, {
    equals: (a, b) => JSON.stringify(a?.installations) === JSON.stringify(b?.installations),
  });
  const [failure, setFailure] = createSignal<string | null>(null);
  const [installUrl, setInstallUrl] = createSignal<string | undefined>();
  const load = async () => {
    try {
      const c = await consoleApi.github(props.workspace.id, window.location.pathname);
      setConnections(c);
      setInstallUrl(c.install_url);
      setFailure(null);
    } catch (err) {
      setFailure(message(err));
    }
  };
  void load();

  // What GitHub's redirect back said, taken off the address so a reload does
  // not say it again.
  const backFromGitHub = { error: params.github_error, requested: params.github === 'requested' };
  if (params.github || params.github_error) setParams({ github: undefined, github_error: undefined }, { replace: true });

  const stage = () => {
    if (params.repo === 'none' || (params.repo && params.installation)) return 'run';
    const c = connections();
    if (!c) return failure() ? 'error' : 'loading';
    return c.installations.length ? 'pick' : 'connect';
  };
  // The app was installed in another tab, or from GitHub's own pages. Picking,
  // the list only changes after a trip to GitHub, so that is when to look.
  whileVisible((comingBack) => {
    if (stage() === 'connect' || (stage() === 'pick' && comingBack)) void load();
  }, WATCH_MS);

  const installation = () => connections()?.installations.find((i) => String(i.id) === params.installation);

  return (
    <>
      <PageHead title="New project" />
      <div class="fx-page ob">
        <Stages
          stage={stage}
          account={installation()?.account ?? accountsLine(connections())}
          repo={params.repo && params.repo !== 'none' ? params.repo : undefined}
          scratch={params.repo === 'none'}
          onRepo={() => setParams({ repo: undefined, installation: undefined })}
        />
        <Switch>
          <Match when={stage() === 'loading'}>
            <section class="fx-row">
              <div class="fx-cell">
                <div class="ob-reading">Looking up your GitHub accounts<span class="li-term-caret" /></div>
              </div>
            </section>
          </Match>
          <Match when={stage() === 'error'}>
            <section class="fx-row">
              <div class="fx-cell">
                <p class="cx-reason">{failure()}</p>
                <div class="cx-actions">
                  <button class="btn btn-sm" onClick={() => void load()}>Try again</button>
                </div>
              </div>
            </section>
          </Match>
          <Match when={stage() === 'connect'}>
            <Connect connections={connections()!} installUrl={installUrl()} problem={backFromGitHub.error} requested={backFromGitHub.requested} onScratch={() => setParams({ repo: 'none' })} />
          </Match>
          <Match when={stage() === 'pick'}>
            <Pick
              connections={connections()!}
              installUrl={installUrl()}
              problem={backFromGitHub.error}
              onPick={(i, repo) => setParams({ repo, installation: String(i.id) })}
              onScratch={() => setParams({ repo: 'none' })}
            />
          </Match>
          <Match when={stage() === 'run'}>
            <Run
              workspace={props.workspace}
              repo={params.repo === 'none' ? null : params.repo!}
              installation={params.installation}
              onBack={() => setParams({ repo: undefined, installation: undefined })}
            />
          </Match>
        </Switch>
      </div>
    </>
  );
}

/* ------------------------------------------------------------------ */
/* Where in the three stages this is                                    */
/* ------------------------------------------------------------------ */

function Stages(props: { stage: Accessor<string>; account?: string; repo?: string; scratch: boolean; onRepo: () => void }) {
  const at = () => ({ loading: 0, error: 0, connect: 0, pick: 1, run: 2 })[props.stage()] ?? 0;
  const state = (i: number): StepState => (at() > i ? 'done' : at() === i ? 'active' : 'waiting');
  return (
    <section class="fx-row ob-stages" aria-label="Progress">
      <div class={`fx-cell ob-stage ${state(0)}`}>
        <span class="cap">1 GitHub</span>
        <b>{props.scratch ? 'Skipped' : at() > 0 ? props.account ?? 'Connected' : 'Connect your account'}</b>
      </div>
      <div class={`fx-cell ob-stage ${state(1)}`}>
        <span class="cap">2 Repository</span>
        <Show when={at() > 1} fallback={<b>Pick what to deploy</b>}>
          <button class="ob-stage-back" onClick={() => props.onRepo()} title="Use another repository">
            {props.repo ?? 'Starting from scratch'}
          </button>
        </Show>
      </div>
      <div class={`fx-cell ob-stage ${state(2)}`}>
        <span class="cap">3 Commands</span>
        <b>Run them, watch them land</b>
      </div>
    </section>
  );
}

/* ------------------------------------------------------------------ */
/* Connect                                                              */
/* ------------------------------------------------------------------ */

function Connect(props: { connections: GitHubConnections; installUrl?: string; problem?: string; requested: boolean; onScratch: () => void }) {
  return (
    <section class="fx-row ob-hero">
      <div class="fx-cell ob-hero-text">
        <Show
          when={!props.requested}
          fallback={
            <>
              <h1 class="ob-title">Waiting for an owner</h1>
              <p class="ob-lede">GitHub asked your organization's owners to approve the app. Once one of them does, they can connect it from their console and invite you to the workspace.</p>
            </>
          }
        >
          <h1 class="ob-title">Connect a repository</h1>
          <p class="ob-lede">Sp00ky reads it and gives you the commands that get it running, tailored to what it finds. Read-only, and only the repositories you choose.</p>
        </Show>
        <Show when={props.problem}>
          <div class="fx-log bad ob-problem" role="alert"><span class="fx-what">{props.problem}</span></div>
        </Show>
        <div class="ob-actions">
          <Show when={props.connections.configured && props.installUrl} fallback={<p class="cx-note">GitHub is not set up on this cloud.</p>}>
            <a class="btn btn-primary ob-cta" href={props.installUrl}>
              <GitHubMark />
              Install the GitHub app
            </a>
          </Show>
          <button class="link-btn" onClick={() => props.onScratch()}>Start without a repository</button>
        </div>
      </div>
      <Terminal />
    </section>
  );
}

/* ------------------------------------------------------------------ */
/* Pick a repository                                                    */
/* ------------------------------------------------------------------ */

const SHOWN_REPOS = 40;

function pushedAgo(at: string | undefined): string {
  if (!at) return '';
  const days = Math.floor((Date.now() - new Date(at).getTime()) / 86_400_000);
  if (days < 1) return 'today';
  if (days < 30) return `${days}d ago`;
  if (days < 365) return `${Math.floor(days / 30)}mo ago`;
  return `${Math.floor(days / 365)}y ago`;
}

function Pick(props: { connections: GitHubConnections; installUrl?: string; problem?: string; onPick: (i: GitHubInstallation, repo: string) => void; onScratch: () => void }) {
  const [activeId, setActiveId] = createSignal<number | undefined>();
  const [q, setQ] = createSignal('');
  const [hot, setHot] = createSignal(0);
  const active = () => props.connections.installations.find((i) => i.id === activeId()) ?? props.connections.installations[0];
  const matches = createMemo(() => {
    const needle = q().trim().toLowerCase();
    const repos = active()?.repos ?? [];
    return needle ? repos.filter((r) => r.full_name.toLowerCase().includes(needle)) : repos;
  });
  const onKey = (e: KeyboardEvent) => {
    const count = Math.min(matches().length, SHOWN_REPOS);
    if (e.key === 'ArrowDown' || e.key === 'ArrowUp') {
      e.preventDefault();
      if (count) setHot((hot() + (e.key === 'ArrowDown' ? 1 : count - 1)) % count);
    } else if (e.key === 'Enter' && matches()[hot()]) {
      props.onPick(active()!, matches()[hot()].full_name);
    }
  };
  return (
    <section class="fx-row">
      <div class="fx-cell ob-pick">
        <h1 class="ob-title ob-title-sm">Pick a repository</h1>
        <Show when={props.problem}>
          <div class="fx-log bad ob-problem" role="alert"><span class="fx-what">{props.problem}</span></div>
        </Show>
        <div class="ob-pick-bar">
          <div class="ob-accounts" role="tablist" aria-label="GitHub accounts">
            <For each={props.connections.installations}>
              {(i) => (
                <button
                  class="ob-account"
                  classList={{ on: i.id === active()?.id }}
                  role="tab"
                  aria-selected={i.id === active()?.id}
                  onClick={() => {
                    setActiveId(i.id);
                    setHot(0);
                  }}
                >
                  <Show when={i.avatar_url}><img src={`${i.avatar_url}${i.avatar_url!.includes('?') ? '&' : '?'}s=48`} alt="" /></Show>
                  {i.account || `Installation ${i.id}`}
                </button>
              )}
            </For>
            <Show when={props.installUrl}>
              <a class="ob-account ob-account-add" href={props.installUrl}>Add an account</a>
            </Show>
          </div>
          <label class="cx-find cap ob-find">
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" aria-hidden="true">
              <path d="M10.5 4a6.5 6.5 0 1 0 0 13 6.5 6.5 0 0 0 0-13z M20 20l-4.5-4.5" />
            </svg>
            <input
              placeholder="Find a repository"
              value={q()}
              onInput={(e) => {
                setQ(e.currentTarget.value);
                setHot(0);
              }}
              onKeyDown={onKey}
              autofocus
            />
          </label>
        </div>
        <Show when={active()?.error}>
          <div class="fx-log bad ob-problem" role="alert"><span class="fx-what">{active()!.error}</span></div>
        </Show>
        <div class="ob-repos" role="listbox" aria-label="Repositories">
          <For
            each={matches().slice(0, SHOWN_REPOS)}
            fallback={
              <p class="cx-note ob-repos-empty">
                {q() ? `Nothing on ${active()?.account} matches.` : `The app cannot see any repository on ${active()?.account} yet.`}
              </p>
            }
          >
            {(r, i) => (
              <button class="ob-repo" classList={{ hot: hot() === i() }} role="option" aria-selected={hot() === i()} onMouseEnter={() => setHot(i())} onClick={() => props.onPick(active()!, r.full_name)}>
                <span class="ob-repo-name">
                  <span class="dim">{r.owner}/</span>
                  <b>{r.name}</b>
                  <Show when={r.private}><span class="cx-chip">private</span></Show>
                </span>
                <span class="ob-repo-desc">{r.description}</span>
                <span class="ob-repo-lang">{r.language}</span>
                <span class="ob-repo-when">{pushedAgo(r.pushed_at)}</span>
              </button>
            )}
          </For>
        </div>
        <div class="ob-pick-foot">
          <Show when={matches().length > SHOWN_REPOS}>
            <span class="cap">{SHOWN_REPOS} of {matches().length}, newest first</span>
          </Show>
          <Show when={active()?.selection === 'selected' && active()?.manage_url}>
            <a class="link-btn" href={active()!.manage_url} target="_blank" rel="noreferrer">Choose which repositories the app can see <span aria-hidden="true">↗</span></a>
          </Show>
          <button class="link-btn" onClick={() => props.onScratch()}>Start without a repository</button>
        </div>
      </div>
    </section>
  );
}

/* ------------------------------------------------------------------ */
/* Read it, say what to run, watch it land                              */
/* ------------------------------------------------------------------ */

function Run(props: { workspace: Workspace; repo: string | null; installation?: string; onBack: () => void }) {
  const cx = useConsoleChrome()!;
  const go = useGo();

  // Anything that appears after this moment is the project being waited for.
  const before = new Set(cx.workspaces().flatMap((w) => w.projects.map((p) => p.id)));

  const [now, setNow] = createSignal(Date.now());
  const [me, setMe] = createSignal<Awaited<ReturnType<typeof consoleApi.me>> | undefined>();
  // Polls answer with new objects; only a new commit is news.
  const [inspection, setInspection] = createSignal<Inspection | undefined>(undefined, {
    equals: (a, b) => a?.sha === b?.sha && a?.repo === b?.repo && !!a === !!b,
  });
  const [problem, setProblem] = createSignal<string | null>(null);

  const readRepo = async () => {
    if (!props.repo || !props.installation) return;
    try {
      setInspection(await consoleApi.inspect(props.workspace.id, props.installation, props.repo));
      setProblem(null);
    } catch (err) {
      setProblem(message(err));
    }
  };
  const readMe = () => consoleApi.me().then(setMe, () => undefined);
  void readRepo();
  void readMe();

  const target = createMemo(() => {
    const all = cx.workspaces().flatMap((w) => w.projects.map((p) => ({ w, p })));
    const slug = inspection()?.config?.slug;
    return (slug ? all.find((x) => x.p.slug === slug) : undefined) ?? all.find((x) => !before.has(x.p.id));
  });
  const project = () => target()?.p;

  /* -- whether pushes deploy: read once the project is paid for, changed only by a click -- */
  const [link, setLink] = createSignal<RepoLink>({ state: 'unknown' });
  let looked: string | undefined;
  createEffect(() => {
    const t = target();
    if (!t || !props.repo || t.p.status === 'pending_payment' || looked === t.p.id) return;
    looked = t.p.id;
    cloud<{ status: string; repo_owner?: string; repo_name?: string; branch?: string }>(linkPath(t.p), { tenant: t.w.id }).then(
      (l) => setLink(l.status === 'linked' ? { state: 'linked', repo: `${l.repo_owner}/${l.repo_name}`, branch: l.branch ?? '' } : { state: 'none' }),
      (err) => setLink({ state: 'failed', error: message(err) }),
    );
  });
  const connectRepo = async () => {
    const t = target();
    const ins = inspection();
    if (!t || !ins || !props.installation) return;
    setLink({ state: 'working' });
    const [repo_owner, repo_name] = ins.repo.split('/');
    try {
      await cloud(linkPath(t.p), {
        method: 'PATCH',
        tenant: t.w.id,
        body: { repo_owner, repo_name, branch: ins.branch, config_path: ins.config?.path ?? 'sp00ky.yml', installation_id: Number(props.installation) },
      });
      setLink({ state: 'linked', repo: ins.repo, branch: ins.branch });
    } catch (err) {
      setLink({ state: 'failed', error: message(err) });
    }
  };

  const facts = createMemo<Facts>(() => ({
    login: me()?.account.github_login,
    cliLoginAt: me()?.account.cli_login_at,
    apiUrl: me()?.api_url,
    inspection: props.repo ? inspection() : null,
    project: project(),
    workspaceType: props.workspace.type,
    link: link(),
    now: now(),
  }));
  const steps = createMemo(() => plan(facts()));
  const live = createMemo(() => isLive(project()));

  // One round at a time: on a slow connection each would otherwise overtake the last.
  let watching = false;
  whileVisible(async () => {
    setNow(Date.now());
    if (watching) return;
    watching = true;
    await Promise.allSettled([
      facts().cliLoginAt ? undefined : readMe(),
      props.repo && !inspection()?.config?.valid ? readRepo() : undefined,
      live() ? undefined : cx.refresh(),
    ]);
    watching = false;
  }, WATCH_MS);

  const open = () => {
    const t = target();
    if (t) go(`/${t.w.slug}/${t.p.slug}`);
  };

  return (
    <section class="fx-row ob-run">
      <div class="fx-cell ob-found">
        <Switch>
          <Match when={!props.repo}>
            <Scratch />
          </Match>
          <Match when={inspection()}>
            {(ins) => <Found inspection={ins()} />}
          </Match>
          <Match when={problem()}>
            <div class="fx-log bad ob-problem" role="alert"><span class="fx-what">{problem()}</span></div>
            <div class="cx-actions"><button class="btn btn-sm" onClick={() => void readRepo()}>Try again</button></div>
          </Match>
          <Match when={true}>
            <div class="ob-reading">Reading {props.repo}<span class="li-term-caret" /></div>
          </Match>
        </Switch>
        <button class="link-btn ob-other" onClick={() => props.onBack()}>
          {props.repo ? 'Use another repository' : 'Connect a repository instead'}
        </button>
      </div>
      <div class="fx-cell ob-sheet">
        <Show
          when={live()}
          fallback={
            <div class="fx-head">
              <span class="cap">Run these</span>
              <span class="fx-meta cap"><span class="fx-sq ob-pulse" /> Watching</span>
            </div>
          }
        >
          <div class="ob-live">
            <h2 class="ob-title"><span class="iri-text">{project()!.slug}</span> is live</h2>
            <button class="btn btn-primary" onClick={open}>Open {project()!.slug}</button>
          </div>
        </Show>
        <ol class="ob-steps">
          <Index each={steps()}>
            {(step, i) => <StepRow step={step()} index={i} onConnect={() => void connectRepo()} />}
          </Index>
        </ol>
        <Show when={project() && !live()}>
          <div class="ob-sheet-foot">
            <button class="link-btn" onClick={open}>Open {project()!.slug} while this runs</button>
          </div>
        </Show>
      </div>
    </section>
  );
}

const linkPath = (p: WorkspaceProject) => `/projects/${encodeURIComponent(p.id)}/link`;

function Scratch() {
  return (
    <>
      <h1 class="ob-title ob-title-sm">Starting from scratch</h1>
      <p class="ob-lede">spky init writes a schema, an app wired to it and a local stack. Connect a repository whenever you like, and pushes deploy from then on.</p>
    </>
  );
}

function Found(props: { inspection: Inspection }) {
  // A new object per commit; the reveal runs when there is something new to show.
  const list = createMemo(() => findings(props.inspection));
  return (
    <>
      <h1 class="ob-title ob-title-sm ob-repo-title">
        <Show when={props.inspection.url} fallback={props.inspection.repo}>
          <a href={props.inspection.url} target="_blank" rel="noreferrer">{props.inspection.repo}</a>
        </Show>
      </h1>
      <p class="ob-repo-meta">
        {props.inspection.branch}
        <Show when={props.inspection.sha}> at <span class="cx-sha">{commit(props.inspection.sha)}</span></Show>
        <Show when={props.inspection.private}> <span class="cx-chip">private</span></Show>
      </p>
      <ul class="ob-findings">
        <For each={list()}>
          {(f, i) => (
            <li class={`ob-finding ${f.tone}`} style={{ '--i': i() }}>
              <span class="ob-finding-mark" aria-hidden="true" />
              <span>
                <Show when={f.href} fallback={<b>{f.title}</b>}>
                  <a href={f.href} target="_blank" rel="noreferrer"><b>{f.title}</b> <span aria-hidden="true">↗</span></a>
                </Show>
                <Show when={f.detail}><small>{f.detail}</small></Show>
              </span>
            </li>
          )}
        </For>
      </ul>
    </>
  );
}

function StepRow(props: { step: Step; index: number; onConnect: () => void }) {
  return (
    <li class={`ob-step ${props.step.state}`}>
      <div class="ob-step-head">
        <span class="ob-step-mark">
          <Show when={props.step.state === 'done'} fallback={props.index + 1}>
            <svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="3" aria-hidden="true"><path d="M4 12.5l5.5 5.5L20 6.5" /></svg>
          </Show>
        </span>
        <b>{props.step.title}</b>
        <Show when={props.step.state === 'done'}>
          <span class="ob-step-result">{props.step.result}</span>
        </Show>
      </div>
      <Show when={props.step.state !== 'done'}>
        <Index each={props.step.blocks}>{(block) => <Script lines={block()} />}</Index>
        <Show when={props.step.state === 'active'}>
          <div class="ob-watching">{props.step.watching}<span class="li-term-caret" /></div>
        </Show>
      </Show>
      <Show when={props.step.checks && props.step.state !== 'waiting'}>
        <ul class="ob-checks">
          <Index each={props.step.checks!}>{(c) => <CheckRow check={c()} onConnect={props.onConnect} />}</Index>
        </ul>
      </Show>
    </li>
  );
}

function CheckRow(props: { check: Check; onConnect: () => void }) {
  return (
    <li class={`ob-check ${props.check.state}`}>
      <span class="ob-check-mark" aria-hidden="true" />
      <b>{props.check.label}</b>
      <span>{props.check.detail}</span>
      <Show when={props.check.action === 'connect'}>
        <button class="btn btn-sm" onClick={() => props.onConnect()}>{props.check.state === 'bad' ? 'Try again' : 'Connect'}</button>
      </Show>
    </li>
  );
}

/** A block of commands, as a terminal would show them, with its own copy button. */
function Script(props: { lines: string[] }) {
  const [copied, setCopied] = createSignal(false);
  let reset: ReturnType<typeof setTimeout> | undefined;
  onCleanup(() => clearTimeout(reset));
  const copy = async () => {
    const commands = props.lines.filter((l) => !l.startsWith('# '));
    await navigator.clipboard.writeText(commands.join('\n'));
    setCopied(true);
    clearTimeout(reset);
    reset = setTimeout(() => setCopied(false), 1600);
  };
  return (
    <div class="ob-script">
      <button class="btn btn-sm ob-copy" onClick={() => void copy()}>{copied() ? 'Copied' : 'Copy'}</button>
      <Index each={props.lines}>
        {(line) => (
          <Show when={!line().startsWith('# ')} fallback={<div class="ob-note">{line()}</div>}>
            <div class="ob-line"><span class="ob-prompt" aria-hidden="true">$</span>{line()}</div>
          </Show>
        )}
      </Index>
    </div>
  );
}
