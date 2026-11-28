import { For, type JSX } from 'solid-js';
import { Sq } from '../components/Frame';
import { Sk, SkRepeat, vary } from '../components/Skeleton';

/*
 * A workspace's pages while they load: each is the page body under its head,
 * the same rows and tables with the words that never change left in. Forms
 * are drawn disabled with their real labels so they keep their size.
 */

/**
 * For bars in small text: top-aligned, a bar no longer stretches the line box
 * by the px or two a middle-aligned one adds.
 */
const top = { 'vertical-align': 'top' } as const;

/** A cell's head: the label and, when given, the meta on the right. */
function Head(props: { label: string; meta?: JSX.Element }) {
  return (
    <div class="fx-head">
      <span class="cap">{props.label}</span>
      {props.meta ? <span class="fx-meta cap">{props.meta}</span> : null}
    </div>
  );
}

/** A table's header row from its column names; `r` marks right-aligned ones. */
function Cols(props: { cols: string[] }) {
  return (
    <thead>
      <tr>
        <For each={props.cols}>{(c) => <th class={c.startsWith('r:') ? 'cap r' : 'cap'}>{c.replace(/^r:/, '')}</th>}</For>
      </tr>
    </thead>
  );
}

/**
 * Projects, for a cold load before the workspace list is in: the page itself
 * has its projects on arrival and never waits.
 */
export function ProjectsSkeleton() {
  return (
    <div class="fx-page" aria-busy="true" aria-label="Loading">
      <section class="fx-row cx-cards">
        <SkRepeat n={4}>
          {(i) => (
            <div class="fx-cell cx-card">
              <div class="cx-card-top">
                <Sq tone="idle" />
                <b><Sk w={vary(i, 80, 60)} i={i} style={top} /></b>
                <span class="cx-tag"><Sk w="1.4em" i={i} style={top} /></span>
              </div>
              <span class="cap"><Sk w={vary(i, 110, 50)} i={i} style={top} /></span>
            </div>
          )}
        </SkRepeat>
      </section>
    </div>
  );
}

/** Members, and for an admin the invite form and pending invitations. */
export function MembersSkeleton(props: { admin: boolean }) {
  return (
    <div class="fx-page" aria-busy="true" aria-label="Loading">
      <section class="fx-row">
        <div class="fx-cell">
          <Head label="Members" meta={<Sk w="1em" />} />
          <table class="fx-list cx-members">
            <Cols cols={['Who', 'Role', 'Since', '']} />
            <tbody>
              <SkRepeat n={3}>
                {(i) => (
                  <tr>
                    <td class="cx-who"><b><Sk w={vary(i, 70, 50)} i={i} /></b> <span class="dim"><Sk w={vary(i, 90, 40)} i={i} /></span></td>
                    <td><span class="cx-chip dim"><Sk w="3.5em" i={i} /></span></td>
                    <td><Sk w="5em" i={i} /></td>
                    <td class="r">{props.admin ? <button class="btn btn-sm" disabled>Remove</button> : null}</td>
                  </tr>
                )}
              </SkRepeat>
            </tbody>
          </table>
        </div>
      </section>
      {props.admin ? (
        <section class="fx-row" style={{ 'grid-template-columns': 'minmax(0, 1fr) minmax(0, 1fr)' }}>
          <div class="fx-cell">
            <Head label="Invite" meta={<Sk w="12em" />} />
            <div class="cx-form">
              <label class="li-field"><span>Email</span><input type="email" placeholder="someone@example.com" disabled /></label>
              <div class="fx-range">
                <button type="button" class="on" disabled>Member</button>
                <button type="button" disabled>Admin</button>
              </div>
              <button class="btn btn-primary" type="button" disabled>Invite</button>
            </div>
          </div>
          <div class="fx-cell">
            <Head label="Pending invitations" meta={<Sk w="1em" />} />
            <div class="cx-line">
              <span class="cx-line-main"><b><Sk w="8em" /></b><span class="cap dim"><Sk w="16em" /></span></span>
              <button class="btn btn-sm" disabled>Revoke</button>
            </div>
          </div>
        </section>
      ) : null}
    </div>
  );
}

/** Billing & usage: the usage table and the note under it. */
export function BillingSkeleton() {
  return (
    <div class="fx-page" aria-busy="true" aria-label="Loading">
      <section class="fx-row">
        <div class="fx-cell">
          <Head label="Usage this period" meta={<span>per project</span>} />
          <table class="fx-list">
            <Cols cols={['Project', 'Metric', 'r:Total']} />
            <tbody>
              <SkRepeat n={3}>
                {(i) => (
                  <tr>
                    <td>{i === 0 ? <b><Sk w="6em" i={i} /></b> : ''}</td>
                    <td><Sk w={vary(i, 80, 60)} i={i} /></td>
                    <td class="r"><Sk w="4em" i={i} /></td>
                  </tr>
                )}
              </SkRepeat>
            </tbody>
          </table>
        </div>
      </section>
      <section class="fx-row">
        <div class="fx-cell"><p class="cx-note">Plan changes and spend caps: `spky cloud billing` for now; invoices and payment methods open in Stripe above.</p></div>
      </section>
    </div>
  );
}

/** Activity: the recent events table. */
export function ActivitySkeleton() {
  return (
    <div class="fx-page" aria-busy="true" aria-label="Loading">
      <section class="fx-row">
        <div class="fx-cell">
          <Head label="Recent" meta={<Sk w="1em" />} />
          <table class="fx-list cx-activity">
            <Cols cols={['When', 'Who', 'What', 'Project', 'From']} />
            <tbody>
              <SkRepeat n={4}>
                {(i) => (
                  <tr>
                    <td><Sk w="5em" i={i} /></td>
                    <td><b><Sk w={vary(i, 50, 40)} i={i} /></b></td>
                    <td><Sk w={vary(i, 140, 60)} i={i} /></td>
                    <td><Sk w={vary(i, 70, 50)} i={i} /></td>
                    <td class="dim"><Sk w="7em" i={i} /></td>
                  </tr>
                )}
              </SkRepeat>
            </tbody>
          </table>
        </div>
      </section>
    </div>
  );
}

/** Sessions: the console sessions table. */
export function SessionsSkeleton() {
  return (
    <div class="fx-page" aria-busy="true" aria-label="Loading">
      <section class="fx-row">
        <div class="fx-cell">
          <Head label="Console sessions" />
          <table class="fx-list cx-sessions">
            <Cols cols={['Browser', 'From', 'Signed in', 'Last seen', '']} />
            <tbody>
              <SkRepeat n={3}>
                {(i) => (
                  <tr>
                    <td class="cx-who"><b><Sk w={vary(i, 90, 40)} i={i} /></b></td>
                    <td class="dim"><Sk w="7em" i={i} /></td>
                    <td><Sk w="5em" i={i} /></td>
                    <td><Sk w="5em" i={i} /></td>
                    <td class="r"><button class="btn btn-sm" disabled>Sign out</button></td>
                  </tr>
                )}
              </SkRepeat>
            </tbody>
          </table>
        </div>
      </section>
    </div>
  );
}

/** Vault as a locked one reads: its state, push deploys and the secrets by project. */
export function VaultSkeleton(props: { projects?: number }) {
  return (
    <div class="fx-page" aria-busy="true" aria-label="Loading">
      <section class="fx-row cx-vault-top">
        <div class="fx-cell">
          <Head label="State" meta={<Sk w="5em" />} />
          <div class="cx-state"><Sq tone="idle" /><span class="fx-big cx-mid"><Sk w="4em" /></span></div>
          {/* Two lines of text; top-aligned bars keep each line at the text's height. */}
          <p class="cx-note"><Sk w="90%" style={top} /><br /><Sk w="40%" style={top} /></p>
          <div class="cx-unlock">
            <input type="password" placeholder="Vault passphrase" disabled aria-label="Vault passphrase" />
            <button class="btn btn-sm btn-primary" type="button" disabled>Unlock</button>
          </div>
          <p class="cx-note">The passphrase never leaves this page: it is stretched here (argon2id) and only the result is sent.</p>
        </div>
        <div class="fx-cell">
          <Head label="Push deploys" meta={<Sk w="1.5em" i={1} />} />
          <div class="cx-state"><Sq tone="idle" /><span class="fx-big cx-mid"><Sk w="7em" i={1} /></span></div>
          <p class="cx-note">Lets git-linked builds read production values without a passphrase. The CLI never needs this.</p>
          <div class="cx-actions"><button class="btn btn-sm" disabled>Turn on</button></div>
        </div>
      </section>
      <section class="fx-row">
        <div class="fx-cell">
          <Head label="Secrets by project" meta={<Sk w="5em" />} />
          <table class="fx-list cx-secrets">
            <Cols cols={['Project', 'r:Variables', 'r:Development', 'r:Production', '']} />
            <tbody>
              <SkRepeat n={props.projects ?? 3}>
                {(i) => (
                  <tr>
                    <td><Sq tone="idle" /> <b><Sk w={vary(i, 80, 60)} i={i} /></b></td>
                    <td class="r"><Sk w="1.5em" i={i} /></td>
                    <td class="r dim"><Sk w="1.5em" i={i} /></td>
                    <td class="r dim"><Sk w="1.5em" i={i} /></td>
                    <td class="r"><button class="btn btn-sm" disabled>Open</button></td>
                  </tr>
                )}
              </SkRepeat>
            </tbody>
          </table>
        </div>
      </section>
    </div>
  );
}

/** API keys and MCP tokens: two lists, the token form above its list. */
export function KeysSkeleton() {
  const line = (i: number, w: number) => (
    <div class="cx-line">
      <span class="cx-line-main"><b><Sk w={vary(i, w, 40)} i={i} /></b><span class="cap dim"><Sk w="12em" i={i} /></span></span>
      <button class="btn btn-sm" disabled>Revoke</button>
    </div>
  );
  return (
    <div class="fx-page" aria-busy="true" aria-label="Loading">
      <section class="fx-row" style={{ 'grid-template-columns': 'minmax(0, 1fr) minmax(0, 1fr)' }}>
        <div class="fx-cell">
          <Head label="API keys" meta={<button class="btn btn-sm btn-primary" disabled>New key</button>} />
          <SkRepeat n={2}>{(i) => line(i, 90)}</SkRepeat>
          <p class="cx-note">For CI and scripts (`SPOOKY_API_KEY`). A key acts as you in every workspace you belong to.</p>
        </div>
        <div class="fx-cell">
          <Head label="MCP tokens" meta={<Sk w="1em" />} />
          <div class="cx-form">
            <label class="li-field"><span>Name</span><input placeholder="claude on my laptop" disabled /></label>
            <button class="btn btn-primary" type="button" disabled>New token</button>
          </div>
          <SkRepeat n={2}>{(i) => line(i, 120)}</SkRepeat>
        </div>
      </section>
    </div>
  );
}

/**
 * Workspace settings, for a cold load before the workspace list is in: the
 * page itself has all but the creation date on arrival.
 */
export function WorkspaceSettingsSkeleton() {
  return (
    <div class="fx-page" aria-busy="true" aria-label="Loading">
      <section class="fx-row">
        <div class="fx-cell">
          <Head label="General" meta={<Sk w="3em" style={top} />} />
          <div class="cx-form">
            <label class="li-field cx-grow"><span>Name</span><input disabled /></label>
            <button class="btn btn-primary" type="button" disabled>Save</button>
          </div>
          <p class="cx-note"><Sk w="30em" style={top} /></p>
          <div class="cx-kv">
            <For each={['Address', 'Your role', 'Projects', 'Created']}>
              {(label, i) => <div><span class="cap">{label}</span><b><Sk w="5em" i={i()} style={top} /></b></div>}
            </For>
          </div>
        </div>
      </section>
      <section class="fx-row">
        <div class="fx-cell">
          <Head label="Workspace ID" />
          <div class="cx-secret cx-idbox"><code><Sk w="18em" style={top} /></code><button class="btn btn-sm" disabled>Copy</button></div>
          <p class="cx-note">Send it as <code>X-Tenant-ID</code> to act in this workspace through the cloud API.</p>
        </div>
      </section>
      <section class="fx-row cx-manage-row">
        <For
          each={[
            ['Members', 'Who is in this workspace, their roles and pending invitations.'],
            ['Billing & usage', 'The plan, invoices and what each project used.'],
            ['Vault', "The passphrase that encrypts every project's variables."],
            ['API keys', 'Tokens for the CLI, CI and the cloud API.'],
          ]}
        >
          {([label, what], i) => (
            <div class="fx-cell cx-card cx-manage">
              <div class="cx-card-top"><Sk block w={16} h={16} i={i()} /><b>{label}</b></div>
              <span class="cx-note">{what}</span>
            </div>
          )}
        </For>
      </section>
    </div>
  );
}
