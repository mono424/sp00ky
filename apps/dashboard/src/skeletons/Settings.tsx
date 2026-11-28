import { For, type JSX } from 'solid-js';
import { Sk, SkRepeat, vary } from '../components/Skeleton';

/**
 * Bars sit at the top of their line: in small text a middle-aligned bar
 * stretches the line box by a px or two, and the row with it.
 */
const top = { 'vertical-align': 'top' } as const;

/*
 * A project's Settings tabs while they load. Each is the tab's content, the
 * rows that sit in the page under the tab bar; the forms are drawn disabled
 * with their real labels, so they keep their size. Machines and Danger zone
 * have none: Danger zone is static, and Machines keeps its loading line.
 */

const busy = { 'aria-busy': 'true', 'aria-label': 'Loading' } as const;

/** A cell's head: the label and, when given, the meta on the right. */
function Head(props: { label: string; meta?: JSX.Element }) {
  return (
    <div class="fx-head">
      <span class="cap">{props.label}</span>
      {props.meta ? <span class="fx-meta cap">{props.meta}</span> : null}
    </div>
  );
}

/** Environment: the vault bar, the variables and the form to set one. */
export function SettingsEnvSkeleton() {
  return (
    <>
      <section class="fx-row" {...busy}>
        <div class="fx-cell">
          <div class="cx-unlock">
            <span class="fx-what"><Sk w="28em" style={top} /></span>
            <input type="password" placeholder="Vault passphrase" disabled aria-label="Vault passphrase" />
            <button class="btn btn-sm btn-primary" type="button" disabled>Unlock</button>
          </div>
        </div>
      </section>
      <section class="fx-row" {...busy}>
        <div class="fx-cell">
          <Head label="Variables" meta={<Sk w="1em" style={top} />} />
          <table class="fx-list cx-env">
            <thead><tr><th class="cap">Name</th><th class="cap">Development</th><th class="cap">Production</th><th class="cap">Updated</th><th class="cap" /></tr></thead>
            <tbody>
              <SkRepeat n={3}>
                {(i) => (
                  <tr>
                    <td class="cx-env-name"><b><Sk w={vary(i, 100, 60)} i={i} style={top} /></b></td>
                    <td data-label="dev"><Sk w="5em" i={i} style={top} /></td>
                    <td data-label="prod"><Sk w="5em" i={i} style={top} /></td>
                    <td><Sk w="5em" i={i} style={top} /></td>
                    <td class="r cx-acts">
                      <button class="btn btn-sm" disabled>Edit</button>
                      <button class="btn btn-sm" disabled>Delete</button>
                    </td>
                  </tr>
                )}
              </SkRepeat>
            </tbody>
          </table>
        </div>
      </section>
      <section class="fx-row" {...busy}>
        <div class="fx-cell">
          <Head label="Set a variable" meta={<span>encrypted with the workspace key</span>} />
          <div class="cx-form cx-setvar">
            <label class="li-field"><span>Name</span><input placeholder="STRIPE_SECRET_KEY" disabled /></label>
            <label class="li-field"><span>Value</span><input type="password" disabled /></label>
            <div class="fx-range">
              <For each={['Both', 'Production', 'Development']}>
                {(label, i) => <button type="button" classList={{ on: i() === 0 }} disabled>{label}</button>}
              </For>
            </div>
            <button class="btn btn-primary" type="button" disabled>Save</button>
          </div>
        </div>
      </section>
    </>
  );
}

/** Domains: the custom domains and the form to add one. */
export function SettingsDomainsSkeleton() {
  return (
    <>
      <section class="fx-row" {...busy}>
        <div class="fx-cell">
          <Head label="Custom domains" />
          <table class="fx-list cx-domains">
            <thead><tr><th class="cap">Domain</th><th class="cap">Serves</th><th class="cap">Status</th><th class="cap">DNS</th><th class="cap" /></tr></thead>
            <tbody>
              <SkRepeat n={2}>
                {(i) => (
                  <tr>
                    <td><b><Sk w={vary(i, 100, 70)} i={i} style={top} /></b></td>
                    <td><Sk w={vary(i, 70, 40)} i={i} style={top} /></td>
                    <td><span class="cx-chip dim"><Sk w="3.5em" i={i} style={top} /></span></td>
                    <td class="cx-dns"><div><Sk w="14em" i={i} style={top} /></div></td>
                    <td class="r"><button class="btn btn-sm" disabled>Remove</button></td>
                  </tr>
                )}
              </SkRepeat>
            </tbody>
          </table>
        </div>
      </section>
      <section class="fx-row" {...busy}>
        <div class="fx-cell">
          <Head label="Add a domain" />
          <div class="cx-form">
            <label class="li-field"><span>Domain</span><input placeholder="app.example.com" disabled /></label>
            <label class="li-field"><span>App (optional)</span><input placeholder="web" disabled /></label>
            <button class="btn btn-primary" type="button" disabled>Add</button>
          </div>
        </div>
      </section>
    </>
  );
}

/** Git & builds as a linked project shows it: the link's four facts and the builds. */
export function SettingsGitSkeleton() {
  return (
    <>
      <section class="fx-row cx-strip" style={{ 'grid-template-columns': 'repeat(4, minmax(0, 1fr))' }} {...busy}>
        <div class="fx-cell">
          <Head label="Repository" meta={<button class="btn btn-sm" disabled>Change</button>} />
          <div class="fx-big cx-mid cx-ellipsis"><Sk w="9em" style={top} /></div>
        </div>
        <For each={['Branch', 'Manifest']}>
          {(label, i) => (
            <div class="fx-cell">
              <Head label={label} />
              <div class="fx-big cx-mid cx-ellipsis"><Sk w="5em" i={i() + 1} style={top} /></div>
            </div>
          )}
        </For>
        <div class="fx-cell">
          <Head label="On push" />
          <div class="cx-row">
            <span class="fx-big cx-mid"><Sk w="3.5em" i={3} style={top} /></span>
            <button class="btn btn-sm" disabled>Turn off</button>
          </div>
        </div>
      </section>
      <section class="fx-row" {...busy}>
        <div class="fx-cell">
          <Head label="Builds" meta={<button class="btn btn-sm btn-primary" disabled>Build now</button>} />
          <table class="fx-list cx-builds">
            <thead><tr><th class="cap">Commit</th><th class="cap">Author</th><th class="cap">Status</th><th class="cap">Started</th></tr></thead>
            <tbody>
              <SkRepeat n={3}>
                {(i) => (
                  <tr>
                    <td><Sk w={vary(i, 220, 120)} i={i} style={top} /></td>
                    <td><Sk w="5em" i={i} style={top} /></td>
                    <td><span class="cx-chip dim"><Sk w="4em" i={i} style={top} /></span></td>
                    <td><Sk w="5em" i={i} style={top} /></td>
                  </tr>
                )}
              </SkRepeat>
            </tbody>
          </table>
        </div>
      </section>
    </>
  );
}

/** Scale: the SSP count, once the project says how many run. */
export function SettingsScaleSkeleton() {
  return (
    <section class="fx-row" {...busy}>
      <div class="fx-cell">
        <Head label="Sync processors" meta={<Sk w="3.5em" style={top} />} />
        <div class="cx-form">
          <label class="li-field"><span>SSPs</span><input type="number" disabled /></label>
          <button class="btn btn-primary" type="button" disabled>Scale</button>
        </div>
        <p class="cx-note">Starter runs one SSP, Pro up to three. More SSPs spread views and survive one failing.</p>
      </div>
    </section>
  );
}

/** Database: the version row; an offered upgrade or a past one appear with the data. */
export function SettingsDatabaseSkeleton() {
  return (
    <section class="fx-row" {...busy}>
      <div class="fx-cell">
        <Head label="Version" meta={<Sk w="5em" style={top} />} />
        <div class="cx-stat cx-upver"><Sk w="3.2em" style={top} /></div>
      </div>
    </section>
  );
}
