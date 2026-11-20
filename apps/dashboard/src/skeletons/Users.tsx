import { For } from 'solid-js';
import { Sq } from '../components/Frame';
import { Sk, SkRepeat, vary } from '../components/Skeleton';

/** A bar on the line's top: centred, it can stretch a tight line by a pixel. */
const TOP = { 'vertical-align': 'top' } as const;

/** A row of four counted tiles, as Users and Feature flags open with. */
export function StatTilesSkeleton(props: { tiles: [label: string, sub: string | null][] }) {
  return (
    <section class="fx-row dx-stats">
      <For each={props.tiles}>
        {([label, sub], i) => (
          <div class="fx-cell">
            <div class="fx-head"><span class="cap">{label}</span></div>
            <div class="fx-big"><Sk w="1.2em" i={i()} /></div>
            <div class="fx-sub cap"><span>{sub ?? <Sk w="5em" i={i()} style={TOP} />}</span></div>
          </div>
        )}
      </For>
    </section>
  );
}

/** The filter strip over a list: the segment, the search box, Refresh. */
export function ListFiltersSkeleton(props: { segments: string[]; placeholder: string }) {
  return (
    <div class="inc-filters dx-user-filters">
      <div class="fx-range" role="group">
        <For each={props.segments}>{(label, i) => <button classList={{ on: i() === 0 }} tabIndex={-1}>{label}</button>}</For>
      </div>
      <div class="inc-find">
        <input type="search" placeholder={props.placeholder} aria-label="Search" disabled />
        <button class="btn btn-sm" tabIndex={-1}>Refresh</button>
      </div>
    </div>
  );
}

/** Users while the first page loads: the four tiles, the filters and the list. */
export function UsersSkeleton() {
  return (
    <div class="fx-page" aria-busy="true" aria-label="Loading">
      <section class="fx-row dx-stats">
        <For each={[['Users', null], ['Online now', 'with a live view'], ['Admins', 'on the roster']] as const}>
          {([label, sub], i) => (
            <div class="fx-cell">
              <div class="fx-head"><span class="cap">{label}</span></div>
              <div class="fx-big"><Sk w="1.2em" i={i()} /></div>
              <div class="fx-sub cap"><span>{sub ?? <Sk w="5em" style={TOP} />}</span></div>
            </div>
          )}
        </For>
        <div class="fx-cell">
          <div class="fx-head"><span class="cap">Sign-in</span></div>
          <div class="dx-source"><Sk w="5em" style={TOP} /></div>
          <div class="fx-sub cap"><span><Sk w="12em" style={TOP} /></span></div>
        </div>
      </section>

      <section class="fx-row">
        <div class="fx-cell">
          <ListFiltersSkeleton segments={['All', 'Online', 'Admins']} placeholder="Search users" />
          <table class="fx-list dx-users">
            <thead>
              <tr>
                <th class="cap">User</th>
                <th class="cap dx-col-wide"><Sk w="4em" style={TOP} /></th>
                <th class="cap dx-col-wide">Id</th>
                <th class="cap dx-col-mid">Joined</th>
                <th class="cap">Now</th>
              </tr>
            </thead>
            <tbody>
              <SkRepeat n={6}>
                {(i) => (
                  <tr class="inc-tr">
                    <td class="dx-who">
                      <span class="dx-avatar" aria-hidden="true" />
                      <span class="dx-who-name"><Sk w={vary(i, 40, 50)} i={i} /></span>
                    </td>
                    <td class="dx-col-wide"><Sk w={vary(i, 90, 50)} i={i} /></td>
                    <td class="dx-col-wide dx-dim"><Sk w={vary(i, 70, 40)} i={i} /></td>
                    <td class="dx-col-mid"><Sk w="4.5em" i={i} /></td>
                    <td><span class="fx-state cap"><Sq tone="idle" /><Sk w="3em" i={i} /></span></td>
                  </tr>
                )}
              </SkRepeat>
            </tbody>
          </table>
          <div class="dx-users-foot">
            <PagerSkeleton />
          </div>
        </div>
      </section>
    </div>
  );
}

/** The pager's shape: the range read out and the two arrows. */
export function PagerSkeleton() {
  return (
    <div class="dx-pager">
      <span class="cap"><Sk w="2.5em" /></span>
      <div class="fx-range">
        <button disabled aria-hidden="true">‹</button>
        <button disabled aria-hidden="true">›</button>
      </div>
    </div>
  );
}
