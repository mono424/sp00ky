import { For } from 'solid-js';
import { Sk, SkRepeat, vary } from '../components/Skeleton';

/** The table browser while the table list loads: the column of names, then one table. */
export function DatabaseSkeleton(props: { table?: string }) {
  return (
    <div class="dx-page dx-split" aria-busy="true" aria-label="Loading">
      <aside class="dx-side">
        <div class="dx-side-find">
          <input type="search" placeholder="Find a table" aria-label="Find a table" disabled />
          <button class="dx-iconbtn" tabIndex={-1} aria-hidden="true">↻</button>
        </div>
        <select class="dx-select dx-side-pick" aria-label="Table" disabled />
        <div class="dx-side-list">
          <div class="dx-side-head cap"><span>Tables</span><Sk w="1em" /></div>
          <SkRepeat n={5}>
            {(i) => (
              <div class="dx-tlink">
                <span class="dx-tname"><Sk w={vary(i, 40, 50)} i={i} /></span>
              </div>
            )}
          </SkRepeat>
        </div>
      </aside>
      <div class="dx-main">
        <TableSkeleton name={props.table} />
      </div>
    </div>
  );
}

/** One table before anything about it has answered; the name is known once a link is open. */
export function TableSkeleton(props: { name?: string }) {
  return (
    <div class="dx-table" aria-busy="true" aria-label="Loading">
      <div class="dx-bar">
        <div class="dx-bar-title">
          <b>{props.name ?? <Sk w="5em" />}</b>
          <span class="cap"><Sk w="7em" /></span>
        </div>
        <div class="fx-range" role="tablist" aria-label="Table view">
          <button class="on" tabIndex={-1}>Data</button>
          <button tabIndex={-1}>Structure</button>
        </div>
        <div class="dx-bar-end">
          <button class="dx-iconbtn" tabIndex={-1} aria-hidden="true">↻</button>
        </div>
      </div>
      <TableDataSkeleton />
    </div>
  );
}

/** The Data tab's grid while its first page of rows loads: filter bar, grid and foot. */
export function TableDataSkeleton() {
  return (
    <>
      <div class="dx-filters">
        <button class="btn btn-sm" tabIndex={-1}>Filter</button>
      </div>
      <div class="dx-grid-wrap">
        <table class="dx-grid">
          <thead>
            <tr>
              <th class="dx-check"><input type="checkbox" disabled aria-hidden="true" /></th>
              <For each={[30, 70, 60, 70, 40, 80]}>
                {(w) => (
                  <th>
                    <button class="dx-th" tabIndex={-1}><span><Sk w={w} /></span></button>
                  </th>
                )}
              </For>
            </tr>
          </thead>
          <tbody>
            <SkRepeat n={8}>
              {(i) => (
                <tr>
                  <td class="dx-check"><input type="checkbox" disabled aria-hidden="true" /></td>
                  <For each={[70, 50, 100, 60, 30, 70]}>{(w, c) => <td><Sk w={vary(i + c(), w, 30)} i={i} /></td>}</For>
                </tr>
              )}
            </SkRepeat>
          </tbody>
        </table>
      </div>
      <div class="dx-foot">
        <div class="dx-count cap"><Sk w="6em" /></div>
        <div class="dx-pager">
          <span class="cap"><Sk w="2.5em" /></span>
          <div class="fx-range">
            <button disabled aria-hidden="true">‹</button>
            <button disabled aria-hidden="true">›</button>
          </div>
          <select class="dx-select" aria-label="Rows per page" disabled>
            <option>50 rows</option>
          </select>
        </div>
      </div>
    </>
  );
}
