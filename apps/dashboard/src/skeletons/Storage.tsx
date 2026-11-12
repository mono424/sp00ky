import { Sk, SkRepeat, vary } from '../components/Skeleton';

/** Storage while the bucket list loads: the buckets, then one bucket's bar, a column and its folder pane. */
export function StorageSkeleton(props: { bucket?: string }) {
  return (
    <div class="dx-page dx-split st-page" aria-busy="true" aria-label="Loading">
      <aside class="dx-side">
        <div class="dx-side-find dx-side-title">
          <span class="cap">Buckets</span>
          <button class="dx-iconbtn" tabIndex={-1} aria-hidden="true">↻</button>
        </div>
        <select class="dx-select dx-side-pick" aria-label="Bucket" disabled />
        <div class="dx-side-list">
          <SkRepeat n={3}>
            {(i) => (
              <div class="dx-tlink st-blink">
                <span class="dx-tname"><Sk w={vary(i, 50, 50)} i={i} /></span>
                <span class="st-backend"><Sk w="3em" i={i} /></span>
              </div>
            )}
          </SkRepeat>
        </div>
      </aside>
      <div class="dx-main">
        <div class="st-bucket">
          <div class="st-bar">
            <nav class="st-crumbs" aria-label="Path">
              <span class="st-crumb here">{props.bucket ?? <Sk w="7em" />}</span>
            </nav>
            <div class="st-tools">
              <div class="st-goto"><input placeholder="Go to folder" aria-label="Go to folder" disabled /></div>
              <input class="st-filter" type="search" placeholder="Filter" aria-label="Filter this folder" disabled />
              <button class="dx-iconbtn" tabIndex={-1} aria-hidden="true">↻</button>
              {/* Drawn as for a writable bucket, the usual case. */}
              <button class="btn btn-sm btn-primary" tabIndex={-1}>Upload</button>
            </div>
          </div>
          <div class="st-columns">
            <section class="st-col">
              <div class="st-skel" aria-hidden="true"><i /><i /><i /><i /></div>
            </section>
            <aside class="st-pane st-folder">
              {/* The folder glyph is the same for every bucket, so it is drawn for real. */}
              <div class="st-folder-mark">
                <svg class="st-glyph folder" width="15" height="15" viewBox="0 0 24 24" aria-hidden="true"><path fill="currentColor" d="M3 5h6.5l2 2.5H21V19H3z" /></svg>
              </div>
              <h2 class="st-title">{props.bucket ?? <Sk w="7em" />}</h2>
              <p class="st-holds"><Sk w="6em" /></p>
              <div class="st-pane-actions">
                <button class="btn btn-sm" tabIndex={-1}>Upload here</button>
                <span class="st-quiet">or drop files anywhere on this page</span>
              </div>
              <dl class="st-facts">
                <div><dt>Kept</dt><dd><Sk w="16em" /></dd></div>
              </dl>
            </aside>
          </div>
        </div>
      </div>
    </div>
  );
}
