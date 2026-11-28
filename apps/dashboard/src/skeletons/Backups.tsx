import { Sq } from '../components/Frame';
import { Sk, SkRepeat, vary } from '../components/Skeleton';
import { BASE } from './Ssps';

/** Backups while they load: where things stand, history and schedule, then the list. */
export function BackupsSkeleton() {
  return (
    <div class="fx-page" aria-busy="true" aria-label="Loading">
      <section class="fx-row" style={{ 'grid-template-columns': 'repeat(4, minmax(0, 1fr))' }}>
        <div class="fx-cell">
          <div class="fx-head"><span class="cap">Last backup</span></div>
          <div class="fx-big"><Sk w="3.5em" /></div>
          <div class="fx-sub cap"><span><b><Sk w="3.5em" /></b></span><span>seq <b><Sk w="4em" /></b></span></div>
        </div>
        <div class="fx-cell">
          <div class="fx-head"><span class="cap">Next backup</span></div>
          <div class="fx-big"><Sk w="3.5em" /></div>
          <div class="fx-sub cap"><span><Sk w="10em" /></span></div>
        </div>
        <div class="fx-cell">
          <div class="fx-head"><span class="cap">Kept</span></div>
          <div class="fx-big"><Sk w="1.2em" /><small>backups</small></div>
          <div class="fx-sub cap"><span><b><Sk w="3em" /></b></span><span>newest <b><Sk w="1.2em" /></b> kept</span></div>
        </div>
        <div class="fx-cell">
          <div class="fx-head"><span class="cap">Stored in</span><span class="fx-meta cap"><span><Sk w="7em" /></span></span></div>
          <div class="fx-big bu-where"><span><Sk w="3.5em" style={BASE} /></span><small><Sk w="2.5em" /></small></div>
          <div class="fx-sub cap bu-store"><span>bucket</span><b><Sk w="14em" /></b></div>
        </div>
      </section>

      <section class="fx-row" style={{ 'grid-template-columns': 'minmax(0, 2fr) minmax(0, 1fr)' }}>
        <div class="fx-cell">
          <div class="fx-head"><span class="cap">History</span><span class="fx-meta cap"><span>size of each backup, oldest left</span></span></div>
          {/* No height: the bars grow to the schedule beside them, as the real ones do. */}
          <Sk block class="bu-hist" />
          <div class="fx-axis cap"><Sk w="100%" /></div>
        </div>
        <div class="fx-cell">
          <div class="fx-head"><span class="cap">Schedule</span><span class="fx-meta cap"><span><Sk w="7em" /></span></span></div>
          <div class="bu-form">
            <label class="switch">
              <input type="checkbox" disabled />
              <span class="switch-track" />
              <span class="cap"><Sk w="1.5em" /></span>
            </label>
            <label class="li-field">
              <span>Cron, UTC</span>
              <input class="mono" disabled />
              <small class="cap"><Sk w="10em" /></small>
            </label>
            <label class="li-field">
              <span>Keep</span>
              <input disabled />
              <small class="cap">newest backups · empty or 0 keeps all</small>
            </label>
            <button class="btn btn-sm btn-primary bu-save" disabled>Save</button>
          </div>
        </div>
      </section>

      <section class="fx-row">
        <div class="fx-cell">
          <div class="fx-head"><span class="cap">Backups</span><span class="fx-meta cap"><span><Sk w="1.2em" /></span></span></div>
          <table class="fx-list bu-list">
            <colgroup>
              <col style={{ width: '22%' }} /><col style={{ width: '14%' }} /><col style={{ width: '11%' }} />
              <col style={{ width: '12%' }} /><col /><col style={{ width: '200px' }} />
            </colgroup>
            <thead>
              <tr>
                <th class="cap">Backup</th><th class="cap">Taken</th><th class="cap r">Size</th>
                <th class="cap r">Snapshot</th><th class="cap">State</th><th class="cap" />
              </tr>
            </thead>
            <tbody>
              <SkRepeat n={6}>
                {(i) => (
                  <tr>
                    <td>
                      <span class="fx-who"><Sq tone="idle" /><Sk w={vary(i, 50, 60)} i={i} /></span>
                      <span class="vw-meta cap"><Sk w="6em" i={i} /></span>
                    </td>
                    <td><Sk w="4em" i={i} /></td>
                    <td class="r"><Sk w="3.5em" i={i} /></td>
                    <td class="r"><Sk w="4em" i={i} /></td>
                    <td class="bu-state"><Sk w="5.5em" i={i} /></td>
                    {/* The actions only show on hover; the buttons keep the row their height. */}
                    <td class="r"><span class="bu-actions"><button class="btn btn-sm" disabled>Restore</button></span></td>
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
