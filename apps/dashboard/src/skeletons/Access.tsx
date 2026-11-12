import { Sq } from '../components/Frame';
import { Sk, SkRepeat, vary } from '../components/Skeleton';

/** The tool list while `tools/list` answers: one group of rows under the filter bar. */
export function AccessToolsSkeleton() {
  return (
    <table class="fx-list ax-tools" aria-busy="true" aria-label="Loading">
      <colgroup><col style={{ width: '28%' }} /><col /><col style={{ width: '14%' }} /></colgroup>
      <tbody>
        <tr class="fx-gh"><th class="cap" colSpan={3}><span class="fx-gt"><Sk w="4em" /></span><Sk w="1em" /></th></tr>
        <SkRepeat n={8}>
          {(i) => (
            <tr>
              <td><Sk w={vary(i, 90, 70)} i={i} /></td>
              <td class="inc-sum"><Sk w={vary(i, 180, 220)} i={i} /></td>
              <td class="r"><span class="fx-state cap"><Sq tone="idle" /><Sk w="3em" i={i} /></span></td>
            </tr>
          )}
        </SkRepeat>
      </tbody>
    </table>
  );
}
