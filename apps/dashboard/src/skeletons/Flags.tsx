import { Sq } from '../components/Frame';
import { Sk, SkRepeat, vary } from '../components/Skeleton';
import { ListFiltersSkeleton, StatTilesSkeleton } from './Users';

/** Feature flags while they load: the four counts, the filters and the list. */
export function FlagsSkeleton() {
  return (
    <div class="fx-page" aria-busy="true" aria-label="Loading">
      <StatTilesSkeleton
        tiles={[
          ['Flags', 'defined'],
          ['On', 'evaluating their rules'],
          ['Rolling out', 'to part of the users'],
          ['Pinned', 'user pins across flags'],
        ]}
      />
      <section class="fx-row">
        <div class="fx-cell">
          <ListFiltersSkeleton segments={['All', 'On', 'Off']} placeholder="Search key or description" />
          <table class="fx-list dx-users ff-list">
            <thead>
              <tr>
                <th class="cap">Flag</th>
                <th class="cap">State</th>
                <th class="cap dx-col-wide">Variants</th>
                <th class="cap">Targeting</th>
                <th class="cap dx-col-mid">Changed</th>
              </tr>
            </thead>
            <tbody>
              <SkRepeat n={4}>
                {(i) => (
                  <tr class="inc-tr">
                    <td class="ff-key">
                      <span class="dx-who-name"><Sk w={vary(i, 60, 40)} i={i} /></span>
                      <span class="ff-desc"><Sk w={vary(i, 120, 80)} i={i} /></span>
                    </td>
                    <td><span class="fx-state cap"><Sq tone="idle" /><Sk w="1.5em" i={i} /></span></td>
                    <td class="dx-col-wide">
                      <span class="ff-variants">
                        <span class="dx-tag"><Sk w="1.5em" i={i} /></span>
                        <span class="dx-tag"><Sk w="1.5em" i={i} /></span>
                      </span>
                    </td>
                    <td class="dx-dim"><Sk w={vary(i, 80, 60)} i={i} /></td>
                    <td class="dx-col-mid"><Sk w="4em" i={i} /></td>
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
