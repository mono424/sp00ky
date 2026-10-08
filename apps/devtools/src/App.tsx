import { Show } from 'solid-js';
import { DevToolsProvider, useDevTools } from './context/DevToolsContext';
import { useTheme } from './hooks/useTheme';
import { Tabs } from './components/Tabs';
import { QueriesTab } from './components/queries/QueriesTab';
import { TimingTab } from './components/timing/TimingTab';
import { DatabaseTab } from './components/database/DatabaseTab';
import { StorageTab } from './components/storage/StorageTab';
import { AccessTab } from './components/access/AccessTab';
import { VersionsTab } from './components/versions/VersionsTab';
import { McpTab } from './components/mcp/McpTab';
import { MutationsTab } from './components/mutations/MutationsTab';
import { LogsTab } from './components/logs/LogsTab';

function AppContent() {
  const { activeTab } = useDevTools();
  // Initialize theme syncing with Chrome DevTools
  useTheme();

  return (
    <>
      <Tabs />
      <div class="content">
        <div class="tab-content" classList={{ active: activeTab() === 'queries' }}>
          <Show when={activeTab() === 'queries'}>
            <QueriesTab />
          </Show>
        </div>

        <div class="tab-content" classList={{ active: activeTab() === 'mutations' }}>
          <Show when={activeTab() === 'mutations'}>
            <MutationsTab />
          </Show>
        </div>

        <div class="tab-content" classList={{ active: activeTab() === 'logs' }}>
          <Show when={activeTab() === 'logs'}>
            <LogsTab />
          </Show>
        </div>

        <div class="tab-content" classList={{ active: activeTab() === 'timing' }}>
          <Show when={activeTab() === 'timing'}>
            <TimingTab />
          </Show>
        </div>

        <div class="tab-content" classList={{ active: activeTab() === 'database' }}>
          <Show when={activeTab() === 'database'}>
            <DatabaseTab />
          </Show>
        </div>

        <div class="tab-content" classList={{ active: activeTab() === 'storage' }}>
          <Show when={activeTab() === 'storage'}>
            <StorageTab />
          </Show>
        </div>

        <div class="tab-content" classList={{ active: activeTab() === 'access' }}>
          <Show when={activeTab() === 'access'}>
            <AccessTab />
          </Show>
        </div>

        <div class="tab-content" classList={{ active: activeTab() === 'versions' }}>
          <Show when={activeTab() === 'versions'}>
            <VersionsTab />
          </Show>
        </div>

        <div class="tab-content" classList={{ active: activeTab() === 'mcp' }}>
          <Show when={activeTab() === 'mcp'}>
            <McpTab />
          </Show>
        </div>
      </div>
    </>
  );
}

export function App() {
  return (
    <DevToolsProvider>
      <AppContent />
    </DevToolsProvider>
  );
}
