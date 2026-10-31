import type { JSX } from 'solid-js';
import { render } from 'solid-js/web';
import { getToken } from './api/client';
import { FrameSkeleton } from './components/Shell';
import './styles/fonts.css';
import './styles/theme.css';
import './styles/frame.css';

const root = document.getElementById('root');
if (!root) throw new Error('#root is missing from index.html');

// One codebase, two hosts: `vite build --mode console` is the Sp00ky Cloud
// console (workspaces, projects, a grant per project); the default build is
// the dashboard a scheduler serves at /admin. The other root is never loaded.
//
// The frame paints first, from this chunk, while the app's own loads: the
// console always opens onto it, a scheduler's dashboard only when a session
// was kept (otherwise the sign-in page comes next).
const isConsole = import.meta.env.MODE === 'console';
const skeleton = isConsole || getToken() ? render(() => <FrameSkeleton console={isConsole} />, root) : undefined;
const mount = (App: () => JSX.Element) => {
  skeleton?.();
  render(() => <App />, root);
};
if (isConsole) {
  void import('./console/ConsoleApp').then(({ ConsoleApp }) => mount(ConsoleApp));
} else {
  void import('./App').then(({ App }) => mount(App));
}
