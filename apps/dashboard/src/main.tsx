import { render } from 'solid-js/web';
import './styles/fonts.css';
import './styles/theme.css';
import './styles/frame.css';

const root = document.getElementById('root');
if (!root) throw new Error('#root is missing from index.html');

// One codebase, two hosts: `vite build --mode console` is the Sp00ky Cloud
// console (workspaces, projects, a grant per project); the default build is
// the dashboard a scheduler serves at /admin. The other root is never loaded.
if (import.meta.env.MODE === 'console') {
  void import('./console/ConsoleApp').then(({ ConsoleApp }) => render(() => <ConsoleApp />, root));
} else {
  void import('./App').then(({ App }) => render(() => <App />, root));
}
