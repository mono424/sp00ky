import { render } from 'solid-js/web';
import { App } from './App';
import './styles/fonts.css';
import './styles/theme.css';
import './styles/frame.css';

const root = document.getElementById('root');
if (!root) throw new Error('#root is missing from index.html');

render(() => <App />, root);
