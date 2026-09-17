import type { ImpersonationInfo } from './impersonation';

/** Height the banner occupies. Also published as the CSS variable
 *  `--sp00ky-impersonation-banner-height` on `<html>` (`0px` when hidden), so
 *  an app can offset fixed headers. */
export const BANNER_HEIGHT_PX = 36;

const HOST_TAG = 'sp00ky-impersonation-banner';

// The shadow root keeps app CSS away from the bar, but the host element itself
// is in the page and matches the page's selectors (`div { display: none }`).
// An `!important` rule on `:host` wins over the page's own `!important`
// (inner tree context), which keeps the host rendered whatever the app says.
const STYLE = `
:host {
  all: initial !important;
  display: block !important;
  visibility: visible !important;
  opacity: 1 !important;
  position: static !important;
  transform: none !important;
  clip-path: none !important;
}
.bar {
  position: fixed; inset: 0 0 auto 0; height: ${BANNER_HEIGHT_PX}px; z-index: 2147483647;
  display: flex; align-items: center; justify-content: center; gap: 12px; padding: 0 12px;
  box-sizing: border-box; background: #b91c1c; color: #fff;
  font: 600 13px/1 system-ui, -apple-system, "Segoe UI", sans-serif;
  box-shadow: 0 1px 4px rgba(0,0,0,.35);
}
.text { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; min-width: 0; }
.text b { font-weight: 800; }
button {
  flex: none; cursor: pointer; border: 1px solid rgba(255,255,255,.8); border-radius: 6px;
  background: #fff; color: #b91c1c; font: 700 12px/1 system-ui, sans-serif; padding: 6px 12px;
}
button:disabled { opacity: .6; cursor: progress; }
`;

/**
 * The "you are impersonating" bar. Mounted by the client whenever the session
 * is an impersonation, in its own closed shadow root so app styles cannot hide
 * or restyle it. Every piece of text goes through `textContent`.
 */
export class ImpersonationBanner {
  private host: HTMLElement | null = null;
  private text: HTMLElement | null = null;

  constructor(private onStop: () => Promise<void>) {}

  static supported(): boolean {
    return typeof document !== 'undefined' && typeof document.createElement === 'function';
  }

  update(info: ImpersonationInfo | null): void {
    if (!ImpersonationBanner.supported()) return;
    if (!info) {
      this.unmount();
      return;
    }
    this.mount();
    const text = this.text!;
    text.replaceChildren(
      document.createTextNode('Impersonating '),
      bold(info.target),
      document.createTextNode(` as ${info.admin}`)
    );
    text.title = `Impersonating ${info.target} as ${info.admin}. Every write is audited.`;
  }

  unmount(): void {
    this.host?.remove();
    this.host = this.text = null;
    if (typeof document !== 'undefined') {
      document.documentElement?.style.setProperty('--sp00ky-impersonation-banner-height', '0px');
    }
  }

  private mount(): void {
    if (this.host?.isConnected) return;
    const host = document.createElement(HOST_TAG);
    const root = host.attachShadow({ mode: 'closed' });
    const style = document.createElement('style');
    style.textContent = STYLE;
    const bar = document.createElement('div');
    bar.className = 'bar';
    bar.setAttribute('role', 'alert');
    const text = document.createElement('span');
    text.className = 'text';
    const button = document.createElement('button');
    button.type = 'button';
    button.textContent = 'Stop impersonating';
    button.addEventListener('click', () => {
      button.disabled = true;
      this.onStop().finally(() => {
        button.disabled = false;
      });
    });
    bar.append(text, button);
    root.append(style, bar);
    (document.body ?? document.documentElement).appendChild(host);
    document.documentElement.style.setProperty('--sp00ky-impersonation-banner-height', `${BANNER_HEIGHT_PX}px`);
    this.host = host;
    this.text = text;
  }
}

function bold(value: string): HTMLElement {
  const b = document.createElement('b');
  b.textContent = value;
  return b;
}
