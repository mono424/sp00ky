import type { ImpersonationInfo } from './impersonation';

/** Height of the warning layer the page is shifted down by. Also published as
 *  the CSS variable `--sp00ky-impersonation-banner-height` on `<html>` (`0px`
 *  when hidden), so an app can offset its own fixed header. */
export const BANNER_HEIGHT_PX = 44;

/** Radius of the page's new top corners. */
const PAGE_RADIUS_PX = 14;

const HOST_TAG = 'sp00ky-impersonation-banner';

/** Hazard stripes: the warning layer, and the `<html>` backdrop the page's
 *  rounded corners reveal. */
const STRIPES = 'repeating-linear-gradient(45deg, #f2b600 0 14px, #1c1403 14px 28px)';

/** Keeps only the area OUTSIDE a corner's curve, so the nub is striped and
 *  everything inside the curve stays the app's own. `center` is the curve's
 *  centre inside the corner square. */
function cornerMask(center: string): string {
  return `radial-gradient(circle ${PAGE_RADIUS_PX}px at ${center}, transparent 0 ${PAGE_RADIUS_PX}px, #000 ${PAGE_RADIUS_PX}px)`;
}

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
.layer {
  position: fixed; inset: 0 0 auto 0; z-index: 2147483646; pointer-events: none;
  height: ${BANNER_HEIGHT_PX}px; background: ${STRIPES};
}
/* The page's rounded top corners, drawn over whatever the app puts there.
   The body element's own border-radius rounds its background, but a fixed app
   header is not clipped by it, so each corner is also painted here: a striped
   square masked down to the nub OUTSIDE the corner's curve, so the app's own
   colour still shows inside it. */
.corners {
  position: fixed; left: 0; right: 0; top: ${BANNER_HEIGHT_PX}px;
  height: ${PAGE_RADIUS_PX}px; z-index: 2147483646; pointer-events: none;
}
.corner {
  position: absolute; top: 0; width: ${PAGE_RADIUS_PX}px; height: ${PAGE_RADIUS_PX}px;
  background: ${STRIPES};
}
.corner.left {
  left: 0;
  -webkit-mask: ${cornerMask('100% 100%')};
  mask: ${cornerMask('100% 100%')};
}
.corner.right {
  right: 0;
  -webkit-mask: ${cornerMask('0% 100%')};
  mask: ${cornerMask('0% 100%')};
}
.bar {
  position: fixed; inset: 0 0 auto 0; height: ${BANNER_HEIGHT_PX}px; z-index: 2147483647;
  display: flex; align-items: center; justify-content: center; gap: 10px;
  padding: 0 12px; box-sizing: border-box;
  font: 600 13px/1 system-ui, -apple-system, "Segoe UI", sans-serif;
}
.pill {
  display: flex; align-items: center; gap: 8px; min-width: 0; max-width: 100%;
  padding: 6px 10px; border-radius: 999px;
  background: rgba(14, 11, 2, .88); color: #fde68a;
  box-shadow: 0 1px 6px rgba(0, 0, 0, .45);
}
.sign { flex: none; font-size: 14px; line-height: 1; }
.text { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; min-width: 0; }
.text b { color: #fff; font-weight: 800; }
button {
  flex: none; cursor: pointer; border: 0; border-radius: 999px;
  background: #fde68a; color: #1c1403;
  font: 800 12px/1 system-ui, sans-serif; padding: 7px 12px;
  box-shadow: 0 1px 6px rgba(0, 0, 0, .45);
}
button:hover { background: #fff; }
button:disabled { opacity: .6; cursor: progress; }
@media (max-width: 520px) {
  .bar { gap: 6px; padding: 0 8px; }
  .pill { padding: 5px 8px; }
}
`;

/**
 * The "you are impersonating" warning.
 *
 * Drawn as a layer the page sits on top of: hazard stripes fill the top of the
 * viewport, the page is pushed down by the bar's height and its new top
 * corners are rounded, so the stripes show through around them. The bar itself
 * lives in a closed shadow root, so app styles cannot hide or restyle it; the
 * page-shifting styles are set inline and `!important` on `<html>` / `<body>`
 * (nothing in a stylesheet outranks that), and the previous inline styles are
 * restored on unmount.
 *
 * Every piece of text goes through `textContent`.
 */
export class ImpersonationBanner {
  private host: HTMLElement | null = null;
  private text: HTMLElement | null = null;
  /** `style` attributes as the page had them before the shift. */
  private saved: { html: string | null; body: string | null } | null = null;

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
    if (typeof document === 'undefined') return;
    if (this.saved) {
      restoreStyle(document.documentElement, this.saved.html);
      restoreStyle(document.body, this.saved.body);
      this.saved = null;
    }
    document.documentElement?.style.setProperty('--sp00ky-impersonation-banner-height', '0px');
  }

  private mount(): void {
    if (this.host?.isConnected) return;
    const host = document.createElement(HOST_TAG);
    const root = host.attachShadow({ mode: 'closed' });
    const style = document.createElement('style');
    style.textContent = STYLE;

    // The striped ground the bar's text sits on: fixed, so it stays put while
    // the page scrolls underneath. Never takes a click.
    const layer = document.createElement('div');
    layer.className = 'layer';
    layer.setAttribute('aria-hidden', 'true');
    const corners = document.createElement('div');
    corners.className = 'corners';
    corners.setAttribute('aria-hidden', 'true');
    for (const side of ['left', 'right']) {
      const corner = document.createElement('div');
      corner.className = `corner ${side}`;
      corners.append(corner);
    }

    const bar = document.createElement('div');
    bar.className = 'bar';
    bar.setAttribute('role', 'alert');
    const pill = document.createElement('div');
    pill.className = 'pill';
    const sign = document.createElement('span');
    sign.className = 'sign';
    sign.textContent = '⚠';
    const text = document.createElement('span');
    text.className = 'text';
    const button = document.createElement('button');
    button.type = 'button';
    button.textContent = 'Stop';
    button.addEventListener('click', () => {
      button.disabled = true;
      this.onStop().finally(() => {
        button.disabled = false;
      });
    });
    pill.append(sign, text);
    bar.append(pill, button);
    root.append(style, layer, corners, bar);
    // Hosted on `<html>`, not `<body>`: the page shift clips `<body>` to the
    // rounded top, and a bar inside it would be clipped away with everything
    // else.
    document.documentElement.appendChild(host);

    this.shiftPage();
    this.host = host;
    this.text = text;
  }

  /**
   * Push the page below the warning and round its new top edge.
   *
   * `<html>` carries the stripes as its background, because that is what the
   * page's rounded corners reveal. `<body>` therefore has to be painted
   * opaque: most apps colour it themselves, but a transparent one would let
   * the stripes through behind the whole page, so its resolved colour is
   * computed first and pinned.
   */
  private shiftPage(): void {
    const root = document.documentElement;
    const body = document.body;
    if (!root || !body) return;
    this.saved = { html: root.getAttribute('style'), body: body.getAttribute('style') };

    const surface = resolveSurface(root, body);
    root.style.setProperty('--sp00ky-impersonation-banner-height', `${BANNER_HEIGHT_PX}px`);
    root.style.setProperty('background-image', STRIPES, 'important');
    root.style.setProperty('padding-top', `${BANNER_HEIGHT_PX}px`, 'important');
    root.style.setProperty('box-sizing', 'border-box', 'important');

    body.style.setProperty('background-color', surface, 'important');
    body.style.setProperty(
      'border-radius',
      `${PAGE_RADIUS_PX}px ${PAGE_RADIUS_PX}px 0 0`,
      'important'
    );
    // Clips the app's own content — a fixed header included — to the rounded
    // top, which is what makes the page read as a sheet over the stripes.
    body.style.setProperty(
      'clip-path',
      `inset(0 round ${PAGE_RADIUS_PX}px ${PAGE_RADIUS_PX}px 0 0)`,
      'important'
    );
    body.style.setProperty('box-shadow', '0 -6px 18px rgba(0, 0, 0, .35)', 'important');
    // An app sized to `100vh` would otherwise overflow by the bar's height and
    // put a scrollbar on a page that had none.
    body.style.setProperty('min-height', `calc(100vh - ${BANNER_HEIGHT_PX}px)`, 'important');
  }
}

/** Put an element's inline styles back exactly as they were. */
function restoreStyle(el: HTMLElement | null, saved: string | null): void {
  if (!el) return;
  if (saved === null) el.removeAttribute('style');
  else el.setAttribute('style', saved);
}

/**
 * The colour to paint `<body>` with while the stripes sit behind it: whatever
 * the page already paints (body first, then html), and failing that the canvas
 * colour for the viewer's colour scheme.
 */
function resolveSurface(root: HTMLElement, body: HTMLElement): string {
  for (const el of [body, root]) {
    const color = getComputedStyle(el).backgroundColor;
    if (color && !isTransparent(color)) return color;
  }
  const dark =
    typeof matchMedia === 'function' && matchMedia('(prefers-color-scheme: dark)').matches;
  return dark ? '#111111' : '#ffffff';
}

function isTransparent(color: string): boolean {
  if (color === 'transparent') return true;
  const parts = color.match(/[\d.]+/g);
  return !!parts && parts.length === 4 && Number(parts[3]) === 0;
}

function bold(value: string): HTMLElement {
  const b = document.createElement('b');
  b.textContent = value;
  return b;
}
