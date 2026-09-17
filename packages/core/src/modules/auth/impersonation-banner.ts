import type { ImpersonationInfo } from './impersonation';

/** Default height of the warning layer the page is shifted down by. The
 *  effective height is published as the CSS variable
 *  `--sp00ky-impersonation-banner-height` on `<html>` (`0px` when hidden), so
 *  an app can offset its own fixed header. */
export const BANNER_HEIGHT_PX = 44;

/** Default radius of the page's new top corners. */
export const PAGE_RADIUS_PX = 14;

/** Default hazard stripes: the warning layer, and the `<html>` backdrop the
 *  page's rounded corners reveal. */
export const BANNER_STRIPES =
  'repeating-linear-gradient(45deg, #f2b600 0 14px, #1c1403 14px 28px)';

const HOST_TAG = 'sp00ky-impersonation-banner';

/** How long a `custom` banner has to acknowledge itself before the built-in
 *  one is shown anyway. Long enough for a first paint, short enough that
 *  nobody browses unwarned. */
export const CUSTOM_BANNER_GRACE_MS = 2500;

/**
 * Restyling of the built-in banner. Every field is optional; anything unset
 * keeps the default. Colours are plain CSS values, so a project can pass its
 * own tokens (`var(--brand-warning)`) as long as they resolve at the document
 * level.
 */
export interface ImpersonationBannerTheme {
  /** Height of the bar, and therefore how far the page is pushed down. */
  heightPx?: number;
  /** Radius of the page's new top corners. `0` squares them off. */
  radiusPx?: number;
  /** Background of the warning layer. Any CSS `background` value. */
  background?: string;
  /** Background of the pill the text sits in. */
  pill?: string;
  /** Text colour inside the pill. */
  pillText?: string;
  /** Background of the Stop button. */
  accent?: string;
  /** Text colour of the Stop button. */
  accentText?: string;
  /** Label for the Stop button. Defaults to `Stop`. */
  stopLabel?: string;
  /** The warning sentence. Defaults to `Impersonating <target> as <admin>`. */
  label?: (info: ImpersonationInfo) => string;
  /** Drop the page shift and the rounded corners, leaving only the bar.
   *  The CSS variable is still published, so the app can do its own offset. */
  noPageShift?: boolean;
}

export interface ImpersonationBannerConfig {
  /**
   * - `'default'` renders the built-in banner.
   * - `'custom'` leaves the page alone so the app can render its own from
   *   `useImpersonation()` / `auth.subscribeImpersonation`. Acknowledge it
   *   with `client.acknowledgeImpersonationBanner()` while it is on screen;
   *   if nothing acknowledges within {@link CUSTOM_BANNER_GRACE_MS}, the
   *   built-in banner appears, on the assumption that the custom one failed
   *   to render rather than that nobody should be warned.
   * - `'none'` renders nothing, ever. The app takes over responsibility for
   *   telling whoever is looking at the page that it is not their session.
   */
  mode?: 'default' | 'custom' | 'none';
  /** Restyle the built-in banner (also used if `custom` falls back). */
  theme?: ImpersonationBannerTheme;
}

interface ResolvedTheme extends Required<Omit<ImpersonationBannerTheme, 'label' | 'stopLabel'>> {
  stopLabel: string;
  label: (info: ImpersonationInfo) => string;
}

function resolveTheme(theme: ImpersonationBannerTheme = {}): ResolvedTheme {
  return {
    heightPx: theme.heightPx ?? BANNER_HEIGHT_PX,
    radiusPx: theme.radiusPx ?? PAGE_RADIUS_PX,
    background: theme.background ?? BANNER_STRIPES,
    pill: theme.pill ?? 'rgba(14, 11, 2, .88)',
    pillText: theme.pillText ?? '#fde68a',
    accent: theme.accent ?? '#fde68a',
    accentText: theme.accentText ?? '#1c1403',
    stopLabel: theme.stopLabel ?? 'Stop',
    noPageShift: theme.noPageShift ?? false,
    label: theme.label ?? ((info) => `Impersonating **${info.target}** as ${info.admin}`),
  };
}

/** Keeps only the area OUTSIDE a corner's curve, so the nub is striped and
 *  everything inside the curve stays the app's own. `center` is the curve's
 *  centre inside the corner square. */
function cornerMask(center: string, radius: number): string {
  return `radial-gradient(circle ${radius}px at ${center}, transparent 0 ${radius}px, #000 ${radius}px)`;
}

// The shadow root keeps app CSS away from the bar, but the host element itself
// is in the page and matches the page's selectors (`div { display: none }`).
// An `!important` rule on `:host` wins over the page's own `!important`
// (inner tree context), which keeps the host rendered whatever the app says.
export function bannerCss(t: ResolvedTheme): string {
  return `
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
  height: ${t.heightPx}px; background: ${t.background};
}
/* The page's rounded top corners, drawn over whatever the app puts there.
   The body element's own border-radius rounds its background, but a fixed app
   header is not clipped by it, so each corner is also painted here: a striped
   square masked down to the nub OUTSIDE the corner's curve, so the app's own
   colour still shows inside it. */
.corners {
  position: fixed; left: 0; right: 0; top: ${t.heightPx}px;
  height: ${t.radiusPx}px; z-index: 2147483646; pointer-events: none;
}
.corner {
  position: absolute; top: 0; width: ${t.radiusPx}px; height: ${t.radiusPx}px;
  background: ${t.background};
}
.corner.left {
  left: 0;
  -webkit-mask: ${cornerMask('100% 100%', t.radiusPx)};
  mask: ${cornerMask('100% 100%', t.radiusPx)};
}
.corner.right {
  right: 0;
  -webkit-mask: ${cornerMask('0% 100%', t.radiusPx)};
  mask: ${cornerMask('0% 100%', t.radiusPx)};
}
.bar {
  position: fixed; inset: 0 0 auto 0; height: ${t.heightPx}px; z-index: 2147483647;
  display: flex; align-items: center; justify-content: center; gap: 10px;
  padding: 0 12px; box-sizing: border-box;
  font: 600 13px/1 system-ui, -apple-system, "Segoe UI", sans-serif;
}
.pill {
  display: flex; align-items: center; gap: 8px; min-width: 0; max-width: 100%;
  padding: 6px 10px; border-radius: 999px;
  background: ${t.pill}; color: ${t.pillText};
  box-shadow: 0 1px 6px rgba(0, 0, 0, .45);
}
.sign { flex: none; font-size: 14px; line-height: 1; }
.text { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; min-width: 0; }
button {
  flex: none; cursor: pointer; border: 0; border-radius: 999px;
  background: ${t.accent}; color: ${t.accentText};
  font: 800 12px/1 system-ui, sans-serif; padding: 7px 12px;
  box-shadow: 0 1px 6px rgba(0, 0, 0, .45);
}
button:hover { filter: brightness(1.12); }
button:disabled { opacity: .6; cursor: progress; }
@media (max-width: 520px) {
  .bar { gap: 6px; padding: 0 8px; }
  .pill { padding: 5px 8px; }
}
`;
}

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
  private theme: ResolvedTheme;
  private mode: 'default' | 'custom' | 'none';
  /** Grace timer for `custom` mode. */
  private fallbackTimer: ReturnType<typeof setTimeout> | null = null;
  /** The app said its own banner is on screen for this impersonation. */
  private acknowledged = false;
  private warnedAboutNone = false;

  constructor(
    private onStop: () => Promise<void>,
    config: ImpersonationBannerConfig = {}
  ) {
    this.mode = config.mode ?? 'default';
    this.theme = resolveTheme(config.theme);
  }

  /**
   * The app's own banner is rendered. Cancels the `custom`-mode fallback, and
   * removes the built-in banner if the fallback already fired (a slow first
   * paint, say). No-op in the other modes.
   */
  acknowledge(): void {
    this.acknowledged = true;
    this.clearFallback();
    if (this.mode === 'custom') this.teardown();
  }

  static supported(): boolean {
    return typeof document !== 'undefined' && typeof document.createElement === 'function';
  }

  update(info: ImpersonationInfo | null): void {
    if (!ImpersonationBanner.supported()) return;
    if (!info) {
      this.acknowledged = false;
      this.unmount();
      return;
    }
    if (this.mode === 'none') {
      // The app owns the warning entirely. Said once, loudly, because the
      // page now looks like an ordinary session of someone else's account.
      if (!this.warnedAboutNone) {
        this.warnedAboutNone = true;
        // oxlint-disable-next-line no-console
        console.warn(
          '[sp00ky] impersonation is active and impersonationBanner.mode is "none": ' +
            'nothing marks this page as another user\'s session.'
        );
      }
      return;
    }
    if (this.mode === 'custom' && !this.acknowledged) {
      // Give the app its grace period, then warn anyway.
      if (!this.fallbackTimer && !this.host) {
        this.fallbackTimer = setTimeout(() => {
          this.fallbackTimer = null;
          if (!this.acknowledged) this.render(info);
        }, CUSTOM_BANNER_GRACE_MS);
      }
      return;
    }
    if (this.mode === 'custom') return;
    this.render(info);
  }

  unmount(): void {
    this.clearFallback();
    this.teardown();
  }

  /** Mount (if needed) and write the current identities into the bar. */
  private render(info: ImpersonationInfo): void {
    this.mount();
    const text = this.text!;
    text.textContent = '';
    text.append(...labelNodes(this.theme.label(info)));
    text.title = `${this.theme.label(info)}. Every write is audited.`;
  }

  /** Remove the bar and undo the page shift. */
  private teardown(): void {
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

  private clearFallback(): void {
    if (this.fallbackTimer) clearTimeout(this.fallbackTimer);
    this.fallbackTimer = null;
  }

  private mount(): void {
    if (this.host?.isConnected) return;
    const host = document.createElement(HOST_TAG);
    const root = host.attachShadow({ mode: 'closed' });
    const style = document.createElement('style');
    style.textContent = bannerCss(this.theme);

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
    if (this.theme.noPageShift || this.theme.radiusPx <= 0) corners.remove();

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
    button.textContent = this.theme.stopLabel;
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
    const { heightPx, radiusPx, background, noPageShift } = this.theme;
    // Published even when the shift is off, so an app doing its own offset
    // has one number to read either way.
    root.style.setProperty('--sp00ky-impersonation-banner-height', `${heightPx}px`);
    if (noPageShift) return;

    this.saved = { html: root.getAttribute('style'), body: body.getAttribute('style') };
    const surface = resolveSurface(root, body);
    root.style.setProperty('background-image', background, 'important');
    root.style.setProperty('padding-top', `${heightPx}px`, 'important');
    root.style.setProperty('box-sizing', 'border-box', 'important');

    body.style.setProperty('background-color', surface, 'important');
    if (radiusPx > 0) {
      body.style.setProperty('border-radius', `${radiusPx}px ${radiusPx}px 0 0`, 'important');
      // Clips the app's own content — a fixed header included — to the rounded
      // top, which is what makes the page read as a sheet over the stripes.
      body.style.setProperty(
        'clip-path',
        `inset(0 round ${radiusPx}px ${radiusPx}px 0 0)`,
        'important'
      );
    }
    body.style.setProperty('box-shadow', '0 -6px 18px rgba(0, 0, 0, .35)', 'important');
    // An app sized to `100vh` would otherwise overflow by the bar's height and
    // put a scrollbar on a page that had none.
    body.style.setProperty('min-height', `calc(100vh - ${heightPx}px)`, 'important');
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

/**
 * The label, with every `<b>…</b>`-free segment as a text node and the record
 * ids emphasised. The label is a plain string (an app's own function can
 * return anything), so it never reaches the DOM as markup: `**bold**` marks
 * emphasis instead.
 */
function labelNodes(label: string): Node[] {
  return labelSegments(label).map(({ text, bold }) => {
    if (!bold) return document.createTextNode(text);
    const b = document.createElement('b');
    b.textContent = text;
    return b;
  });
}

/** The label split into plain and emphasised runs. Pure, so it is unit tested. */
function labelSegments(label: string): Array<{ text: string; bold: boolean }> {
  return label
    .split(/\*\*(.+?)\*\*/g)
    .map((text, i) => ({ text, bold: i % 2 === 1 }))
    .filter((part) => part.text !== '');
}

/** Exported for unit tests only. */
export const __test = { labelSegments, resolveTheme };
