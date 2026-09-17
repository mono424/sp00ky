import type { ImpersonationInfo } from './impersonation';

/** Default height of the warning bar above the framed page. The effective
 *  height is published as the CSS variable
 *  `--sp00ky-impersonation-banner-height` on `<html>` (`0px` when hidden), so
 *  an app can offset its own fixed header. */
export const BANNER_HEIGHT_PX = 40;

/** Default gutter left, right and below the framed page. */
export const FRAME_INSET_PX = 10;

/** Default radius of the framed page's corners. */
export const PAGE_RADIUS_PX = 14;

/** The default backdrop: a warm amber-to-orange wash with a faint diagonal
 *  hazard rake over it. Layered background-image, topmost layer first. */
export const BANNER_BACKGROUND =
  'repeating-linear-gradient(45deg, rgba(69, 26, 3, .07) 0 9px, rgba(69, 26, 3, 0) 9px 18px),' +
  ' linear-gradient(135deg, #fbbf24 0%, #fb923c 52%, #f59e0b 100%)';

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
  /** Height of the warning bar above the page. */
  heightPx?: number;
  /** Gutter left, right and below the page. `0` frames only from the top. */
  insetPx?: number;
  /** Radius of the framed page's corners. `0` squares them off. */
  radiusPx?: number;
  /** The backdrop behind the page. Any CSS `background` value. */
  background?: string;
  /** Colour of the warning text on the backdrop. */
  text?: string;
  /** Background of the Stop button. */
  accent?: string;
  /** Text colour of the Stop button. */
  accentText?: string;
  /** Label for the Stop button. Defaults to `Stop`. */
  stopLabel?: string;
  /** The warning sentence. Defaults to `Impersonating <target> as <admin>`.
   *  Wrap a run in `**` to emphasise it; the result is text, never markup. */
  label?: (info: ImpersonationInfo) => string;
  /** Keep the bar but leave the page's own layout completely alone. The CSS
   *  variable is still published, so the app can do its own offset. */
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

interface ResolvedTheme
  extends Required<Omit<ImpersonationBannerTheme, 'label' | 'stopLabel'>> {
  stopLabel: string;
  label: (info: ImpersonationInfo) => string;
}

function resolveTheme(theme: ImpersonationBannerTheme = {}): ResolvedTheme {
  return {
    heightPx: theme.heightPx ?? BANNER_HEIGHT_PX,
    insetPx: theme.insetPx ?? FRAME_INSET_PX,
    radiusPx: theme.radiusPx ?? PAGE_RADIUS_PX,
    background: theme.background ?? BANNER_BACKGROUND,
    text: theme.text ?? '#45230a',
    accent: theme.accent ?? '#45230a',
    accentText: theme.accentText ?? '#fff7ed',
    stopLabel: theme.stopLabel ?? 'Stop',
    noPageShift: theme.noPageShift ?? false,
    label: theme.label ?? ((info) => `Impersonating **${info.target}** as ${info.admin}`),
  };
}

/**
 * How much the page has to shrink to fit inside the frame, and how wide its
 * layout box must be to fill the frame at that scale.
 *
 * Pure so the arithmetic is unit tested: the frame takes `heightPx` off the
 * top and `insetPx` off the other three sides, and the page is scaled
 * uniformly (one factor for both axes, so nothing is distorted) by the
 * vertical ratio. `width` is the CSS width the page's box needs, since a
 * zoomed box occupies `width * scale` of its parent.
 */
export function frameMetrics(
  viewport: { width: number; height: number },
  theme: { heightPx: number; insetPx: number }
): { scale: number; width: number; contentHeight: number } {
  const { width: w, height: h } = viewport;
  const contentHeight = Math.max(1, h - theme.heightPx - theme.insetPx);
  const contentWidth = Math.max(1, w - theme.insetPx * 2);
  // Uniform: the vertical fit decides, so the aspect ratio is untouched and
  // the page keeps every pixel of its height reachable.
  const scale = h > 0 ? contentHeight / h : 1;
  return { scale, width: contentWidth / scale, contentHeight };
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
/* The bar. The backdrop itself is painted on the root element, which is
   BEHIND the page's sheet; an overlay here would cover the page. The bar
   repeats that background with a fixed attachment, so its gradient lines up
   with the root's to the pixel. */
.bar {
  position: fixed; inset: 0 0 auto 0; height: ${t.heightPx}px; z-index: 2147483647;
  display: flex; align-items: center; justify-content: center; gap: 10px;
  padding: 0 ${Math.max(t.insetPx, 10)}px; box-sizing: border-box;
  background-image: ${t.background};
  background-attachment: fixed;
  color: ${t.text};
  font: 600 12.5px/1 system-ui, -apple-system, "Segoe UI", sans-serif;
  letter-spacing: .01em;
}
.sign {
  flex: none; display: grid; place-items: center;
  width: 17px; height: 17px; border-radius: 50%;
  background: ${t.text}; color: #fff7ed;
  font-size: 11px; font-weight: 800;
}
.text { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; min-width: 0; }
.text b { font-weight: 800; }
button {
  flex: none; cursor: pointer; border: 0; border-radius: 999px;
  background: ${t.accent}; color: ${t.accentText};
  font: 700 11.5px/1 system-ui, sans-serif; padding: 6px 11px;
  box-shadow: 0 1px 2px rgba(69, 26, 3, .35);
}
button:hover { filter: brightness(1.15); }
button:disabled { opacity: .6; cursor: progress; }
@media (max-width: 520px) {
  .bar { gap: 7px; padding: 0 8px; font-size: 11.5px; }
  .sign { display: none; }
}
`;
}

/**
 * The "you are impersonating" warning.
 *
 * The page becomes a sheet on a warm striped backdrop: a bar across the top
 * names the session, and the page itself is scaled down to sit inside the
 * remaining frame with rounded corners. Scaled, not pushed: growing the
 * document moved the bottom of an app-shell layout off-screen, while a
 * uniform scale keeps the aspect ratio and every pixel reachable, and
 * `100vh`/`100dvh` layouts still resolve to exactly the frame.
 *
 * The bar lives in a closed shadow root, so app styles cannot hide or
 * restyle it; the framing is written as `!important` inline styles on
 * `<html>` / `<body>` (nothing in a stylesheet outranks that) and the
 * previous inline styles are restored on unmount.
 *
 * Every piece of text goes through `textContent`.
 */
export class ImpersonationBanner {
  private host: HTMLElement | null = null;
  private text: HTMLElement | null = null;
  /** `style` attributes as the page had them before the frame. */
  private saved: { html: string | null; body: string | null } | null = null;
  private theme: ResolvedTheme;
  private mode: 'default' | 'custom' | 'none';
  /** Grace timer for `custom` mode. */
  private fallbackTimer: ReturnType<typeof setTimeout> | null = null;
  /** The app said its own banner is on screen for this impersonation. */
  private acknowledged = false;
  private warnedAboutNone = false;
  private onResize: (() => void) | null = null;

  constructor(
    private onStop: () => Promise<void>,
    config: ImpersonationBannerConfig = {}
  ) {
    this.mode = config.mode ?? 'default';
    this.theme = resolveTheme(config.theme);
  }

  static supported(): boolean {
    return typeof document !== 'undefined' && typeof document.createElement === 'function';
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
            "nothing marks this page as another user's session."
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
    const label = this.theme.label(info);
    const text = this.text!;
    text.textContent = '';
    text.append(...labelNodes(label));
    text.title = `${label.replace(/\*\*/g, '')}. Every write is audited.`;
  }

  /** Remove the bar and put the page back exactly as it was. */
  private teardown(): void {
    this.host?.remove();
    this.host = this.text = null;
    if (typeof document === 'undefined') return;
    if (this.onResize) {
      window.removeEventListener('resize', this.onResize);
      this.onResize = null;
    }
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

    const bar = document.createElement('div');
    bar.className = 'bar';
    bar.setAttribute('role', 'alert');
    const sign = document.createElement('span');
    sign.className = 'sign';
    sign.textContent = '!';
    sign.setAttribute('aria-hidden', 'true');
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
    bar.append(sign, text, button);
    root.append(style, bar);
    // Hosted on `<html>`, not `<body>`: the body is the framed sheet, and a
    // bar inside it would be scaled and clipped along with the page.
    document.documentElement.appendChild(host);

    this.frame();
    this.host = host;
    this.text = text;
  }

  /**
   * Turn `<body>` into the framed sheet.
   *
   * `zoom`, not `transform`: it scales at layout level, so `100vh` children
   * resolve to the frame instead of overflowing it, scrollbars stay correct,
   * and `position: fixed` keeps meaning the viewport (a transform would make
   * the body a containing block and every fixed header would scroll away).
   * The body's CSS width is widened by `1 / scale` so the scaled box still
   * fills the frame.
   */
  private frame(): void {
    const root = document.documentElement;
    const body = document.body;
    if (!root || !body) return;
    const { heightPx, insetPx, radiusPx, noPageShift } = this.theme;
    // Published even when the framing is off, so an app doing its own offset
    // has one number to read either way.
    root.style.setProperty('--sp00ky-impersonation-banner-height', `${heightPx}px`);
    if (noPageShift) return;

    this.saved = { html: root.getAttribute('style'), body: body.getAttribute('style') };
    const surface = resolveSurface(root, body);
    // The root paints the backdrop: it is the one box that is guaranteed to
    // be behind the page's sheet. `overflow: hidden` keeps the document
    // itself from scrolling — the sheet is fixed and scrolls internally.
    root.style.setProperty('background', this.theme.background, 'important');
    root.style.setProperty('overflow', 'hidden', 'important');

    const apply = () => {
      const { scale, width } = frameMetrics(
        { width: window.innerWidth, height: window.innerHeight },
        { heightPx, insetPx }
      );
      body.style.setProperty('position', 'fixed', 'important');
      body.style.setProperty('top', `${heightPx}px`, 'important');
      body.style.setProperty('left', `${insetPx}px`, 'important');
      body.style.setProperty('margin', '0', 'important');
      body.style.setProperty('zoom', String(scale), 'important');
      body.style.setProperty('width', `${width}px`, 'important');
      body.style.setProperty('height', '100vh', 'important');
      body.style.setProperty('overflow', 'auto', 'important');
      body.style.setProperty('background-color', surface, 'important');
      body.style.setProperty('box-shadow', '0 6px 24px rgba(69, 26, 3, .28)', 'important');
      if (radiusPx > 0) {
        const radius = `${radiusPx / scale}px`;
        body.style.setProperty('border-radius', radius, 'important');
        // Clips the page's own fixed layers (a loading overlay, a modal
        // backdrop) to the sheet, so nothing paints over the frame.
        body.style.setProperty('clip-path', `inset(0 round ${radius})`, 'important');
      }
    };
    apply();
    this.onResize = apply;
    window.addEventListener('resize', apply);
  }
}

/** Put an element's inline styles back exactly as they were. An element that
 *  had no `style` attribute (or an empty one) ends up without one again. */
function restoreStyle(el: HTMLElement | null, saved: string | null): void {
  if (!el) return;
  if (saved) el.setAttribute('style', saved);
  else el.removeAttribute('style');
}

/**
 * The colour to paint the sheet with: whatever the page already paints (body
 * first, then html), and failing that the canvas colour for the viewer's
 * colour scheme. A transparent sheet would show the backdrop through the
 * whole page.
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
 * The label as DOM nodes, emphasising the runs between `**` markers. The
 * label is a plain string (an app's own function can return anything), so it
 * never reaches the DOM as markup.
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
