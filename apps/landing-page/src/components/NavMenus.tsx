import { useEffect, useRef, useState } from 'react';
// Types only: the menus arrive as props, so `config/nav.ts` (which reads
// `process.env`) never reaches the browser bundle.
import type { NavMenu, NavMenuItem } from '../config/navMenus';
import { NavIcon } from './NavIcon';

const Caret = () => (
  <svg className="nav-caret" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
    <path d="M6 9 l6 6 6 -6" />
  </svg>
);

export function NavMenuItemLink({ item }: { item: NavMenuItem }) {
  return (
    <a
      className="nav-item"
      href={item.href}
      target={item.newTab ? '_blank' : undefined}
      rel={item.external ? 'noopener' : undefined}
    >
      <span className="nav-item-icon">
        <NavIcon name={item.icon} />
      </span>
      <span className="nav-item-body">
        <span className="nav-item-title">{item.title}</span>
        <span className="nav-item-desc">{item.desc}</span>
      </span>
    </a>
  );
}

/** The desktop dropdowns: one compact floating panel per trigger. */
export function NavMenus({ menus }: { menus: NavMenu[] }) {
  const [open, setOpen] = useState<string | null>(null);
  const closeTimer = useRef<ReturnType<typeof setTimeout> | null>(null);
  const triggers = useRef<Record<string, HTMLButtonElement | null>>({});

  const cancelClose = () => {
    if (closeTimer.current) clearTimeout(closeTimer.current);
    closeTimer.current = null;
  };
  useEffect(() => cancelClose, []);
  useEffect(() => {
    if (!open) return;
    const onDown = (event: PointerEvent) => {
      if (!(event.target as Element).closest('.nav-menu')) setOpen(null);
    };
    document.addEventListener('pointerdown', onDown);
    return () => document.removeEventListener('pointerdown', onDown);
  }, [open]);

  const show = (id: string) => {
    cancelClose();
    setOpen(id);
  };
  const scheduleClose = () => {
    cancelClose();
    closeTimer.current = setTimeout(() => setOpen(null), 180);
  };

  return (
    <>
      {menus.map((menu) => {
        const isOpen = open === menu.id;
        return (
          <div
            key={menu.id}
            className="nav-menu"
            onMouseEnter={() => show(menu.id)}
            onMouseLeave={scheduleClose}
            onFocus={() => show(menu.id)}
            onBlur={(event) => {
              if (!event.currentTarget.contains(event.relatedTarget as Node | null)) scheduleClose();
            }}
            onKeyDown={(event) => {
              if (event.key === 'Escape' && isOpen) {
                event.preventDefault();
                cancelClose();
                setOpen(null);
                triggers.current[menu.id]?.focus();
              }
            }}
          >
            <button
              type="button"
              ref={(el) => {
                triggers.current[menu.id] = el;
              }}
              className={`nav-link${isOpen ? ' active' : ''}`}
              aria-expanded={isOpen}
              aria-controls={`nav-panel-${menu.id}`}
              onClick={() => (isOpen ? setOpen(null) : show(menu.id))}
            >
              {menu.label}
              <Caret />
            </button>
            <div id={`nav-panel-${menu.id}`} className={`nav-panel${isOpen ? ' open' : ''}`} inert={!isOpen}>
              <div className="nav-panel-grid">
                {menu.items.map((item) => (
                  <NavMenuItemLink key={item.href} item={item} />
                ))}
              </div>
            </div>
          </div>
        );
      })}
    </>
  );
}

export default NavMenus;
