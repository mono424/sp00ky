#!/usr/bin/env python3
"""Draw the three "How it works" layers as isometric line sketches.

    python3 apps/landing-page/scripts/architecture-sketches.py

Writes public/architecture/{client,database,engine}.svg and the slab silhouette
src/assets/architecture-slab-mask.svg. Every layer is the same slab on the same
1536 x 1024 canvas, with its top face centred at (768, 418), so the scroll
stacking in SyncArchitecture.astro lines up. The logos ArchitecturePlate.astro
projects onto each part sit on the raised surfaces drawn here: if a height or
position changes, move `.plan-*-logo` in architecture.css with it (the script
prints the numbers).

Plan coordinates (x, y) map to the screen by the same 2:1 isometric matrix the
logos use: screen = C + (x - y, (x + y) / 2); a height z lifts a point by z px.
"""
import math
import os

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
OUT = os.path.join(ROOT, 'public', 'architecture')

W, H = 1536, 1024
CX, CY = 768, 418          # top face centre
A = 265                    # half side of the slab, plan units
R = 46                     # corner radius, plan units
T = 150                    # slab thickness, screen px

# High contrast against the section's #101318.
INK = '#dfe3ea'            # outlines
INK2 = '#a9afba'           # details
INK3 = '#737a87'           # hatching, construction
TOP = '#1b1f27'            # the slab's top face
RAISED = '#232832'         # tops of parts standing on it
SIDE = '#14171d'           # shaded sides
SIDE2 = '#191c23'          # lit sides
FONT = "ui-monospace, 'SFMono-Regular', Menlo, monospace"
NS = 'vector-effect="non-scaling-stroke"'


def iso(x, y, z=0):
    return (CX + x - y, CY + (x + y) / 2 - z)


def plan_group(z=0):
    """A group whose children are drawn in plan units on the plane at height z."""
    return f'<g transform="matrix(1 0.5 -1 0.5 {CX} {CY - z})" fill="none" stroke-linecap="round" stroke-linejoin="round">'


def path(points, close=True):
    d = 'M' + ' L'.join(f'{x:.1f} {y:.1f}' for x, y in points)
    return d + (' Z' if close else '')


def rounded_square(a, r, n=24):
    pts = []
    for cx, cy, start in ((a - r, -a + r, -90), (a - r, a - r, 0), (-a + r, a - r, 90), (-a + r, -a + r, 180)):
        for i in range(n + 1):
            t = math.radians(start + 90 * i / n)
            pts.append((cx + r * math.cos(t), cy + r * math.sin(t)))
    return pts


def hull(points):
    pts = sorted(set((round(x, 2), round(y, 2)) for x, y in points))
    def cross(o, a, b):
        return (a[0] - o[0]) * (b[1] - o[1]) - (a[1] - o[1]) * (b[0] - o[0])
    lower, upper = [], []
    for p in pts:
        while len(lower) >= 2 and cross(lower[-2], lower[-1], p) <= 0:
            lower.pop()
        lower.append(p)
    for p in reversed(pts):
        while len(upper) >= 2 and cross(upper[-2], upper[-1], p) <= 0:
            upper.pop()
        upper.append(p)
    return lower[:-1] + upper[:-1]


def front_edge(top):
    left = min(range(len(top)), key=lambda i: top[i][0])
    right = max(range(len(top)), key=lambda i: top[i][0])
    n = len(top)
    a = [top[(left + i) % n] for i in range(((right - left) % n) + 1)]
    b = [top[(left - i) % n] for i in range(((left - right) % n) + 1)]
    return a if sum(p[1] for p in a) / len(a) > sum(p[1] for p in b) / len(b) else b


def y_on(edge, x):
    for (x1, y1), (x2, y2) in zip(edge, edge[1:]):
        if min(x1, x2) <= x <= max(x1, x2) and x1 != x2:
            return y1 + (y2 - y1) * (x - x1) / (x2 - x1)
    return None


def s(w=1.6, c=INK2, extra=''):
    return f'stroke="{c}" stroke-width="{w}" {extra} {NS}'


def box(x0, y0, x1, y1, z0, z1, top=RAISED, hatch=0, stroke=INK, width=2.4):
    """An axis-aligned block standing on the plan, its two front faces shaded."""
    tp = [iso(x0, y0, z1), iso(x1, y0, z1), iso(x1, y1, z1), iso(x0, y1, z1)]
    right = [iso(x1, y0, z1), iso(x1, y1, z1), iso(x1, y1, z0), iso(x1, y0, z0)]
    left = [iso(x0, y1, z1), iso(x1, y1, z1), iso(x1, y1, z0), iso(x0, y1, z0)]
    out = [
        f'<path d="{path(left)}" fill="{SIDE2}" stroke="{stroke}" stroke-width="{width}" stroke-linejoin="round"/>',
        f'<path d="{path(right)}" fill="{SIDE}" stroke="{stroke}" stroke-width="{width}" stroke-linejoin="round"/>',
    ]
    if hatch and z1 - z0 > 10:
        lines = []
        steps = max(1, int((y1 - y0) / hatch))
        for i in range(1, steps):
            yy = y0 + (y1 - y0) * i / steps
            (ax, ay), (bx, by) = iso(x1, yy, z1), iso(x1, yy, z0)
            lines.append(f'M{ax:.1f} {ay + 3:.1f} L{bx:.1f} {by - 3:.1f}')
        out.append(f'<path d="{" ".join(lines)}" stroke="{INK3}" stroke-width="1.2"/>')
    out.append(f'<path d="{path(tp)}" fill="{top}" stroke="{stroke}" stroke-width="{width}" stroke-linejoin="round"/>')
    return ''.join(out)


def cylinder(cx, cy, r, z0, z1, rings=()):
    """An upright cylinder: a plan circle of radius r is an iso ellipse."""
    rx, ry = r * math.sqrt(2), r / math.sqrt(2)
    sx, sy0 = iso(cx, cy, z0)
    _, sy1 = iso(cx, cy, z1)
    body = f'M{sx - rx:.1f} {sy1:.1f} V{sy0:.1f} A{rx:.1f} {ry:.1f} 0 0 0 {sx + rx:.1f} {sy0:.1f} V{sy1:.1f} Z'
    out = [f'<path d="{body}" fill="{SIDE2}" stroke="{INK}" stroke-width="2.4" stroke-linejoin="round"/>']
    for z in rings:
        zy = iso(cx, cy, z)[1]
        out.append(f'<path d="M{sx - rx:.1f} {zy:.1f} A{rx:.1f} {ry:.1f} 0 0 0 {sx + rx:.1f} {zy:.1f}" stroke="{INK2}" stroke-width="1.6"/>')
    out.append(f'<ellipse cx="{sx:.1f}" cy="{sy1:.1f}" rx="{rx:.1f}" ry="{ry:.1f}" fill="{RAISED}" stroke="{INK}" stroke-width="2.4"/>')
    out.append(f'<ellipse cx="{sx:.1f}" cy="{sy1:.1f}" rx="{rx * 0.55:.1f}" ry="{ry * 0.55:.1f}" stroke="{INK3}" stroke-width="1.4"/>')
    return ''.join(out)


def standing_panel(origin, length, height, body, radius=10):
    """A thin upright panel on the plane y = origin[1], running along +x.

    `body` is drawn in panel space: u along the base, v upwards.
    """
    ox, oy = iso(*origin)
    return (
        f'<g transform="matrix(1 0.5 0 -1 {ox:.1f} {oy:.1f})" fill="none" stroke-linecap="round">'
        f'<rect x="0" y="0" width="{length}" height="{height}" rx="{radius}" fill="{RAISED}" stroke="{INK}" stroke-width="2.6" {NS}/>'
        f'{body}</g>'
    )


def slab(top_details, parts, caption):
    top = [iso(x, y) for x, y in rounded_square(A, R)]
    bottom = [(x, y + T) for x, y in top]
    silhouette = hull(top + bottom)
    edge = front_edge(top)
    front_x = iso(A, A)[0]
    out = []

    out.append(f'<path d="{path(silhouette)}" fill="{SIDE}" stroke="{INK}" stroke-width="3.2" stroke-linejoin="round"/>')
    seam = [(px, py + T * 0.36) for px, py in edge]
    out.append(f'<path d="{path(seam, False)}" stroke="{INK2}" stroke-width="2"/>')
    for ang in (0, 90):
        px, py = iso(A - R + R * math.cos(math.radians(ang)), A - R + R * math.sin(math.radians(ang)))
        out.append(f'<path d="M{px:.1f} {py:.1f} V{py + T:.1f}" stroke="{INK2}" stroke-width="1.6"/>')
    out.append(f'<path d="{path(top)}" fill="{TOP}" stroke="{INK}" stroke-width="3.2" stroke-linejoin="round"/>')

    out.append(plan_group())
    for sx in (-1, 1):
        for sy in (-1, 1):
            out.append(f'<circle cx="{sx * (A - 28)}" cy="{sy * (A - 28)}" r="7" {s(1.6)}/>')
    out.append(top_details)
    out.append('</g>')

    out.append(parts)

    out.append(
        f'<g font-family="{FONT}">'
        f'<text x="40" y="404" font-size="22" letter-spacing="3" fill="{INK2}">{caption[0]}</text>'
        f'<text x="40" y="440" font-size="26" fill="{INK}">{caption[1]}</text>'
        f'<path d="M40 456 H170" stroke="{INK3}" stroke-width="1.4"/>'
        '</g>'
    )
    svg = (
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {H}" width="{W}" height="{H}" fill="none">'
        + ''.join(out)
        + '</svg>'
    )
    return svg, silhouette


# FIG. 01, the client: one browser lying on the plate, a stylised website in
# it. The logo ArchitecturePlate projects is the site's hero mark.
BROWSER_Z = 10


def client():
    parts = [box(-218, -218, 218, 218, 0, BROWSER_Z, hatch=0, width=2.4)]
    g = plan_group(BROWSER_Z)
    w = []
    # Window chrome: title bar, three dots, address field.
    w.append(f'<path d="M-218 -186 H218" {s(1.6, INK)}/>')
    w.extend(f'<circle cx="{-198 + i * 16}" cy="-202" r="4.5" {s(1.4)}/>' for i in range(3))
    w.append(f'<rect x="-110" y="-210" width="220" height="16" rx="8" {s(1.3)}/>')
    # Site navigation: a mark and a button.
    w.append(f'<rect x="-196" y="-166" width="38" height="14" rx="3" {s(1.4)}/>')
    w.append(f'<rect x="124" y="-168" width="72" height="18" rx="9" {s(1.4)}/>')
    # Hero: the mark at the centre, one headline and a button below it.
    w.append(f'<path d="M-90 76 H90" {s(2.2, INK2)}/>')
    w.append(f'<rect x="-44" y="98" width="88" height="20" rx="10" {s(1.4, INK)}/>')
    # Three cards.
    for x0 in (-200, -64, 72):
        w.append(f'<rect x="{x0}" y="148" width="128" height="52" rx="6" {s(1.4)}/>')
    parts.append(g + ''.join(w) + '</g>')
    return slab('', ''.join(parts), ('FIG. 01', 'Client'))


# FIG. 02, SurrealDB: a raised vault in the middle, two permission rings with
# gates guarded by posts.
VAULT_Z = 28


def database():
    rings = []
    for a, w, c in ((206, 1.6, INK3), (160, 2, INK2)):
        g = 34  # half the gate width
        segs = [
            f'M{-a + 24} {-a} H{-g} M{g} {-a} H{a - 24}',
            f'M{-a + 24} {a} H{-g} M{g} {a} H{a - 24}',
            f'M{-a} {-a + 24} V{-g} M{-a} {g} V{a - 24}',
            f'M{a} {-a + 24} V{-g} M{a} {g} V{a - 24}',
        ]
        for cx, cy in ((-1, -1), (1, -1), (1, 1), (-1, 1)):
            segs.append(f'M{cx * (a - 24)} {cy * a} Q{cx * a} {cy * a} {cx * a} {cy * (a - 24)}')
        rings.append(f'<path d="{" ".join(segs)}" {s(w, c)}/>')
    details = ''.join(rings)

    posts = sorted(
        ((px, py) for a in (160,) for px, py in ((-34, -a), (34, -a), (-a, -34), (-a, 34), (a, -34), (a, 34), (-34, a), (34, a))),
        key=lambda p: p[0] + p[1],
    )
    parts = []
    for px, py in posts:
        if px + py < 0:
            parts.append(box(px - 7, py - 7, px + 7, py + 7, 0, 24, hatch=0, width=1.8))
    parts.append(box(-110, -110, 110, 110, 0, VAULT_Z))
    for px, py in posts:
        if px + py >= 0:
            parts.append(box(px - 7, py - 7, px + 7, py + 7, 0, 24, hatch=0, width=1.8))
    return slab(details, ''.join(parts), ('FIG. 02', 'SurrealDB'))


# FIG. 03, DBSP: a raised board, one processor and four chips standing on it,
# traces between them and an edge connector.
BOARD_Z = 8
CORE_Z = 34
CHIP_Z = 22
CHIPS = ((-150, 0), (0, -150), (0, 150), (150, 0))


def engine():
    parts = [box(-226, -226, 226, 226, 0, BOARD_Z, top=TOP, hatch=0, width=2.2)]

    traces = []
    for dx, dy in ((1, 0), (-1, 0), (0, 1), (0, -1)):
        for k in (-24, -8, 8, 24):
            ox, oy = (k, 0) if dx == 0 else (0, k)
            traces.append(f'M{dx * 80 + ox} {dy * 80 + oy} L{dx * 108 + ox} {dy * 108 + oy}')
    fingers = ' '.join(f'M208 {y} H220' for y in range(-120, 121, 24))
    parts.append(
        plan_group(BOARD_Z)
        + f'<path d="{" ".join(traces)}" {s(1.4)}/>'
        + f'<path d="{fingers}" {s(2.6, INK2)}/>'
        + '</g>'
    )

    blocks = [((cx, cy), 42, CHIP_Z) for cx, cy in CHIPS] + [((0, 0), 80, CORE_Z)]
    for (cx, cy), h, z in sorted(blocks, key=lambda b: b[0][0] + b[0][1]):
        parts.append(box(cx - h, cy - h, cx + h, cy + h, BOARD_Z, z))
        inset = 14 if h > 50 else 9
        parts.append(plan_group(z) + f'<rect x="{cx - h + inset}" y="{cy - h + inset}" width="{2 * (h - inset)}" height="{2 * (h - inset)}" rx="4" {s(1.2)}/></g>')
    return slab('', ''.join(parts), ('FIG. 03', 'DBSP'))


LOGOS = {
    'client hero mark': iso(0, 0, BROWSER_Z),
    'database vault': iso(0, 0, VAULT_Z),
    'engine core': iso(0, 0, CORE_Z),
    **{f'engine chip {i}': iso(cx, cy, CHIP_Z) for i, (cx, cy) in enumerate(CHIPS)},
}


def main():
    os.makedirs(OUT, exist_ok=True)
    silhouette = None
    for name, build in (('client', client), ('database', database), ('engine', engine)):
        svg, silhouette = build()
        with open(os.path.join(OUT, f'{name}.svg'), 'w') as f:
            f.write(svg)
    mask = f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {H}"><path fill="white" d="{path(silhouette)}"/></svg>'
    with open(os.path.join(ROOT, 'src', 'assets', 'architecture-slab-mask.svg'), 'w') as f:
        f.write(mask)
    for k, (x, y) in LOGOS.items():
        print(f'{k}: left {x / W * 100:.2f}%  top {y / H * 100:.2f}%')


if __name__ == '__main__':
    main()
