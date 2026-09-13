# Landing page artwork

Generated with the built-in image generation tool on 2026-09-13. The user's attached images were visual references only.

- `munich-california-hero.png`: homepage landscape, consumed by `src/pages/index.astro`.
- `munich-california-hero-mobile.png`: recomposed portrait for mobile, selected by the hero’s `<picture>` source.
- `private-sync-stack.png`: original composite stack, retained as an art-direction reference. The walkthrough now uses three separate layer images; see `architecture-prompts.md`.
- `architecture-*-mask.svg`: precise silhouettes traced from the original hardware artwork, used as CSS masks. They remove the external black backgrounds while keeping the dark hardware surfaces opaque, allowing the three layers to stack cleanly.

All three rendered layers reuse the SurrealDB illustration's chassis for identical proportions and lighting. The frontend screens and engine circuits are separate overlays from their original generated illustrations, isolated with `architecture-devices-mask.svg` and `architecture-circuit-mask.svg`.

Astro generates responsive WebP variants during the build; original PNGs are retained here as source assets. The architecture labels are accessible HTML, not part of the illustration.

## Hero prompt

Use case: stylized-concept. Asset type: premium open source software website hero background, panoramic 16:9 high resolution.
Create a breathtaking surreal monochrome ultramarine-blue and ivory photographic etching, inspired by the attached visual's grainy cyanotype print quality, enormous sculptural cloud, impossible architectural landscape and still reflective water. This is NEW artwork, reference only for art direction.
Scene: a dreamlike meeting of Bavaria and Northern California across one still alpine lake. Left shore: dark Bavarian Forest spruce, Munich's recognizable twin onion-domed Frauenkirche towers and a subtle academic stone facade evoking TUM's Munich campus. Right shore: Tahoe granite boulders and towering Sierra pines, Berkeley's slender Campanile rising among trees, a small distant section of the Golden Gate Bridge appearing through fog. A colossal white cumulus cloud rises behind the mountains on the right. Foreground dark blue pine silhouettes frame mirror-still water, fine etched ripples and fog ribbons. Hide one tiny ivory sheet ghost with two dark eyes near the trees on the lower right shore: charming but eerie easter egg. One coherent magical landscape, not a collage of tourist postcards.
Composition: cinematic wide establishing shot. Reserve top left/center upper 45 percent as very deep ultramarine open sky with subdued fog for white webpage heading overlay; architecture and landscape mainly across lower half, spectacular white cloud on right. Detailed lake reflections. Analog silver-grain texture, stippled print, sharp sculptural architecture, deep blue shadows and chalk white highlights. Gallery quality, mysterious, quiet, awe inspiring, sophisticated. No text, no typography, no logos, no watermark.

## Mobile hero prompt

Use case: style-transfer / responsive composition. Adapt the attached cyanotype landscape into a tall 9:16 portrait mobile website hero. Keep the same exact ultramarine blue and chalk white photographic etching aesthetic, granular print texture, dreamy still lake and monumental clouds. Preserve all the scene's core subjects in a cohesive recomposed landscape: Munich twin onion-domed Frauenkirche towers and academic classical TUM-evoking facade on the left, Berkeley Campanile on the right, small Golden Gate Bridge in the middle distance, Bavarian spruce forests and Tahoe granite/pine woods framing the lake, and a tiny white sheet ghost with two dark eyes on the lower right shore. Critical mobile layout: TOP 42 PERCENT should be quiet deep ultramarine mostly empty sky suitable for white website heading and buttons. All recognizable architectural landmarks clustered in the lower 50 percent, smaller scale to fit every landmark across the narrow portrait frame. Sculptural clouds bloom behind them in the middle right, stay away from the top left text space. Lake and mirrored ripples in bottom quarter. This is image-only background, no text or logos, no website mockup or UI. Exquisite mysterious analog blue print, unified scene, no extra elements.

## Stack prompt

Use case: stylized-concept. Asset type: premium software architecture illustration for a dark website, portrait 2:3.
Create a high-end photoreal 3D exploded stack of exactly THREE floating rounded-square hardware slabs, vertically aligned, generous equal gaps, same isometric camera, complete objects inside frame. Inspired by attached reference's elegant black anodized metal, fine etched technical contours, iridescent milled edges and dashed vertical connector lines. Reference only, no copied words or logo.
Top slab symbolizes FRONTEND: smoked glass top with a small sculptural browser window and mobile screen standing slightly above its surface, soft ivory light.
Middle slab symbolizes SURREALDB permission boundary: polished graphite top, a beautiful small translucent cyan crystalline shield engraved with a keyhole at center, silver edges with subtle ice-blue iridescence. Thin dotted vertical connections only between neighboring slabs.
Bottom slab symbolizes private sp00ky DBSP engine: dark brushed metal top with an inset matrix of miniature processor chips connected by exact delicate circuit traces, one small luminous white ghost emblem engraved onto the central processor. Violet-blue subtle rim light, one warm silver glint. Bright precise silhouettes but mostly black.
Background uniform near-black #080a0d seamlessly fading at outer edges, no floor, no pedestal, no horizon, objects float. Extremely refined industrial design render, physically realistic materials, studio macro detail, cinematic restrained light, no rainbow explosion. Balanced composition with top slab centered around 20 percent image height, middle at 50 percent, bottom at 80 percent. No text, no numbers, no labels, no watermarks.
