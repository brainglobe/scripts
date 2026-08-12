---
name: brainglobe-social-card
description: Generate a BrainGlobe-branded social media card (1200×630 Open Graph image) for a blog post announcing a new brain atlas. Use when asked for a social image, share card, link preview, or announcement graphic for a BrainGlobe blog post or new atlas.
---

# BrainGlobe social card

Produces a 1200×630 PNG for announcing a new atlas on Bluesky, Mastodon and LinkedIn.

## Steps

1. **Get the brief.** You need: the blog post (URL or path), which images to show (usually
   the figure images from the post, 1–3 of them), and a short caption for each.
2. **Fetch the images** into a working directory:
   - Published posts: `https://brainglobe.info/_images/<name>.png`
   - Unpublished: `docs/source/blog/images/` in the `brainglobe/brainglobe.github.io` repo.
3. **Write the headline.** One line, sentence case, ending in a full stop, of the form
   "<what has been added>, now in BrainGlobe." Keep it under ~60 characters. Examples:
   - `Three Allen CCFv2 mouse brain atlases, now in BrainGlobe.`
   - `A tawny dragon brain atlas, now in BrainGlobe.`
   The word "BrainGlobe" is automatically coloured in the accent orange — write it plainly.
4. **Render the PNG** with the bundled script:

   ```bash
   python scripts/render_card.py \
     --out card.png \
     --headline "A tawny dragon brain atlas, now in BrainGlobe." \
     --image brain.png \
     --logo assets/brainglobe-logo.png
   ```

   Multiple images, each with a caption:

   ```bash
   python scripts/render_card.py \
     --out card.png \
     --headline "Three Allen CCFv2 mouse brain atlases, now in BrainGlobe." \
     --image ccfv2_mouse_brain.png --caption "CCFv2 Mouse Brain" \
     --image ccfv2_fiber_mouse.png --caption "CCFv2 Mouse Fiber Tracts" \
     --image ccfv2_dev_mouse.png --caption "CCFv2 Developmental Mouse" \
     --logo assets/brainglobe-logo.png
   ```

   `--scale 4` for a higher-resolution export (default is 2, i.e. 2400×1260).

5. **Give the user the PNG** and a one-line alt text description of what the image shows.

`assets/social-template.html` is the same layout in HTML, for previewing or hand-editing in a
browser. The script is the source of truth for exports — browser screenshot tools mis-capture
the scaled layout.

## Design rules

Do not vary these without being asked.

| | |
|---|---|
| Canvas | 1200×630 (1.91:1, the Open Graph ratio all three platforms use) |
| Padding | 64px top/bottom, 80px left/right — a ~10% safe zone so mobile feeds don't crop anything |
| Background | `#0b0d10` |
| Image frames | `#000`, 6px radius. Multiple images: equal columns, 28px gap, cropped to fill. Single image: frame sized to the image's own aspect ratio and centred — never a full-width frame with black bands at the sides |
| Rule | 1px `#22262b` above the footer |
| Headline | 30px, weight 600, white; "BrainGlobe" in `#e3a96b` |
| Captions | 15px, weight 500, `#f3f1ec` |
| Logo | 64px, bottom-right |
| Type | Helvetica Neue / Helvetica / Arial, falling back to Liberation or DejaVu Sans |

No URL, no strapline, no wordmark text — the logo is the sign-off. No numbering next to
captions. One headline line only; let it wrap to two lines if it needs to.
