# BrainGlobe social card

A Claude skill (and a standalone script) for making the 1200×630 images we post when a new
atlas lands in BrainGlobe.

```
brainglobe-social-card/
  SKILL.md                     the instructions Claude reads
  scripts/render_card.py       the renderer
  assets/brainglobe-logo.png   logo
  assets/social-template.html  same layout in HTML, for previewing by hand
```

## Use it with Claude

Drop this folder into a project's skills directory (`.claude/skills/` in a repo for Claude
Code, or upload it as a skill on claude.ai) and ask for a social card for a blog post. Claude
fetches the figure images, writes the headline and runs the renderer.

## Use it on its own

Needs Python and Pillow (`pip install Pillow`).

```bash
python scripts/render_card.py \
  --headline "A tawny dragon brain atlas, now in BrainGlobe." \
  --image hoops_tawny_dragon_annotations.png \
  --out card.png
```

Up to three images, each with a caption:

```bash
python scripts/render_card.py \
  --headline "Three Allen CCFv2 mouse brain atlases, now in BrainGlobe." \
  --image ccfv2_mouse_brain.png --caption "CCFv2 Mouse Brain" \
  --image ccfv2_fiber_mouse.png --caption "CCFv2 Mouse Fiber Tracts" \
  --image ccfv2_dev_mouse.png --caption "CCFv2 Developmental Mouse" \
  --out card.png
```

`--scale 4` renders at 4800×2520. The word "BrainGlobe" in the headline is coloured
automatically.

Blog figure images are at `https://brainglobe.info/_images/<name>.png` once a post is
published, or under `docs/source/blog/images/` in the website repo before that.

## Why 1200×630

It's the Open Graph ratio (1.91:1) that Bluesky, Mastodon and LinkedIn all use for link
previews, so the card is never letterboxed. The 64/80px padding keeps everything inside a
~10% safe zone in case a mobile client crops the edges.
