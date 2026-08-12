#!/usr/bin/env python3
"""Render a BrainGlobe social card (1200x630 Open Graph image).

Mirrors the layout in assets/social-template.html. Requires Pillow.

  python render_card.py --out card.png --headline "X, now in BrainGlobe." --image brain.png
"""
import argparse
import os
import sys

try:
    from PIL import Image, ImageDraw, ImageFont
except ImportError:
    sys.exit("Pillow is required:  pip install Pillow")

BG = "#0b0d10"
FRAME_BG = "#000000"
FG = "#f3f1ec"
WHITE = "#ffffff"
RULE = "#22262b"
ACCENT = "#e3a96b"
ACCENT_WORD = "BrainGlobe"

W, H = 1200, 630
PAD_L = PAD_R = 80
PAD_T = PAD_B = 64
GAP = 28
CAPTION_SIZE = 15
CAPTION_BLOCK = 18
TILE_CAP_GAP = 14
FOOTER_PAD_TOP = 32
FOOTER_MARGIN_TOP = 28
RULE_H = 1
HEAD_SIZE = 30
HEAD_LEADING = 1.15
HEAD_MAX_W = 760
LOGO_SIZE = 64
LOGO_NUDGE = 4
RADIUS = 6
MAX_IMAGES = 3

FONT_DIRS = [
    "/System/Library/Fonts", "/Library/Fonts", os.path.expanduser("~/Library/Fonts"),
    "/usr/share/fonts", "/usr/local/share/fonts", "C:\\Windows\\Fonts",
]
# Explicit bold/regular files first; .ttc collections last (index 0 is Regular).
BOLD_FONTS = ["Arial Bold.ttf", "arialbd.ttf", "LiberationSans-Bold.ttf",
              "DejaVuSans-Bold.ttf", "HelveticaNeue.ttc", "Helvetica.ttc"]
BODY_FONTS = ["Arial.ttf", "arial.ttf", "LiberationSans-Regular.ttf",
              "DejaVuSans.ttf", "HelveticaNeue.ttc", "Helvetica.ttc"]


def find_font(names):
    for name in names:
        for root in FONT_DIRS:
            if not os.path.isdir(root):
                continue
            for dirpath, _, files in os.walk(root):
                if name in files:
                    return os.path.join(dirpath, name)
    return None


def load_font(names, size):
    path = find_font(names)
    if path is None:
        print("warning: no system font found, falling back to PIL default",
              file=sys.stderr)
        return ImageFont.load_default()
    try:
        return ImageFont.truetype(path, size)
    except OSError:
        return ImageFont.load_default()


def wrap(draw, text, font, max_w):
    lines, cur = [], ""
    for word in text.split():
        trial = f"{cur} {word}".strip()
        if not cur or draw.textlength(trial, font=font) <= max_w:
            cur = trial
        else:
            lines.append(cur)
            cur = word
    if cur:
        lines.append(cur)
    return lines or [""]


def rounded_mask(size, radius):
    mask = Image.new("L", size, 0)
    ImageDraw.Draw(mask).rounded_rectangle([0, 0, size[0] - 1, size[1] - 1], radius, fill=255)
    return mask


def fit(img, box_w, box_h, mode):
    """mode 'cover' scales to fill the box and crops; 'contain' scales to fit inside it."""
    pick = max if mode == "cover" else min
    factor = pick(box_w / img.width, box_h / img.height)
    new = img.convert("RGBA").resize(
        (max(round(img.width * factor), 1), max(round(img.height * factor), 1)),
        Image.LANCZOS)
    out = Image.new("RGBA", (box_w, box_h), FRAME_BG)
    out.paste(new, ((box_w - new.width) // 2, (box_h - new.height) // 2), new)
    return out


def draw_headline(draw, lines, font, x, y, leading):
    """Draw the headline, colouring every occurrence of 'BrainGlobe' in the accent."""
    for i, line in enumerate(lines):
        ly = y + round(i * leading)
        cx, rest = x, line
        while rest:
            idx = rest.find(ACCENT_WORD)
            if idx == -1:
                draw.text((cx, ly), rest, font=font, fill=WHITE)
                break
            before, rest = rest[:idx], rest[idx + len(ACCENT_WORD):]
            if before:
                draw.text((cx, ly), before, font=font, fill=WHITE)
                cx += draw.textlength(before, font=font)
            draw.text((cx, ly), ACCENT_WORD, font=font, fill=ACCENT)
            cx += draw.textlength(ACCENT_WORD, font=font)


def render(images, captions, headline, logo_path, out_path, scale):
    s = scale
    card = Image.new("RGB", (W * s, H * s), BG)
    draw = ImageDraw.Draw(card)

    head_font = load_font(BOLD_FONTS, HEAD_SIZE * s)
    cap_font = load_font(BODY_FONTS, CAPTION_SIZE * s)
    leading = HEAD_SIZE * HEAD_LEADING * s

    head_lines = wrap(draw, headline, head_font, HEAD_MAX_W * s)
    head_block = round(HEAD_SIZE * HEAD_LEADING * len(head_lines))
    footer_h = FOOTER_PAD_TOP + RULE_H + FOOTER_MARGIN_TOP + head_block
    footer_h = max(footer_h, FOOTER_PAD_TOP + RULE_H + FOOTER_MARGIN_TOP + LOGO_SIZE)

    has_captions = any(captions)
    cap_space = (TILE_CAP_GAP + CAPTION_BLOCK) if has_captions else 0
    frame_h = H - PAD_T - PAD_B - footer_h - cap_space
    if frame_h < 80:
        sys.exit("headline is too long — shorten it so the images still have room")

    n = len(images)
    if n == 1:
        img = images[0]
        frame_w = min(round(frame_h * (img.width / img.height)), W - PAD_L - PAD_R)
        xs, widths, mode = [(W - frame_w) // 2], [frame_w], "contain"
    else:
        inner = W - PAD_L - PAD_R
        frame_w = (inner - GAP * (n - 1)) // n
        xs = [PAD_L + i * (frame_w + GAP) for i in range(n)]
        widths, mode = [frame_w] * n, "cover"

    for i, img in enumerate(images):
        bw, bh = widths[i] * s, frame_h * s
        card.paste(fit(img, bw, bh, mode), (xs[i] * s, PAD_T * s),
                   rounded_mask((bw, bh), RADIUS * s))
        if has_captions and captions[i]:
            draw.text((xs[i] * s, (PAD_T + frame_h + TILE_CAP_GAP) * s),
                      captions[i], font=cap_font, fill=FG)

    rule_y = (PAD_T + frame_h + cap_space + FOOTER_PAD_TOP) * s
    draw.rectangle([PAD_L * s, rule_y, (W - PAD_R) * s, rule_y + max(s - 1, 0)], fill=RULE)

    draw_headline(draw, head_lines, head_font, PAD_L * s,
                  rule_y + FOOTER_MARGIN_TOP * s, leading)

    if logo_path and os.path.exists(logo_path):
        logo = Image.open(logo_path).convert("RGBA").resize(
            (LOGO_SIZE * s, LOGO_SIZE * s), Image.LANCZOS)
        card.paste(logo, ((W - PAD_R) * s - logo.width,
                          (H - PAD_B) * s - logo.height - LOGO_NUDGE * s), logo)
    elif logo_path:
        print(f"warning: logo not found at {logo_path}", file=sys.stderr)

    card.save(out_path)
    print(f"Wrote {out_path} ({W * s}x{H * s})")


def main():
    default_logo = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "assets", "brainglobe-logo.png")
    p = argparse.ArgumentParser(description="Render a BrainGlobe social card.")
    p.add_argument("--out", default="card.png")
    p.add_argument("--headline", required=True,
                   help='e.g. "A tawny dragon brain atlas, now in BrainGlobe."')
    p.add_argument("--image", action="append", required=True, dest="images",
                   help="repeat for up to 3 images")
    p.add_argument("--caption", action="append", default=[], dest="captions",
                   help="repeat, one per image (omit entirely for no captions)")
    p.add_argument("--logo", default=default_logo)
    p.add_argument("--scale", type=int, default=2,
                   help="multiplier on 1200x630 (default 2 = 2400x1260)")
    a = p.parse_args()

    if len(a.images) > MAX_IMAGES:
        p.error(f"at most {MAX_IMAGES} images")
    if a.captions and len(a.captions) != len(a.images):
        p.error("give one --caption per --image, or none at all")
    captions = list(a.captions) + [""] * (len(a.images) - len(a.captions))
    render([Image.open(path) for path in a.images], captions,
           a.headline, a.logo, a.out, a.scale)


if __name__ == "__main__":
    main()
