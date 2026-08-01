"""Resize and compress a logo for web use.

The badge renders the mark at 22px on the landing page and 34px in the deck,
so a 512px source is ~15x larger than needed and costs bytes on every load.
This downsamples to a sensible cap, strips metadata, and writes an optimised
PNG next to the original.

    python scripts/prep_logo.py web/shots/sonyliv-source.png
    python scripts/prep_logo.py <src> --out web/shots/sonyliv.png --max 96
"""
import argparse
import os
import sys

try:
    from PIL import Image
except ImportError:
    sys.exit("Pillow is required:  python -m pip install pillow")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("src")
    ap.add_argument("--out", default=None)
    ap.add_argument("--max", type=int, default=96,
                    help="longest edge in px (default 96 -- the badge draws at 22-34px, "
                         "so this still covers 2x and 3x displays)")
    a = ap.parse_args()

    if not os.path.exists(a.src):
        sys.exit("not found: " + a.src)
    out = a.out or os.path.join(os.path.dirname(a.src) or ".", "sonyliv.png")

    im = Image.open(a.src)
    before_px, before_bytes = im.size, os.path.getsize(a.src)
    im = im.convert("RGBA")
    im.thumbnail((a.max, a.max), Image.LANCZOS)

    # Rewrite without metadata: an exported brand asset often carries a colour
    # profile and EXIF larger than the pixels it describes at this size.
    clean = Image.new("RGBA", im.size)
    clean.putdata(list(im.getdata()))
    clean.save(out, "PNG", optimize=True)

    after = os.path.getsize(out)
    print(f"  {os.path.basename(a.src)}  {before_px[0]}x{before_px[1]}  "
          f"{before_bytes/1024:,.1f} kB")
    print(f"  -> {os.path.basename(out)}  {clean.size[0]}x{clean.size[1]}  "
          f"{after/1024:,.1f} kB  ({(1 - after/max(before_bytes,1))*100:.0f}% smaller)")


if __name__ == "__main__":
    main()
