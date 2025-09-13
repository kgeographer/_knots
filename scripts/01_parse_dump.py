#!/usr/bin/env python3
"""
01_parse_dump.py

Parse a TypePad-style text dump into JSONL:
- One JSON object per post
- Extracts header fields, BODY/EXTENDED BODY/EXCERPT/KEYWORDS
- Parses date to ISO-8601 (keeps raw)
- Inventories images found in HTML blocks

Usage:
  python 01_parse_dump.py --in dump/endlessknots.txt --out work/posts_raw.jsonl
"""

from __future__ import annotations
import argparse
import json
import re
from pathlib import Path
from dataclasses import dataclass, asdict, field
from datetime import datetime
from html import unescape
from typing import List, Dict, Optional

# --- Config: adjust if needed ---
POST_DIVIDER = re.compile(r"^\s*-{8,}\s*$")  # lines of 8+ dashes
BLOCK_DIVIDER = re.compile(r"^\s*-{5}\s*$")  # lines of exactly 5 dashes between blocks
KEY_VALUE_LINE = re.compile(r"^([A-Z ]+):\s*(.*)$")  # e.g., "TITLE: Foo"
DATE_FORMATS = [
    "%m/%d/%Y %I:%M:%S %p",   # "08/06/2016 01:52:25 PM"
    "%m/%d/%Y %H:%M:%S",      # fallback if 24h format appears
    "%Y-%m-%d %H:%M:%S",      # very defensive fallback
]

# HTML image extraction (simple & resilient)
IMG_TAG = re.compile(r'<img\b[^>]*?>', re.IGNORECASE | re.DOTALL)
ATTR_RE = re.compile(r'([a-zA-Z:-]+)\s*=\s*([\'"])(.*?)\2', re.DOTALL)
ASSET_LINK_RE = re.compile(
    r'<a\b[^>]*class=["\']?[^"\']*asset-img-link[^"\']*["\']?[^>]*href=["\'](.*?)["\'][^>]*>',
    re.IGNORECASE | re.DOTALL
)

@dataclass
class ImageRef:
    src: Optional[str] = None
    alt: Optional[str] = None
    title: Optional[str] = None
    width: Optional[str] = None
    height: Optional[str] = None
    classes: Optional[str] = None
    style: Optional[str] = None
    # If wrapped in an anchor to a popup/original:
    link_href: Optional[str] = None

@dataclass
class Post:
    source_index: int
    author: Optional[str] = None
    author_email: Optional[str] = None
    title: Optional[str] = None
    status: Optional[str] = None
    allow_comments: Optional[bool] = None
    allow_pings: Optional[bool] = None
    convert_breaks: Optional[str] = None
    basename: Optional[str] = None
    categories: List[str] = field(default_factory=list)
    unique_url: Optional[str] = None
    date_raw: Optional[str] = None
    date_iso: Optional[str] = None
    body_html: str = ""
    extended_body_html: str = ""
    excerpt_html: str = ""
    keywords: List[str] = field(default_factory=list)
    # derived
    images: List[ImageRef] = field(default_factory=list)

def parse_bool(s: str | None) -> Optional[bool]:
    if s is None:
        return None
    s = s.strip().lower()
    if s in {"1", "true", "yes", "y"}:
        return True
    if s in {"0", "false", "no", "n"}:
        return False
    return None

def parse_date(datestr: Optional[str]) -> Optional[str]:
    if not datestr:
        return None
    s = datestr.strip()
    for fmt in DATE_FORMATS:
        try:
            dt = datetime.strptime(s, fmt)
            # Keep naive time as-is; later steps may apply timezone if wanted
            return dt.isoformat()
        except ValueError:
            continue
    return None  # leave as None if unparsed; we keep date_raw in any case

def split_posts(raw: str) -> List[str]:
    # Split on lines of many dashes that mark end of a record
    # The sample ends with "--------" after KEYWORDS block.
    chunks = []
    acc = []
    for line in raw.splitlines():
        if POST_DIVIDER.match(line):
            if acc:
                chunks.append("\n".join(acc).strip())
                acc = []
        else:
            acc.append(line)
    if acc:
        # In case the last post isn't followed by divider
        chunks.append("\n".join(acc).strip())
    return [c for c in chunks if c]

def parse_post_block(block: str, index: int) -> Post:
    """
    Parse a single post block into a Post dataclass.
    The block is structured as:
      header key: value lines
      BODY:
      <html...>
      -----
      EXTENDED BODY:
      <html...>
      -----
      EXCERPT:
      <html...>
      -----
      KEYWORDS:
      <comma; separated; maybe empty>
    """
    # First, split into logical sections keyed by the labels ("BODY:", etc.)
    # We'll scan line by line; capture header KV pairs until a known section starts.
    lines = block.splitlines()
    headers: Dict[str, List[str]] = {}
    sections: Dict[str, List[str]] = {"BODY": [], "EXTENDED BODY": [], "EXCERPT": [], "KEYWORDS": []}
    current_section = None
    i = 0

    def set_header(key: str, val: str):
        key = key.strip().upper()
        headers.setdefault(key, []).append(val.strip())

    while i < len(lines):
        line = lines[i]
        # Section starts?
        if line.strip().upper() in {"BODY:", "EXTENDED BODY:", "EXCERPT:", "KEYWORDS:"}:
            current_section = line.strip().rstrip(":").upper()
            # Consume until next BLOCK_DIVIDER ("-----")
            i += 1
            buf = []
            while i < len(lines) and not BLOCK_DIVIDER.match(lines[i]):
                buf.append(lines[i])
                i += 1
            sections[current_section] = buf
            # skip the "-----" divider if present
            if i < len(lines) and BLOCK_DIVIDER.match(lines[i]):
                i += 1
            current_section = None
            continue

        # Not in a section → look for header KV lines
        m = KEY_VALUE_LINE.match(line)
        if m:
            k, v = m.group(1).strip(), m.group(2)
            set_header(k, v)
        i += 1

    # Build Post
    p = Post(source_index=index)

    def hget1(key: str) -> Optional[str]:
        vals = headers.get(key.upper())
        return vals[0] if vals else None

    def hgetall(key: str) -> List[str]:
        return headers.get(key.upper(), [])

    p.author = hget1("AUTHOR")
    p.author_email = hget1("AUTHOR EMAIL")
    p.title = hget1("TITLE")
    p.status = hget1("STATUS")
    p.allow_comments = parse_bool(hget1("ALLOW COMMENTS"))
    p.allow_pings = parse_bool(hget1("ALLOW PINGS"))
    p.convert_breaks = hget1("CONVERT BREAKS")
    p.basename = hget1("BASENAME")
    p.categories = hgetall("CATEGORY")
    p.unique_url = (hget1("UNIQUE URL") or "").strip() or None
    p.date_raw = hget1("DATE")
    p.date_iso = parse_date(p.date_raw)

    # sections → join back; unescape HTML entities once (TypePad dumps often include &#39; etc.)
    p.body_html = unescape("\n".join(sections.get("BODY", [])).strip())
    p.extended_body_html = unescape("\n".join(sections.get("EXTENDED BODY", [])).strip())
    p.excerpt_html = unescape("\n".join(sections.get("EXCERPT", [])).strip())

    # keywords may be comma/semicolon separated, or blank
    raw_kw = unescape("\n".join(sections.get("KEYWORDS", [])).strip())
    if raw_kw:
        parts = re.split(r"[;,]\s*", raw_kw)
        p.keywords = [t for t in (w.strip() for w in parts) if t]
    else:
        p.keywords = []

    # Derive image inventory from HTML blocks
    p.images = extract_images_from_html("\n".join([p.body_html, p.extended_body_html, p.excerpt_html]))

    return p

def extract_images_from_html(html: str) -> List[ImageRef]:
    """
    Extract images with special handling for TypePad's anchor-wrapped pattern:
      <a class="asset-img-link" href="FULL-...-popup"><img src="THUMB-...-320wi" ...></a>
    We:
      - record both THUMB (as src) and FULL (as link_href)
      - also pick up any standalone <img> not already captured
    """
    if not html:
        return []

    images: List[ImageRef] = []
    seen_srcs = set()

    # 1) Anchor-wrapped image blocks (preferred: we get full + thumb together)
    anchor_img_block_re = re.compile(
        r'<a\b[^>]*class=["\'][^"\']*asset-img-link[^"\']*["\'][^>]*href=["\'](?P<href>[^"\']+)["\'][^>]*>'
        r'\s*<img\b(?P<img>[^>]*?)>\s*</a>',
        re.IGNORECASE | re.DOTALL
    )
    for m in anchor_img_block_re.finditer(html):
        full_href = m.group('href')
        img_tag_inner = m.group('img')
        attrs = dict((am.group(1).lower(), am.group(3)) for am in ATTR_RE.finditer(img_tag_inner))
        src = attrs.get('src')
        ref = ImageRef(
            src=src,
            alt=attrs.get('alt'),
            title=attrs.get('title'),
            width=attrs.get('width'),
            height=attrs.get('height'),
            classes=attrs.get('class'),
            style=attrs.get('style'),
            link_href=full_href
        )
        images.append(ref)
        if src:
            seen_srcs.add(src)

    # 2) Any remaining standalone <img> tags not already captured
    for img_tag in IMG_TAG.findall(html):
        attrs = dict((m.group(1).lower(), m.group(3)) for m in ATTR_RE.finditer(img_tag))
        src = attrs.get('src')
        if not src or src in seen_srcs:
            continue
        ref = ImageRef(
            src=src,
            alt=attrs.get('alt'),
            title=attrs.get('title'),
            width=attrs.get('width'),
            height=attrs.get('height'),
            classes=attrs.get('class'),
            style=attrs.get('style'),
            link_href=None
        )
        images.append(ref)
        seen_srcs.add(src)

    return images

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="infile", required=True, help="Path to TypePad dump (e.g., dump/Endless Knots.txt)")
    ap.add_argument("--out", dest="outfile", required=True, help="Path to JSONL output (e.g., work/posts_raw.jsonl)")
    args = ap.parse_args()

    in_path = Path(args.infile)
    out_path = Path(args.outfile)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    raw = in_path.read_text(encoding="utf-8", errors="ignore")
    post_blocks = split_posts(raw)

    with out_path.open("w", encoding="utf-8") as f:
        for idx, block in enumerate(post_blocks, start=1):
            post = parse_post_block(block, idx)
            f.write(json.dumps(asdict(post), ensure_ascii=False) + "\n")

    print(f"Parsed {len(post_blocks)} posts → {out_path}")

if __name__ == "__main__":
    main()
