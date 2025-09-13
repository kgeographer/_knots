#!/usr/bin/env python3
"""
03_build_image_manifest.py

python scripts/03_build_image_manifest.py \
  --in work/posts_raw.jsonl \
  --out-occ work/image_manifest.csv \
  --out-unique work/image_urls_unique.csv

Reads posts from JSONL (output of 01_parse_dump.py) and emits:
  1) work/image_manifest.csv       (one row per image occurrence per post)
  2) work/image_urls_unique.csv    (deduped list of URLs to download with preferred choice)

Heuristics:
- Prefer full-size image URL from `link_href` (TypePad's popup/original).
- If `link_href` missing but `src` looks like a TypePad thumbnail (ends with `-<N>wi`),
  propose a full-size candidate by replacing the trailing `-<N>wi` with `-popup`.
- If neither full-size nor candidate is available, fall back to `src`.

Columns (occurrence CSV):
- post_index, post_unique_url, post_title, occurrence_index
- thumb_url, full_url, inferred_full_url, chosen_download_url
- alt, title, classes, style

Columns (unique CSV):
- chosen_download_url, kind (full|inferred_full|thumb), example_post_index, example_post_title
"""

from __future__ import annotations
import argparse
import csv
import json
import re
from pathlib import Path
from typing import Dict, Any, Optional, Tuple

THUMB_RE = re.compile(r"(-\d{2,4}wi)(?=$|[\?#])", re.IGNORECASE)  # matches -320wi, -1024wi, etc.

def infer_full_from_thumb(src: Optional[str]) -> Optional[str]:
    """
    TypePad pattern: thumbnails often end with -<N>wi, while full-size uses -popup.
    Example:
      http://.../6a00...-320wi  -> http://.../6a00...-popup
    """
    if not src:
        return None
    if THUMB_RE.search(src):
        return THUMB_RE.sub("-popup", src)
    return None

def choose_download_url(thumb_url: Optional[str], full_url: Optional[str], inferred_full: Optional[str]) -> Tuple[str, str]:
    """
    Selection preference:
      1) full_url (explicit popup/original)
      2) inferred_full (from thumb)
      3) thumb_url
    Returns (chosen_url, kind)
    """
    if full_url:
        return full_url, "full"
    if inferred_full:
        return inferred_full, "inferred_full"
    if thumb_url:
        return thumb_url, "thumb"
    return "", "none"

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="infile", required=True, help="Path to posts JSONL (work/posts_raw.jsonl)")
    ap.add_argument("--out-occ", dest="out_occ", required=True, help="Path to per-occurrence CSV (work/image_manifest.csv)")
    ap.add_argument("--out-unique", dest="out_unique", required=True, help="Path to deduped URLs CSV (work/image_urls_unique.csv)")
    args = ap.parse_args()

    in_path = Path(args.infile)
    out_occ_path = Path(args.out_occ)
    out_unique_path = Path(args.out_unique)

    out_occ_path.parent.mkdir(parents=True, exist_ok=True)
    out_unique_path.parent.mkdir(parents=True, exist_ok=True)

    total_posts = 0
    total_images = 0
    unique: Dict[str, Tuple[str, int, str]] = {}  # chosen_url -> (kind, example_post_index, example_post_title)

    with in_path.open("r", encoding="utf-8") as fin, \
         out_occ_path.open("w", newline="", encoding="utf-8") as focc:

        occ_writer = csv.writer(focc)
        occ_writer.writerow([
            "post_index",
            "post_unique_url",
            "post_title",
            "occurrence_index",
            "thumb_url",
            "full_url",
            "inferred_full_url",
            "chosen_download_url",
            "chosen_kind",
            "alt",
            "title",
            "classes",
            "style",
        ])

        for line in fin:
            if not line.strip():
                continue
            post = json.loads(line)
            total_posts += 1

            p_index = post.get("source_index")
            p_title = (post.get("title") or "").strip()
            p_unique = (post.get("unique_url") or "").strip()
            images = post.get("images") or []

            for i, img in enumerate(images):
                thumb = (img.get("src") or "").strip() or None
                full = (img.get("link_href") or "").strip() or None
                inferred_full = infer_full_from_thumb(thumb)
                chosen, kind = choose_download_url(thumb, full, inferred_full)

                occ_writer.writerow([
                    p_index,
                    p_unique,
                    p_title,
                    i,                 # occurrence index within the post
                    thumb or "",
                    full or "",
                    inferred_full or "",
                    chosen,
                    kind,
                    (img.get("alt") or ""),
                    (img.get("title") or ""),
                    (img.get("classes") or ""),
                    (img.get("style") or ""),
                ])

                if chosen:
                    if chosen not in unique:
                        unique[chosen] = (kind, p_index, p_title)

                total_images += 1

    # Emit unique URL list
    with out_unique_path.open("w", newline="", encoding="utf-8") as funiq:
        uniq_writer = csv.writer(funiq)
        uniq_writer.writerow(["chosen_download_url", "kind", "example_post_index", "example_post_title"])
        for url, (kind, pidx, ptitle) in sorted(unique.items()):
            uniq_writer.writerow([url, kind, pidx, ptitle])

    # QA summary
    print(f"[image manifest] Posts processed: {total_posts}")
    print(f"[image manifest] Image occurrences: {total_images}")
    print(f"[image manifest] Unique URLs to download: {len(unique)}")
    if len(unique) == 0:
        print("[image manifest] NOTE: No images found. Confirm that 01_parse_dump.py extracted images.")


if __name__ == "__main__":
    main()
