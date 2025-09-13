#!/usr/bin/env python3
"""
04d_build_wayback_map.py — Map original image URLs to Wayback using the exact chosen URL (no width normalization).

Inputs:
  --occ       work/image_manifest.csv      # must contain chosen_download_url, chosen_kind, and the original fields
  --snapshot  Wayback snapshot timestamp, e.g. 20250912000156

Output:
  --out-map   work/url_map_wayback.csv     # original_url -> new_url (Wayback im_ URL), kind

Behavior:
  - For each row in image_manifest.csv:
      * take chosen_download_url exactly as-is (already has the correct -###wi or -popup)
      * build: new_url = f"https://web.archive.org/web/{snapshot}im_/{chosen_download_url}"
      * map ALL original fields in that row (thumb_url, full_url, inferred_full_url, chosen_download_url)
        to that single new_url
  - Skips file:// originals
  - No CDX lookups, no host/suffix guessing
"""

from __future__ import annotations
import argparse, csv, sys
from pathlib import Path

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--occ", required=True, help="work/image_manifest.csv")
    ap.add_argument("--snapshot", required=True, help="Wayback timestamp, e.g. 20250912000156")
    ap.add_argument("--out-map", default="work/url_map_wayback.csv")
    ap.add_argument("--limit", type=int, default=0, help="only first N rows (for testing)")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    Path(args.out_map).parent.mkdir(parents=True, exist_ok=True)

    def log(msg: str):
        if args.verbose: print(msg)

    total_rows = 0
    mapped_rows = 0
    miss_rows = 0

    with open(args.out_map, "w", newline="", encoding="utf-8") as outf, \
         open(args.occ, newline="", encoding="utf-8") as inf:
        rdr = csv.DictReader(inf)
        need = {"thumb_url","full_url","inferred_full_url","chosen_download_url"}
        if "chosen_download_url" not in rdr.fieldnames:
            print("ERROR: chosen_download_url column is missing in image_manifest.csv", file=sys.stderr)
            sys.exit(1)

        w = csv.writer(outf)
        # Keep columns similar to earlier mapping files
        w.writerow(["original_url","new_url","kind"])

        for row in rdr:
            total_rows += 1
            chosen = (row.get("chosen_download_url") or "").strip()
            kind = (row.get("chosen_kind") or "").strip()
            if not chosen:
                miss_rows += 1
                if args.verbose:
                    log(f"[{total_rows}] SKIP (no chosen_download_url)")
                if args.limit and (mapped_rows + miss_rows) >= args.limit:
                    break
                continue

            # Build the Wayback 'im_' URL using the exact chosen URL (keep its suffix)
            wayback_url = f"https://web.archive.org/web/{args.snapshot}im_/{chosen}"

            # Map all original fields in this row to that wayback_url
            for field in ("thumb_url","full_url","inferred_full_url","chosen_download_url"):
                orig = (row.get(field) or "").strip()
                if not orig or orig.lower().startswith("file://"):
                    continue
                w.writerow([orig, wayback_url, kind])
                mapped_rows += 1

            if args.verbose:
                log(f"[{total_rows}] {chosen} -> {wayback_url}")

            if args.limit and (mapped_rows + miss_rows) >= args.limit:
                break

    print(f"Wrote {mapped_rows} mappings to {args.out_map} (rows processed: {total_rows}, rows without chosen: {miss_rows})")

if __name__ == "__main__":
    main()
