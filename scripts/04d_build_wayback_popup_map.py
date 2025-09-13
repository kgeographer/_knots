#!/usr/bin/env python3
"""
04d_build_wayback_popup_map.py — Map all image variants to the popup form in Wayback.

Inputs:
  --occ       work/image_manifest.csv
  --snapshot  Wayback snapshot timestamp, e.g. 20250912000156

Output:
  --out-map   work/url_map_wayback.csv

python scripts/04d_build_wayback_popup_map.py \
  --occ work/image_manifest.csv \
  --snapshot 20250912000156 \
  --out-map work/url_map_wayback.csv \
  --limit 10 --verbose

Behavior:
  - For each row in image_manifest.csv:
      * Use full_url (-popup) as canonical.
      * Build new_url = https://web.archive.org/web/{snapshot}im_/{full_url}
      * Map ALL URL fields in that row (thumb_url, full_url, inferred_full_url,
        chosen_download_url) → new_url.
  - Skips file:// entries.
"""

from __future__ import annotations
import argparse, csv, sys
from pathlib import Path

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--occ", required=True, help="work/image_manifest.csv")
    ap.add_argument("--snapshot", required=True, help="Wayback snapshot timestamp, e.g. 20250912000156")
    ap.add_argument("--out-map", default="work/url_map_wayback.csv")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    Path(args.out_map).parent.mkdir(parents=True, exist_ok=True)

    def log(msg: str):
        if args.verbose: print(msg)

    total = 0
    mapped = 0
    skipped = 0

    with open(args.out_map, "w", newline="", encoding="utf-8") as outf, \
         open(args.occ, newline="", encoding="utf-8") as inf:
        rdr = csv.DictReader(inf)
        if "full_url" not in rdr.fieldnames:
            print("ERROR: full_url missing in image_manifest.csv", file=sys.stderr)
            sys.exit(1)
        w = csv.writer(outf)
        w.writerow(["original_url","new_url","kind"])

        for row in rdr:
            total += 1
            full = (row.get("full_url") or "").strip()
            if not full or full.lower().startswith("file://"):
                skipped += 1
                continue

            kind = (row.get("chosen_kind") or "").strip()
            new_url = f"https://web.archive.org/web/{args.snapshot}im_/{full}"

            for field in ("thumb_url","full_url","inferred_full_url","chosen_download_url"):
                orig = (row.get(field) or "").strip()
                if not orig or orig.lower().startswith("file://"):
                    continue
                w.writerow([orig, new_url, kind])
                mapped += 1

            log(f"[{total}] {full} -> {new_url}")

            if args.limit and total >= args.limit:
                break

    print(f"Processed {total} rows: mapped {mapped}, skipped {skipped}. Output: {args.out_map}")

if __name__ == "__main__":
    main()
