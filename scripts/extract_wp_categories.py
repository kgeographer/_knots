#!/usr/bin/env python3
"""
extract_wp_categories.py

Extract unique WordPress post categories from a WXR (WordPress export) file.
Produces:
  - category_inventory.csv (category, count)
  - category_mapping.csv (category, mapped_label, mapped_slug)

Usage:
  python extract_wp_categories.py /path/to/knotty.wordpress.2025-09-14.000.xml \
      --outdir ./dump/categories \
      --no-case-fold
"""

import argparse
import csv
import os
import sys
import xml.etree.ElementTree as ET
from collections import Counter

def parse_args():
    ap = argparse.ArgumentParser(description="Extract unique post categories from a WordPress WXR file.")
    ap.add_argument("wxr_path", help="Path to WordPress export XML (WXR).")
    ap.add_argument("--outdir", default=".", help="Directory for output CSV files (default: current dir).")
    ap.add_argument("--case-fold", dest="case_fold", action="store_true", default=True,
                    help="Case-insensitive category aggregation (default: on).")
    ap.add_argument("--no-case-fold", dest="case_fold", action="store_false",
                    help="Disable case-insensitive aggregation.")
    return ap.parse_args()

def main():
    args = parse_args()

    if not os.path.isfile(args.wxr_path):
        print(f"Error: file not found: {args.wxr_path}", file=sys.stderr)
        sys.exit(1)

    os.makedirs(args.outdir, exist_ok=True)

    try:
        tree = ET.parse(args.wxr_path)
    except ET.ParseError as e:
        print(f"XML parse error: {e}", file=sys.stderr)
        sys.exit(1)

    root = tree.getroot()
    channel = root.find("channel")
    if channel is None:
        for child in root:
            if child.tag.endswith("channel"):
                channel = child
                break
    if channel is None:
        print("Error: Could not find <channel> in WXR.", file=sys.stderr)
        sys.exit(1)

    cat_counter = Counter()
    seen_original_case = {}

    for item in channel.findall("item"):
        for cat in item.findall("category"):
            if cat.get("domain") != "category":
                continue
            name = (cat.text or "").strip() or (cat.get("nicename") or "").strip()
            if not name:
                continue
            key = name.lower() if args.case_fold else name
            if key not in seen_original_case:
                seen_original_case[key] = name
            cat_counter[key] += 1

    rows = []
    for key, count in cat_counter.items():
        original = seen_original_case.get(key, key)
        rows.append((original, count))
    rows.sort(key=lambda x: (-x[1], x[0]))

    inv_path = os.path.join(args.outdir, "category_inventory.csv")
    map_path = os.path.join(args.outdir, "category_mapping.csv")

    with open(inv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["category", "count"])
        for cat_name, count in rows:
            w.writerow([cat_name, count])

    with open(map_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["category", "mapped_label", "mapped_slug"])
        for cat_name, _ in rows:
            w.writerow([cat_name, "", ""])

    print(f"Wrote {len(rows)} unique categories.")
    print(f"- {inv_path}")
    print(f"- {map_path}")
    print("\nNext: fill in 'mapped_label' (collapse into ~5 terms), and set 'mapped_slug' once for those.")

if __name__ == "__main__":
    main()
