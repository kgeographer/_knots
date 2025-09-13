#!/usr/bin/env python3
"""
04c_fetch_images_wayback.py

Try to recover TypePad images from the Internet Archive (Wayback Machine).

Inputs:
  --occ     work/image_manifest.csv        # per-occurrence rows from step 03
  --uniq    work/image_urls_unique.csv     # unique chosen_download_url from step 03

Outputs:
  - dump/images/wayback_raw/               # raw files from IA
  - dump/images/dedup_wayback/             # canonical <sha1>.<ext>
  - work/url_map_wayback.csv               # original_url -> new_filename/new_url (Wayback URL) + meta
  - work/wayback_failures.csv              # any URLs not found or download errors
  - work/wayback_fetch.log                 # detailed log

Notes:
  - We use the CDX API to find a 200 snapshot, preferring the newest.
  - Then fetch via: https://web.archive.org/web/{timestamp}id_/{original_url}
  - Set --host-base-url if you intend to rehost later; otherwise new_url is the IA URL.
"""

from __future__ import annotations
import argparse, csv, hashlib, mimetypes, os, re, sys, time, json
from pathlib import Path
from typing import Dict, Optional, Tuple
from urllib.parse import quote, urlsplit, unquote
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

SAFE_RE = re.compile(r"[^A-Za-z0-9._-]+")

def safe_name(url_path: str) -> str:
    name = unquote(url_path.split("?")[0].split("#")[0].rsplit("/",1)[-1] or "file")
    return SAFE_RE.sub("_", name)[:180]

def guess_ext(url: str, content_type: Optional[str]) -> str:
    mapping = {
        "image/jpeg": ".jpg","image/jpg": ".jpg","image/png": ".png","image/gif": ".gif",
        "image/webp": ".webp","image/bmp": ".bmp","image/svg+xml": ".svg","image/tiff": ".tif",
    }
    if content_type:
        ct = content_type.split(";",1)[0].strip().lower()
        if ct in mapping: return mapping[ct]
        ext = mimetypes.guess_extension(ct) or ""
        if ext == ".jpeg": return ".jpg"
        if ext: return ext
    lp = urlsplit(url).path.lower()
    for ext in (".jpg",".jpeg",".png",".gif",".webp",".bmp",".svg",".tif",".tiff"):
        if lp.endswith(ext): return ".jpg" if ext == ".jpeg" else ext
    return ".bin"

def sha1_bytes(b: bytes) -> str:
    h = hashlib.sha1(); h.update(b); return h.hexdigest()

def http_get(url: str, timeout: int = 20, headers: Dict[str,str] | None = None) -> tuple[int, bytes, Dict[str,str], str]:
    try:
        req = Request(url, headers=headers or {"User-Agent":"EK-Importer/Wayback/1.0"}, method="GET")
        with urlopen(req, timeout=timeout) as resp:
            status = getattr(resp, "status", 200)
            data = resp.read()
            hdrs = {k.lower(): v for k, v in resp.headers.items()}
            ctype = hdrs.get("content-type","")
            return status, data, hdrs, ctype
    except HTTPError as e:
        return e.code, b"", {}, ""
    except Exception as e:
        return 0, b"", {}, f"ERR {e.__class__.__name__}: {e}"

def find_wayback_best(url: str, timeout: int = 20) -> Optional[str]:
    """
    Query CDX for a 200 snapshot. Prefer the newest snapshot with status 200.
    API: https://web.archive.org/cdx/search/cdx?url=<url>&output=json&filter=statuscode:200&collapse=digest
    """
    cdx = f"https://web.archive.org/cdx/search/cdx?url={quote(url, safe='')}&output=json&filter=statuscode:200&collapse=digest"
    status, data, _, _ = http_get(cdx, timeout=timeout)
    if status != 200 or not data:
        return None
    try:
        j = json.loads(data.decode("utf-8", "ignore"))
        # First row is header; subsequent rows: [urlkey, timestamp, original, mimetype, statuscode, digest, length]
        rows = j[1:] if isinstance(j, list) and j else []
        if not rows: return None
        # pick the newest timestamp
        rows.sort(key=lambda r: r[1], reverse=True)
        ts = rows[0][1]
        # id_ delivers the original bytes without Wayback HTML wrapper
        wb_url = f"https://web.archive.org/web/{ts}id_/{url}"
        return wb_url
    except Exception:
        return None

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--occ", required=True, help="work/image_manifest.csv")
    ap.add_argument("--uniq", required=True, help="work/image_urls_unique.csv")
    ap.add_argument("--out-map", default="work/url_map_wayback.csv")
    ap.add_argument("--raw-dir", default="dump/images/wayback_raw")
    ap.add_argument("--dedup-dir", default="dump/images/dedup_wayback")
    ap.add_argument("--fail-csv", default="work/wayback_failures.csv")
    ap.add_argument("--log-file", default="work/wayback_fetch.log")
    ap.add_argument("--limit", type=int, default=0, help="only first N (testing)")
    ap.add_argument("--timeout", type=int, default=20)
    ap.add_argument("--host-base-url", default="", help="If provided, also map to your host (new_url). Otherwise use Wayback URL.")
    args = ap.parse_args()

    raw_dir = Path(args.raw_dir); raw_dir.mkdir(parents=True, exist_ok=True)
    dedup_dir = Path(args.dedup_dir); dedup_dir.mkdir(parents=True, exist_ok=True)
    Path(args.out_map).parent.mkdir(parents=True, exist_ok=True)
    Path(args.fail_csv).parent.mkdir(parents=True, exist_ok=True)
    Path(args.log_file).parent.mkdir(parents=True, exist_ok=True)

    host_prefix = args.host_base_url.rstrip("/") if args.host_base_url.lower().startswith("http") else ""

    # load unique chosen URLs
    unique: list[tuple[str,str]] = []
    with open(args.uniq, newline="", encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        if "chosen_download_url" not in rdr.fieldnames:
            print("ERROR: chosen_download_url missing in image_urls_unique.csv", file=sys.stderr); sys.exit(1)
        for row in rdr:
            u = (row.get("chosen_download_url") or "").strip()
            k = (row.get("kind") or "").strip()
            if u: unique.append((u,k))
            if args.limit and len(unique) >= args.limit: break

    if not unique:
        print("No URLs to process.")
        return

    # Build chosen->(filename,new_url,sha1,bytes,ctype) via Wayback
    chosen_map: Dict[str, tuple[str,str,str,int,str]] = {}
    fails = []

    with open(args.log_file, "w", encoding="utf-8") as log:
        log.write(f"START wayback fetch — urls={len(unique)}\n")

    ok = 0
    for i, (u, kind) in enumerate(unique, 1):
        # find wayback snapshot
        wb = find_wayback_best(u, timeout=args.timeout)
        if not wb:
            fails.append((u, 0, "no_wayback_snapshot"))
            with open(args.log_file, "a", encoding="utf-8") as log:
                log.write(f"[{i}] MISS  {u}  (no snapshot)\n")
            continue

        # fetch bytes from Wayback
        status, data, hdrs, ctype = http_get(wb, timeout=args.timeout, headers={"User-Agent":"EK-Importer/Wayback/1.0"})
        if status != 200 or not data:
            fails.append((u, status, f"wb_fetch_fail:{wb}"))
            with open(args.log_file, "a", encoding="utf-8") as log:
                log.write(f"[{i}] FAIL  {u}  wb={wb}  status={status}\n")
            continue

        digest = sha1_bytes(data)
        ext = guess_ext(u, ctype)
        raw_name = safe_name(urlsplit(u).path)
        if not raw_name.endswith(ext): raw_name += ext
        raw_path = raw_dir / raw_name
        if not raw_path.exists():
            raw_path.write_bytes(data)
        dedup_name = f"{digest}{ext}"
        dedup_path = dedup_dir / dedup_name
        if not dedup_path.exists():
            dedup_path.write_bytes(data)

        new_url = f"{host_prefix}/{dedup_name}" if host_prefix else wb  # if you will rehost later, fill host_base_url
        chosen_map[u] = (dedup_name, new_url, digest, len(data), ctype)
        ok += 1
        print(f"[{i}/{len(unique)}] OK {status}  {u} -> {dedup_name}")

    print(f"Wayback successes: {ok}  misses: {len(fails)}  (details in {args.log_file})")

    # Expand mapping to ALL original URLs (thumb/full/inferred/chosen) per occurrence
    expanded: Dict[str, tuple[str,str,str,int,str,str]] = {}
    with open(args.occ, newline="", encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        for row in rdr:
            chosen = (row.get("chosen_download_url") or "").strip()
            kind = (row.get("chosen_kind") or "").strip()
            if not chosen or chosen not in chosen_map: continue
            filename, new_url, sha1, bytelen, ctype = chosen_map[chosen]
            for field in ("thumb_url","full_url","inferred_full_url","chosen_download_url"):
                orig = (row.get(field) or "").strip()
                if not orig: continue
                expanded.setdefault(orig, (filename, new_url, sha1, bytelen, ctype, kind))

    # Write url_map_wayback.csv
    with open(args.out_map, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["original_url","new_filename","new_url","kind","sha1","bytes","content_type"])
        for orig, (filename, new_url, sha1, bytelen, ctype, kind) in sorted(expanded.items()):
            w.writerow([orig, filename, new_url, kind, sha1, bytelen, ctype])

    # Failures CSV
    with open(args.fail_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["url","status","note"])
        for u, s, note in fails:
            w.writerow([u, s, note])

    print(f"Wrote mapping: {args.out_map}  and failures: {args.fail_csv}")

if __name__ == "__main__":
    from pathlib import Path
    main()
