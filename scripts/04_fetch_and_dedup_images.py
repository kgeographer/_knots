#!/usr/bin/env python3
"""
04_fetch_and_dedupe_images.py

Download and de-duplicate images referenced by the image manifests:

Inputs:
  --occ   work/image_manifest.csv        # per-occurrence rows from step 03
  --uniq  work/image_urls_unique.csv     # unique chosen_download_url list from step 03

Outputs:
  - Files saved under:
      <img-dir>/downloads/    # raw downloads (best-effort original filenames)
      <img-dir>/dedup/        # content-addressed files named <sha1>.<ext>
  - work/url_map.csv          # mapping of ORIGINAL URL -> new_filename/new_url (covers thumb/full/inferred)
  - work/download_failures.csv  # any URLs that failed to download with status/error

Notes:
  - Prefers Content-Type header to infer extension, else falls back to URL path suffix, else ".bin".
  - Dedup keyed on SHA-1 of content bytes.
  - Optional --host-base-url to generate public URLs for the rewritten HTML step.
"""

from __future__ import annotations
import argparse
import csv
import hashlib
import mimetypes
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Tuple, Optional
from urllib.parse import urlsplit, unquote
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

# ---------- Helpers ----------

SAFE_NAME_RE = re.compile(r"[^A-Za-z0-9._-]+")

# Common content-type -> extension overrides (mimetypes can be spotty)
CT_EXT = {
    "image/jpeg": ".jpg",
    "image/jpg":  ".jpg",
    "image/png":  ".png",
    "image/gif":  ".gif",
    "image/webp": ".webp",
    "image/bmp":  ".bmp",
    "image/svg+xml": ".svg",
}

def safe_filename(name: str) -> str:
    name = unquote(name.strip())
    name = name.split("?")[0].split("#")[0]
    name = name.rsplit("/", 1)[-1] or "file"
    name = SAFE_NAME_RE.sub("_", name)
    return name[:180]  # keep it sane

def guess_ext(url: str, content_type: Optional[str]) -> str:
    # Prefer explicit header mapping
    if content_type:
        ct = content_type.split(";")[0].strip().lower()
        if ct in CT_EXT:
            return CT_EXT[ct]
        # mimetypes fallback
        ext = mimetypes.guess_extension(ct) or ""
        if ext:
            return ext

    # URL-based guess
    path = urlsplit(url).path.lower()
    for ext in (".jpg",".jpeg",".png",".gif",".webp",".bmp",".svg",".tif",".tiff"):
        if path.endswith(ext):
            return ext if ext != ".jpeg" else ".jpg"
    return ".bin"

def sha1_bytes(data: bytes) -> str:
    h = hashlib.sha1()
    h.update(data)
    return h.hexdigest()

@dataclass
class DownloadResult:
    url: str
    ok: bool
    status: int
    sha1: str = ""
    ext: str = ""
    bytes_len: int = 0
    error: str = ""
    content_type: str = ""
    dedup_filename: str = ""  # <sha1><ext>

# ---------- Download logic ----------

def fetch_url(url: str, timeout: int = 20, retries: int = 2, ua: str = None) -> DownloadResult:
    if not ua:
        ua = "Mozilla/5.0 (compatible; EK-Importer/1.0)"
    headers = {"User-Agent": ua}

    last_err = ""
    for attempt in range(retries + 1):
        try:
            req = Request(url, headers=headers, method="GET")
            with urlopen(req, timeout=timeout) as resp:
                status = getattr(resp, "status", 200)
                ctype = resp.headers.get("Content-Type", "")
                data = resp.read()
                digest = sha1_bytes(data)
                ext = guess_ext(url, ctype)
                return DownloadResult(
                    url=url, ok=True, status=status, sha1=digest, ext=ext,
                    bytes_len=len(data), content_type=ctype,
                    dedup_filename=f"{digest}{ext}"
                )
        except HTTPError as e:
            last_err = f"HTTPError {e.code}: {e.reason}"
            if 400 <= e.code < 500:
                # likely not recoverable
                return DownloadResult(url=url, ok=False, status=e.code, error=last_err)
            # else retry
        except URLError as e:
            last_err = f"URLError: {e.reason}"
        except Exception as e:
            last_err = f"Error: {e.__class__.__name__}: {e}"

        if attempt < retries:
            time.sleep(1.0 * (attempt + 1))  # backoff
        else:
            return DownloadResult(url=url, ok=False, status=0, error=last_err)

    # Shouldn't get here
    return DownloadResult(url=url, ok=False, status=0, error="unknown")

# ---------- Main ----------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--occ", required=True, help="work/image_manifest.csv (per-occurrence)")
    ap.add_argument("--uniq", required=True, help="work/image_urls_unique.csv (unique chosen_download_url)")
    ap.add_argument("--out-map", required=True, help="work/url_map.csv (output mapping for rewrite step)")
    ap.add_argument("--img-dir", default="dump/images", help="Base image directory (default: dump/images)")
    ap.add_argument("--host-base-url", default="", help="Optional base URL for hosted images (e.g., https://your.org/images)")
    ap.add_argument("--workers", type=int, default=4, help="Parallel download workers (default: 4)")
    ap.add_argument("--timeout", type=int, default=20, help="HTTP timeout seconds (default: 20)")
    ap.add_argument("--retries", type=int, default=2, help="HTTP retries (default: 2)")
    args = ap.parse_args()

    occ_path = Path(args.occ)
    uniq_path = Path(args.uniq)
    out_map_path = Path(args.out_map)
    out_map_path.parent.mkdir(parents=True, exist_ok=True)

    img_base = Path(args.img_dir)
    downloads_dir = img_base / "downloads"
    dedup_dir = img_base / "dedup"
    downloads_dir.mkdir(parents=True, exist_ok=True)
    dedup_dir.mkdir(parents=True, exist_ok=True)

    host_base = args.host_base_url.rstrip("/")
    if host_base and host_base.lower().startswith("http"):
        host_prefix = host_base
    else:
        host_prefix = ""  # allow local-only mapping; fill later if desired

    # --- Read unique URL list
    unique_urls: Dict[str, str] = {}  # url -> kind
    with uniq_path.open("r", encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        for row in rdr:
            url = (row.get("chosen_download_url") or "").strip()
            if not url:
                continue
            kind = (row.get("kind") or "").strip()
            unique_urls[url] = kind

    if not unique_urls:
        print("[fetch] No URLs to download. Is work/image_urls_unique.csv empty?", file=sys.stderr)
        sys.exit(0)

    # --- Download in parallel
    results: Dict[str, DownloadResult] = {}
    failures = []

    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as ex:
        futs = {
            ex.submit(fetch_url, url, args.timeout, args.retries): url
            for url in unique_urls.keys()
        }
        for fut in as_completed(futs):
            url = futs[fut]
            res = fut.result()
            results[url] = res
            if not res.ok:
                failures.append(res)

    # --- Save files (raw + dedup) for successes
    # Raw filename attempts to mirror the URL's basename; dedup is <sha1>.<ext>
    raw_written = 0
    dedup_written = 0
    for url, res in results.items():
        if not res.ok:
            continue
        # Don’t re-download; but we do need the body bytes to write.
        # fetch_url already read bytes; but we didn't keep them to save memory.
        # Re-fetch quickly to write files (fast path with no hashing now).
        # Given we have the dedup filename, we only need to write if files are missing.
        need_raw = True
        need_dedup = not (dedup_dir / res.dedup_filename).exists()

        # write dedup (with a tiny re-fetch just once if needed)
        if need_dedup or need_raw:
            # re-fetch one more time to get the bytes
            refetch = fetch_url(url, args.timeout, 0)
            if not refetch.ok:
                failures.append(refetch)
                continue
            data = None
            # We need the bytes; re-run with urlopen directly for speed:
            try:
                req = Request(url, headers={"User-Agent": "Mozilla/5.0 (compatible; EK-Importer/1.0)"}, method="GET")
                with urlopen(req, timeout=args.timeout) as resp:
                    data = resp.read()
            except Exception as e:
                failures.append(DownloadResult(url=url, ok=False, status=0, error=f"refetch-bytes: {e}"))
                continue

            # dedup write
            dedup_path = dedup_dir / res.dedup_filename
            if not dedup_path.exists():
                with open(dedup_path, "wb") as f:
                    f.write(data)
                dedup_written += 1

            # raw write
            raw_name = safe_filename(urlsplit(url).path)
            if not raw_name:
                raw_name = res.dedup_filename
            raw_path = downloads_dir / raw_name
            if not raw_path.exists():
                with open(raw_path, "wb") as f:
                    f.write(data)
                raw_written += 1

    print(f"[fetch] URLs: {len(unique_urls)} | dedup files written: {dedup_written} | raw files written: {raw_written}")
    if failures:
        fail_path = out_map_path.parent / "download_failures.csv"
        with fail_path.open("w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["url","status","error"])
            for fr in failures:
                w.writerow([fr.url, fr.status, fr.error])
        print(f"[fetch] Failures: {len(failures)} → {fail_path}")

    # --- Build base mapping: chosen_download_url -> new file
    chosen_to_new: Dict[str, Tuple[str, str, str, int, str]] = {}  # url -> (filename, new_url, sha1, bytes, content_type)
    for url, res in results.items():
        if not res.ok:
            continue
        filename = res.dedup_filename
        new_url = f"{host_prefix}/{filename}" if host_prefix else ""
        chosen_to_new[url] = (filename, new_url, res.sha1, res.bytes_len, res.content_type)

    # --- Expand mapping to ALL originals seen in occ file (thumb/full/inferred/chosen)
    #     Each original URL maps to the SAME new file as that row's chosen_download_url.
    mapping: Dict[str, Tuple[str, str, str, int, str, str]] = {}  # original -> (filename, new_url, sha1, bytes, ctype, kind)
    with occ_path.open("r", encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        for row in rdr:
            chosen = (row.get("chosen_download_url") or "").strip()
            kind   = (row.get("chosen_kind") or "").strip()
            if not chosen or chosen not in chosen_to_new:
                continue
            filename, new_url, sha1, bytelen, ctype = chosen_to_new[chosen]
            # Map any present URLs in this row to the same target
            for field in ("thumb_url","full_url","inferred_full_url","chosen_download_url"):
                original = (row.get(field) or "").strip()
                if not original:
                    continue
                # Don't overwrite if already present (first win)
                mapping.setdefault(original, (filename, new_url, sha1, bytelen, ctype, kind))

    # --- Write url_map.csv
    with out_map_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["original_url","new_filename","new_url","kind","sha1","bytes","content_type"])
        for original, (filename, new_url, sha1, bytelen, ctype, kind) in sorted(mapping.items()):
            w.writerow([original, filename, new_url, kind, sha1, bytelen, ctype])

    print(f"[fetch] Wrote mapping: {out_map_path} ({len(mapping)} rows)")
    print("[fetch] Done.")

if __name__ == "__main__":
    main()
