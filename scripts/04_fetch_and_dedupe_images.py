#!/usr/bin/env python3
"""
04_fetch_and_dedupe_images.py  — single, robust, verbose downloader

python scripts/04_fetch_and_dedupe_images.py \
  --occ work/image_manifest.csv \
  --uniq work/image_urls_unique.csv \
  --out-map work/url_map.csv \
  --img-dir dump/images \
  --host-base-url "https://YOURDOMAIN.example/images" \
  --workers 6 --timeout 20 --retries 2 --verbose --limit 10 \
  --headers-file headers.txt.

Inputs:
  --occ     work/image_manifest.csv        # per-occurrence rows (thumb/full/inferred/chosen)
  --uniq    work/image_urls_unique.csv     # deduped chosen_download_url list
Outputs:
  - dump/images/downloads/   (raw filenames for spot checks)
  - dump/images/dedup/       (<sha1>.<ext> canonical files)
  - work/url_map.csv         (original_url -> new_filename/new_url + meta)
  - work/download_failures.csv
  - work/fetch.log

Notes:
  - Uses certifi CA by default. Optionally --no-verify or --force-http-on-ssl-error.
  - Add --headers-file to inject UA/Cookie etc. for sites behind bot protection.
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
import ssl
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional, Tuple
from urllib.parse import urlsplit, unquote
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

# __cf_bm=QguM6unrj.zUIErTOGgxFQg85uBqgpw.pLqssOTUiLA-1757757274-1.0.1.1-tXl1k_DOcndKsvdqfX84r9sHry2zThYCHfANehu.aP6ow5VDbPo.sp4AJLEJuGTbK5NKF0DkxlkCgrBFvk4eUsu6FG6R.u4A146akUO5L1s
# cf_clearance=GpBbjYZcyR_AM2UK.oayThFmxEhTfys.BaXAQYB85sI-1757757274-1.2.1.1-uzfYaLWBWmkNknUDg4iuydnsIcgDopnZMY_csDDMJaNTWX9yyeyuby9sWHiBRpT.KG9pO6QFQQgkrYfT2emU_NZAsQ5AcuUjENZWzq0IfVb7Pa3ZYjhrKdfsh3kN64PKbtG96XvU31UTvlqDTu.HJCKx4PhKU87QlmQ0JEkEyAgGj6am6L1idM5UXXxGXFLRRcaxH7abGwi8jRgDTldQym6pXWA4UxyuWzVEKv3Z6eQ


# ---------- Certs ----------
try:
    import certifi
    CERTIFI_CA = certifi.where()
except Exception:
    CERTIFI_CA = None

def build_ssl_context(no_verify: bool) -> ssl.SSLContext:
    if no_verify:
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        return ctx
    if CERTIFI_CA:
        return ssl.create_default_context(cafile=CERTIFI_CA)
    return ssl.create_default_context()

# ---------- Helpers ----------
SAFE_NAME_RE = re.compile(r"[^A-Za-z0-9._-]+")
CT_EXT = {
    "image/jpeg": ".jpg", "image/jpg": ".jpg", "image/png": ".png", "image/gif": ".gif",
    "image/webp": ".webp", "image/bmp": ".bmp", "image/svg+xml": ".svg", "image/tiff": ".tif",
}

def safe_filename(url_path: str) -> str:
    name = unquote(url_path.strip()).split("?")[0].split("#")[0]
    name = name.rsplit("/", 1)[-1] or "file"
    name = SAFE_NAME_RE.sub("_", name)
    return name[:180]

def guess_ext(url: str, content_type: Optional[str]) -> str:
    if content_type:
        ct = content_type.split(";")[0].strip().lower()
        if ct in CT_EXT: return CT_EXT[ct]
        ext = mimetypes.guess_extension(ct) or ""
        if ext: return ".jpg" if ext == ".jpeg" else ext
    path = urlsplit(url).path.lower()
    for ext in (".jpg",".jpeg",".png",".gif",".webp",".bmp",".svg",".tif",".tiff"):
        if path.endswith(ext): return ".jpg" if ext == ".jpeg" else ext
    return ".bin"

def sha1_bytes(data: bytes) -> str:
    h = hashlib.sha1(); h.update(data); return h.hexdigest()

def load_headers_file(path: Optional[str]) -> Dict[str, str]:
    headers: Dict[str,str] = {}
    if not path: return headers
    p = Path(path)
    if not p.exists(): return headers
    for line in p.read_text(encoding="utf-8").splitlines():
        if not line.strip() or line.strip().startswith("#"): continue
        if ":" not in line: continue
        k, v = line.split(":", 1)
        headers[k.strip()] = v.strip()
    return headers

# ---------- Result ----------
@dataclass
class DLResult:
    url: str
    ok: bool
    status: int
    sha1: str = ""
    ext: str = ""
    bytes_len: int = 0
    error: str = ""
    content_type: str = ""
    data: bytes | None = None
    used_http_fallback: bool = False

# ---------- Fetch ----------
def fetch_once(url: str, timeout: int, headers: Dict[str,str], ctx: ssl.SSLContext) -> DLResult:
    try:
        req = Request(url, headers=headers or {"User-Agent": "EK-Importer/1.2"}, method="GET")
        with urlopen(req, timeout=timeout, context=ctx) as resp:
            status = getattr(resp, "status", 200)
            ctype = resp.headers.get("Content-Type", "")
            data = resp.read()
            digest = sha1_bytes(data)
            ext = guess_ext(url, ctype)
            return DLResult(url, True, status, digest, ext, len(data), "", ctype, data)
    except HTTPError as e:
        # We can still inspect headers/body on HTTPError if needed
        return DLResult(url, False, e.code, error=f"HTTP {e.code} {e.reason}")
    except ssl.SSLError as e:
        return DLResult(url, False, 0, error=f"SSL error: {e}")
    except URLError as e:
        return DLResult(url, False, 0, error=f"URL error: {e.reason}")
    except Exception as e:
        return DLResult(url, False, 0, error=f"{e.__class__.__name__}: {e}")

def fetch(url: str, timeout: int, retries: int, headers: Dict[str,str],
          no_verify: bool, force_http_fallback: bool) -> DLResult:
    ctx = build_ssl_context(no_verify)
    last_err = ""
    for attempt in range(retries + 1):
        res = fetch_once(url, timeout, headers, ctx)
        if res.ok:
            return res
        last_err = res.error
        # SSL fallback: try http:// if requested and original was https://
        if "SSL" in (res.error or "") and force_http_fallback and url.lower().startswith("https://"):
            http_url = "http://" + url.split("://", 1)[1]
            res2 = fetch_once(http_url, timeout, headers, ctx)
            if res2.ok:
                res2.used_http_fallback = True
                return res2
            last_err = f"{res.error} | http-fallback: {res2.error}"
        time.sleep(1 + attempt)  # simple backoff
    return DLResult(url, False, 0, error=last_err)

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
    ap.add_argument("--limit", type=int, default=0, help="Only process first N URLs (for testing)")
    ap.add_argument("--verbose", action="store_true", help="Print per-URL log lines to stdout")
    ap.add_argument("--log-file", default="work/fetch.log", help="Path to detailed log file")
    ap.add_argument("--headers-file", default="", help="Optional headers file (Key: Value lines)")
    ap.add_argument("--no-verify", action="store_true", help="Disable TLS verification (last resort)")
    ap.add_argument("--force-http-on-ssl-error", action="store_true",
                    help="On SSL failure, retry once with http:// instead of https://")
    args = ap.parse_args()

    # Paths
    img_base = Path(args.img_dir)
    downloads_dir = img_base / "downloads"
    dedup_dir = img_base / "dedup"
    downloads_dir.mkdir(parents=True, exist_ok=True)
    dedup_dir.mkdir(parents=True, exist_ok=True)
    Path(args.out_map).parent.mkdir(parents=True, exist_ok=True)
    Path(args.log_file).parent.mkdir(parents=True, exist_ok=True)

    # Hosted prefix
    host_prefix = args.host_base_url.rstrip("/") if args.host_base_url.lower().startswith("http") else ""

    # Load headers (UA/Cookie if provided)
    headers = load_headers_file(args.headers_file)

    # Load unique URLs
    unique: Dict[str, str] = {}
    with open(args.uniq, newline="", encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        if "chosen_download_url" not in rdr.fieldnames:
            print("ERROR: chosen_download_url column missing in work/image_urls_unique.csv", file=sys.stderr)
            sys.exit(1)
        for row in rdr:
            url = (row.get("chosen_download_url") or "").strip()
            if not url: continue
            unique[url] = (row.get("kind") or "").strip()
            if args.limit and len(unique) >= args.limit: break

    total = len(unique)
    if total == 0:
        print("No URLs to fetch (image_urls_unique.csv empty or limited to 0).")
        return

    # Logging helpers
    def log_line(s: str):
        if args.verbose: print(s)
        with open(args.log_file, "a", encoding="utf-8") as log:
            log.write(s + "\n")

    # Start log
    with open(args.log_file, "w", encoding="utf-8") as log:
        log.write(f"START fetch — urls={total} workers={args.workers} timeout={args.timeout} retries={args.retries}\n")
        if args.no_verify: log.write("WARNING: TLS verification disabled (--no-verify)\n")
        if args.headers_file: log.write(f"Using headers from: {args.headers_file}\n")

    print(f"Fetching {total} image(s) with {args.workers} worker(s)…")
    if args.no_verify:
        print("WARNING: TLS verification disabled (--no-verify). Use only for testing.")

    # Download
    results: Dict[str, DLResult] = {}
    started = time.time()
    done = 0
    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as ex:
        futs = {
            ex.submit(fetch, url, args.timeout, args.retries, headers, args.no_verify, args.force_http_on_ssl_error): url
            for url in unique.keys()
        }
        for fut in as_completed(futs):
            url = futs[fut]
            res = fut.result()
            results[url] = res
            done += 1
            # progress
            short = (url[:70] + "…") if len(url) > 70 else url
            status_txt = "OK" if res.ok else "FAIL"
            print(f"\r[{done}/{total}] {status_txt} {short}", end="", flush=True)
            if res.ok:
                extra = " via-http" if res.used_http_fallback else ""
                log_line(f"OK {res.status} {url} bytes={res.bytes_len} ct={res.content_type} sha1={res.sha1}{extra}")
            else:
                log_line(f"FAIL {url} err={res.error}")

    print()  # newline
    dur = time.time() - started
    ok_count = sum(1 for r in results.values() if r.ok)
    fail_count = total - ok_count
    print(f"Completed in {dur:.1f}s. Success: {ok_count}  Fail: {fail_count}")

    # Failures CSV
    fail_path = Path(args.out_map).parent / "download_failures.csv"
    with fail_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f); w.writerow(["url","status","error"])
        for r in results.values():
            if not r.ok: w.writerow([r.url, r.status, r.error])
    print(f"Failures written: {fail_path}")

    # Write files for successes + build chosen->new mapping
    raw_written = dedup_written = 0
    chosen_to_new: Dict[str, Tuple[str,str,str,int,str]] = {}
    for url, r in results.items():
        if not r.ok or r.data is None: continue
        dedup_name = f"{r.sha1}{r.ext}"
        dedup_path = dedup_dir / dedup_name
        if not dedup_path.exists():
            with open(dedup_path, "wb") as f: f.write(r.data)
            dedup_written += 1
        raw_name = safe_filename(urlsplit(url).path) or dedup_name
        raw_path = downloads_dir / raw_name
        if not raw_path.exists():
            with open(raw_path, "wb") as f: f.write(r.data)
            raw_written += 1
        new_url = f"{host_prefix}/{dedup_name}" if host_prefix else ""
        chosen_to_new[url] = (dedup_name, new_url, r.sha1, r.bytes_len, r.content_type)

    print(f"Wrote dedup: {dedup_written} | raw: {raw_written}")

    # Expand mapping to ALL original URLs from occurrences CSV
    mapping: Dict[str, Tuple[str,str,str,int,str,str]] = {}
    with open(args.occ, newline="", encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        for row in rdr:
            chosen = (row.get("chosen_download_url") or "").strip()
            kind = (row.get("chosen_kind") or "").strip()
            if not chosen or chosen not in chosen_to_new: continue
            filename, new_url, sha1, bytelen, ctype = chosen_to_new[chosen]
            for field in ("thumb_url","full_url","inferred_full_url","chosen_download_url"):
                original = (row.get(field) or "").strip()
                if not original: continue
                mapping.setdefault(original, (filename, new_url, sha1, bytelen, ctype, kind))

    # url_map.csv
    with open(args.out_map, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["original_url","new_filename","new_url","kind","sha1","bytes","content_type"])
        for original, (filename, new_url, sha1, bytelen, ctype, kind) in sorted(mapping.items()):
            w.writerow([original, filename, new_url, kind, sha1, bytelen, ctype])

    print(f"Mapping written: {args.out_map} ({len(mapping)} rows)")
    print(f"Log: {args.log_file}")

if __name__ == "__main__":
    main()
