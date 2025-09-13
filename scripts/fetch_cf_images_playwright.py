#!/usr/bin/env python3
# Requires: pip install playwright
# Then once: python -m playwright install chromium
#
# Usage example:
#   python fetch_cf_images_playwright.py \
#     --urls https://endlessknots.netage.com/.a/6a00df3523b1d08834010535f0ca47970b-popup \
#     --out-dir dump/images/cf_raw
#
# or from CSV (first column header 'url'):
#   python fetch_cf_images_playwright.py \
#     --csv work/image_urls_unique.csv --limit 5 \
#     --out-dir dump/images/cf_raw
#
# It writes one file per URL (best-guess extension), prints OK/FAIL lines,
# and exits nonzero if any fail. You can later hash/dedup these outputs
# with your existing pipeline.

import argparse, asyncio, csv, hashlib, mimetypes, os, re, sys
from pathlib import Path
from urllib.parse import urlsplit, unquote
from playwright.async_api import async_playwright

SAFE = re.compile(r"[^A-Za-z0-9._-]+")

def safe_name_from_url(u: str) -> str:
    path = urlsplit(u).path
    name = unquote(path.rsplit("/", 1)[-1]) or "file"
    name = name.split("?")[0].split("#")[0]
    return SAFE.sub("_", name)[:180]

def guess_ext(u: str, content_type: str | None) -> str:
    if content_type:
        ct = content_type.split(";",1)[0].strip().lower()
        if ct in {"image/jpeg","image/jpg"}: return ".jpg"
        if ct in {"image/png"}: return ".png"
        if ct in {"image/gif"}: return ".gif"
        if ct in {"image/webp"}: return ".webp"
        if ct in {"image/bmp"}: return ".bmp"
        if ct in {"image/svg+xml"}: return ".svg"
        if ct in {"image/tiff"}: return ".tif"
        ext = mimetypes.guess_extension(ct) or ""
        if ext == ".jpeg": return ".jpg"
        if ext: return ext
    lp = urlsplit(u).path.lower()
    for ext in (".jpg",".jpeg",".png",".gif",".webp",".bmp",".svg",".tif",".tiff"):
        if lp.endswith(ext): return ".jpg" if ext == ".jpeg" else ext
    return ".bin"

async def fetch_one(context, url: str, out_dir: Path, referer: str | None = None, timeout_ms: int = 30000):
    # First try APIRequestContext (fast path)
    headers = {}
    if referer:
        headers["referer"] = referer
    r = await context.request.get(url, headers=headers, timeout=timeout_ms)
    if r.ok:
        ct = r.headers.get("content-type", "")
        data = await r.body()
        base = safe_name_from_url(url)
        ext = guess_ext(url, ct)
        out_path = out_dir / (base if base.endswith(ext) else base + ext)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_bytes(data)
        return True, out_path.name, r.status, ct

    # If blocked (e.g., 403 challenge), open a real page to solve challenge
    page = await context.new_page()
    # Use a plausible referer from same site (helps some CF configs)
    if referer:
        await page.goto(referer, wait_until="domcontentloaded")
    # Now go to the image URL directly; CF will interpose the challenge if needed
    resp = await page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
    # give CF’s JS time to run and drop cookies
    # patch
    await page.wait_for_timeout(8000)
    status = resp.status if resp else 0
    # end patch
    if resp and (200 <= status < 300):
        data = await resp.body()
        ct = resp.headers.get("content-type", "")
        base = safe_name_from_url(url)
        ext = guess_ext(url, ct)
        out_path = out_dir / (base if base.endswith(ext) else base + ext)
        out_path.write_bytes(data)
        await page.close()
        return True, out_path.name, status, ct

    # One more try via request API after challenge solved
    r2 = await context.request.get(url, headers=headers, timeout=timeout_ms)
    if r2.ok:
        ct = r2.headers.get("content-type", "")
        data = await r2.body()
        base = safe_name_from_url(url)
        ext = guess_ext(url, ct)
        out_path = out_dir / (base if base.endswith(ext) else base + ext)
        out_path.write_bytes(data)
        await page.close()
        return True, out_path.name, r2.status, ct

    await page.close()
    text = ""
    try:
        text = await r2.text()
    except Exception:
        pass
    return False, text[:200], r2.status if r2 else status, ""

async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--urls", nargs="*", help="One or more image URLs to fetch.")
    ap.add_argument("--csv", help="CSV with a 'url' column (e.g., image_urls_unique.csv).")
    ap.add_argument("--limit", type=int, default=3, help="Only first N URLs (testing).")
    ap.add_argument("--out-dir", default="dump/images/cf_raw", help="Output directory.")
    ap.add_argument("--referer", default="https://endlessknots.netage.com/", help="Referer to set (same site).")
    ap.add_argument("--timeout-ms", type=int, default=30000)
    args = ap.parse_args()

    urls = []
    if args.urls:
        urls.extend(args.urls)
    if args.csv:
        with open(args.csv, newline="", encoding="utf-8") as f:
            rdr = csv.DictReader(f)
            # accept either 'url' or 'chosen_download_url'
            field = "url" if "url" in rdr.fieldnames else ("chosen_download_url" if "chosen_download_url" in rdr.fieldnames else None)
            if not field:
                print("CSV must have 'url' or 'chosen_download_url' column.", file=sys.stderr)
                sys.exit(1)
            for row in rdr:
                u = (row.get(field) or "").strip()
                if u: urls.append(u)
                if args.limit and len(urls) >= args.limit: break

    if not urls:
        print("No URLs provided.", file=sys.stderr)
        sys.exit(1)

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    ok = fail = 0
    async with async_playwright() as pw:
        # Persistent context mimics a real browser (user data dir keeps cookies if you repeat runs)
        browser = await pw.chromium.launch(headless=True)
        context = await browser.new_context(  # default desktop UA and viewport are fine
            ignore_https_errors=False
        )

        # Warm up on the site root (often solves CF once per session)
        try:
            page = await context.new_page()
            await page.goto(args.referer, wait_until="domcontentloaded", timeout=args.timeout_ms)
            await page.close()
        except Exception:
            pass

        for i, u in enumerate(urls, 1):
            try:
                success, info, status, ct = await fetch_one(context, u, out_dir, referer=args.referer, timeout_ms=args.timeout_ms)
                if success:
                    ok += 1
                    print(f"[{i}/{len(urls)}] OK  {status}  {u}  -> {info}")
                else:
                    fail += 1
                    print(f"[{i}/{len(urls)}] FAIL {status} {u}  note={info}")
            except Exception as e:
                fail += 1
                print(f"[{i}/{len(urls)}] FAIL 0 {u}  err={e}")

        await context.close()
        await browser.close()

    print(f"Done. Success={ok}  Fail={fail}")
    sys.exit(0 if fail == 0 else 2)

if __name__ == "__main__":
    asyncio.run(main())
