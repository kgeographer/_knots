#!/usr/bin/env python3
# Requires:
#   pip install playwright
#   python -m playwright install chrome
#
# What it does:
#   - Opens a REAL Chrome window (non-headless) with a persistent profile.
#   - You visit the site once in that window (if CF shows a challenge, let it pass).
#   - Then it loops through URLs from work/image_urls_unique.csv and fetches each file
#     using the same trusted session.
#   - Saves files to dump/images/cf_raw (and a dedup copy by sha1 in dump/images/dedup_cf).
#   - Emits work/cf_download_results.csv (url,status,filename,sha1,content_type,bytes,error).
#
#/ Usage example:
#   python scripts/04b_fetch_cf_images_persistent.py \
#       --uniq work/image_urls_unique.csv \
#       --out-dir dump/images/cf_raw \
#       --dedup-dir dump/images/dedup_cf \
#       --limit 25
# Notes:
#   - Run from the same machine/IP you’ll browse with.
#   - First run: the Chrome window will open; load https://endlessknots.netage.com/ once,
#     wait a few seconds if challenged. Then come back to the terminal and press Enter.
#   - You can re-run with --start-index N to resume where you left off.

from __future__ import annotations
import argparse, asyncio, csv, hashlib, mimetypes, os, re, sys, time
from pathlib import Path
from typing import Dict, Tuple, Optional
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
        mapping = {
            "image/jpeg": ".jpg", "image/jpg": ".jpg", "image/png": ".png",
            "image/gif": ".gif", "image/webp": ".webp", "image/bmp": ".bmp",
            "image/svg+xml": ".svg", "image/tiff": ".tif",
        }
        if ct in mapping: return mapping[ct]
        ext = mimetypes.guess_extension(ct) or ""
        if ext == ".jpeg": return ".jpg"
        if ext: return ext
    lp = urlsplit(u).path.lower()
    for ext in (".jpg",".jpeg",".png",".gif",".webp",".bmp",".svg",".tif",".tiff"):
        if lp.endswith(ext): return ".jpg" if ext == ".jpeg" else ext
    return ".bin"

def sha1_bytes(data: bytes) -> str:
    h = hashlib.sha1(); h.update(data); return h.hexdigest()

async def fetch_with_context(context, url: str, referer: str, timeout_ms: int) -> Tuple[bool, int, bytes, str]:
    """Try fast request API; if 403, open a page to let CF JS set cookies, then retry."""
    headers = {"referer": referer} if referer else {}
    r = await context.request.get(url, headers=headers, timeout=timeout_ms)
    if r.ok:
        return True, r.status, await r.body(), r.headers.get("content-type","")

    # fallback: load the URL in a visible page (managed challenge needs JS)
    page = await context.new_page()
    try:
        await page.goto(referer, wait_until="domcontentloaded", timeout=timeout_ms)
    except Exception:
        pass
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
        # give CF JS some time to run
        await page.wait_for_timeout(8000)
    except Exception:
        pass
    finally:
        try: await page.close()
        except Exception: pass

    # retry with request API once cookies are set
    r2 = await context.request.get(url, headers=headers, timeout=timeout_ms)
    if r2.ok:
        return True, r2.status, await r2.body(), r2.headers.get("content-type","")
    return False, r2.status, b"", r2.headers.get("content-type","")

async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--uniq", required=True, help="work/image_urls_unique.csv")
    ap.add_argument("--out-dir", default="dump/images/cf_raw")
    ap.add_argument("--dedup-dir", default="dump/images/dedup_cf")
    ap.add_argument("--results-csv", default="work/cf_download_results.csv")
    ap.add_argument("--referer", default="https://endlessknots.netage.com/")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--start-index", type=int, default=0, help="0-based index into the unique URL list to resume")
    ap.add_argument("--timeout-ms", type=int, default=30000)
    ap.add_argument("--profile-dir", default=str(Path.home()/".ek_chrome_profile"),
                    help="Persistent Chrome user data dir to keep cookies/session")
    args = ap.parse_args()

    # read URLs
    urls: list[str] = []
    kinds: Dict[str,str] = {}
    with open(args.uniq, newline="", encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        if "chosen_download_url" not in rdr.fieldnames:
            print("ERROR: chosen_download_url missing in CSV", file=sys.stderr); sys.exit(1)
        for row in rdr:
            u = (row.get("chosen_download_url") or "").strip()
            if not u: continue
            urls.append(u); kinds[u] = (row.get("kind") or "").strip()
    if args.limit: urls = urls[args.start_index:args.start_index+args.limit]
    else: urls = urls[args.start_index:]

    if not urls:
        print("No URLs to process."); return

    out_dir = Path(args.out_dir); out_dir.mkdir(parents=True, exist_ok=True)
    dedup_dir = Path(args.dedup_dir); dedup_dir.mkdir(parents=True, exist_ok=True)
    Path(args.results_csv).parent.mkdir(parents=True, exist_ok=True)

    ok = fail = 0
    started = time.time()

    async with async_playwright() as pw:
        # Use REAL Chrome with a persistent profile and a visible window
        browser = await pw.chromium.launch_persistent_context(
            user_data_dir=args.profile_dir,
            channel="chrome",
            headless=False,
            viewport={"width": 1280, "height": 900}
        )

        # One-time warmup: open root in the real window; you may see CF challenge once.
        page = await browser.new_page()
        print("Opening site root in real Chrome…")
        try:
            await page.goto(args.referer, wait_until="domcontentloaded", timeout=args.timeout_ms)
        except Exception:
            pass
        print("If a challenge appears in the Chrome window, wait until it clears.")
        input("When the site looks normal in the window, press Enter here to start downloads… ")
        await page.close()

        # Results CSV
        with open(args.results_csv, "w", newline="", encoding="utf-8") as rf:
            w = csv.writer(rf)
            w.writerow(["url","status","filename","sha1","content_type","bytes","error"])
            for i, u in enumerate(urls, 1):
                try:
                    success, status, data, ct = await fetch_with_context(browser, u, args.referer, args.timeout_ms)
                    if success:
                        digest = sha1_bytes(data)
                        ext = guess_ext(u, ct)
                        raw_name = safe_name_from_url(u)
                        if not raw_name.endswith(ext): raw_name = raw_name + ext

                        # write raw
                        raw_path = out_dir / raw_name
                        if not raw_path.exists():
                            raw_path.write_bytes(data)

                        # write dedup
                        dedup_name = f"{digest}{ext}"
                        dedup_path = dedup_dir / dedup_name
                        if not dedup_path.exists():
                            dedup_path.write_bytes(data)

                        w.writerow([u, status, dedup_name, digest, ct, len(data), ""])
                        ok += 1
                        print(f"[{i}/{len(urls)}] OK  {status}  -> {dedup_name}")
                    else:
                        w.writerow([u, status, "", "", ct, 0, "blocked or error"])
                        fail += 1
                        print(f"[{i}/{len(urls)}] FAIL {status}  {u}")
                except Exception as e:
                    w.writerow([u, 0, "", "", "", 0, str(e)])
                    fail += 1
                    print(f"[{i}/{len(urls)}] FAIL 0  {u}  err={e}")

        await browser.close()

    dur = time.time() - started
    print(f"Done. Success={ok}  Fail={fail}  Time={dur:.1f}s")
    sys.exit(0 if fail == 0 else 2)

if __name__ == "__main__":
    asyncio.run(main())
