#!/usr/bin/env python3
# wp_transform.py Transform a WordPress WXR (XML) export for Substack import.
"""
usage:
# Example: randomly pick 25 from the 83 posts that have both tags and comments
python scripts/wp_transform.py dump/wordpress_posts/knotty.wordpress.2025-09-14.000.xml \
  work/category_mappings.tsv out/substack_import_sample_5.xml \
  --filter both --limit 5 --sample-random --sample-seed 11 \
  --slug-override work/substack_tag_slugs.tsv

run with seed to repeatable random sampling
run without seed to get a different random sample each time

"""
from __future__ import annotations
import argparse, csv, html, re, random
from datetime import datetime, timedelta, timezone
# import xml.etree.ElementTree as ET
from lxml import etree as ET
import uuid

NS = {
    "content": "http://purl.org/rss/1.0/modules/content/",
    "wp": "http://wordpress.org/export/1.2/",
    "dc": "http://purl.org/dc/elements/1.1/",
    "excerpt": "http://wordpress.org/export/1.2/excerpt/",
    "atom": "http://www.w3.org/2005/Atom",
    "wfw": "http://wellformedweb.org/CommentAPI/",
    "slash": "http://purl.org/rss/1.0/modules/slash/",
}
for k, v in NS.items():
    ET.register_namespace(k, v)

TAGS_FOOTER_CLASS = "import-tags-footer"
COMMENTS_WRAPPER_CLASS = "legacy-comments"

def set_cdata(el, text: str):
    # split any accidental ']]>' so CDATA remains well-formed
    safe = text.replace("]]>", "]]]]><![CDATA[>")
    el.text = ET.CDATA(safe)

def build_wxr_root(channel_title="Seed Import", channel_link="https://example.com", channel_desc="Tag seed"):
    # Register namespaces explicitly; lxml will serialize xmlns:* on the root as needed.
    for prefix, uri in NS.items():
        ET.register_namespace(prefix, uri)

    rss = ET.Element("rss", {"version": "2.0"})
    channel = ET.SubElement(rss, "channel")

    # Basic channel metadata
    ET.SubElement(channel, "title").text = channel_title
    ET.SubElement(channel, "link").text = channel_link
    ET.SubElement(channel, "description").text = channel_desc
    ET.SubElement(channel, "generator").text = "WordPress/5.9.3; wp_transform seed-emitter"

    # Minimal WXR marker so Substack treats this like a WP export
    wxr = ET.SubElement(channel, f"{{{NS['wp']}}}wxr_version")
    wxr.text = "1.2"

    return rss, channel

def emit_tag_seed(mapping_tsv: str, out_path: str):
    """
    Build a minimal WXR that contains one post per mapped label.
    This forces Substack to create its own authoritative tag slugs for each label.
    TSV columns expected: category, count, mapped label
    """
    # Collect unique, non-empty mapped labels
    labels = []
    with open(mapping_tsv, "r", encoding="utf-8") as f:
        rdr = csv.DictReader(f, delimiter="\t")
        for row in rdr:
            dst = (row.get("mapped label") or "").strip()
            if dst and dst not in labels:
                labels.append(dst)

    rss, channel = build_wxr_root(channel_title="Substack Tag Seed",
                                  channel_desc="One post per tag label")

    # Minimal author table so dc:creator resolves to a valid user
    wp_author = ET.SubElement(channel, f"{{{NS['wp']}}}author")
    ET.SubElement(wp_author, f"{{{NS['wp']}}}author_id").text = "1"
    ET.SubElement(wp_author, f"{{{NS['wp']}}}author_login").text = "seed"
    ET.SubElement(wp_author, f"{{{NS['wp']}}}author_display_name").text = "Seed"

    t0 = datetime.now(timezone.utc)

    for i, label in enumerate(labels, start=1):
        link_slug = slugify(label)
        unique_link = f"https://example.com/seed/{link_slug}-{i}"

        item = ET.SubElement(channel, "item")
        ET.SubElement(item, "title").text = f"(seed) {label}"
        ET.SubElement(item, "link").text = unique_link

        guid = ET.SubElement(item, "guid", {"isPermaLink": "false"})
        guid.text = f"seed:{uuid.uuid4()}"

        # Standard RSS pubDate (UTC)
        ET.SubElement(item, "pubDate").text = (t0 + timedelta(seconds=i)).strftime("%a, %d %b %Y %H:%M:%S +0000")

        # WordPress-ish fields so importers treat this as a real post
        ET.SubElement(item, f"{{{NS['dc']}}}creator").text = "seed"
        ET.SubElement(item, f"{{{NS['wp']}}}post_id").text = str(i)
        ET.SubElement(item, f"{{{NS['wp']}}}post_date").text = (t0 + timedelta(seconds=i)).strftime("%Y-%m-%d %H:%M:%S")
        ET.SubElement(item, f"{{{NS['wp']}}}post_date_gmt").text = (t0 + timedelta(seconds=i)).strftime("%Y-%m-%d %H:%M:%S")
        ET.SubElement(item, f"{{{NS['wp']}}}post_name").text = f"seed-{link_slug}-{i}"
        ET.SubElement(item, f"{{{NS['wp']}}}status").text = "publish"
        ET.SubElement(item, f"{{{NS['wp']}}}post_type").text = "post"
        ET.SubElement(item, f"{{{NS['wp']}}}comment_status").text = "closed"
        ET.SubElement(item, f"{{{NS['wp']}}}ping_status").text = "closed"

        # Minimal HTML content in CDATA
        content_el = ET.SubElement(item, f"{{{NS['content']}}}encoded")
        set_cdata(content_el, f"<p>Seeding tag: {html.escape(label)}</p>")

        # The tag itself. Substack will ignore nicename and derive its own slug from this label.
        tag_el = ET.SubElement(item, "category", {"domain": "post_tag", "nicename": link_slug})
        tag_el.text = label

    tree = ET.ElementTree(rss)
    tree.write(out_path, encoding="utf-8", xml_declaration=True, pretty_print=True)
    print(f"[seed] Emitted {len(labels)} seed posts to {out_path}")

import re, unicodedata

def slugify(label: str) -> str:
    s = unicodedata.normalize("NFKD", label).encode("ascii", "ignore").decode("ascii")
    s = s.lower().strip()
    # 1) normalize connectors to words/separators
    s = s.replace("&", " and ")           # match Substack-like behavior
    s = re.sub(r"[\/]+", " ", s)          # slashes act as word breaks
    # 2) drop other punctuation except spaces/hyphens
    s = re.sub(r"[^\w\s-]", "", s)
    # 3) collapse whitespace to single hyphens
    s = re.sub(r"\s+", "-", s)
    # 4) collapse repeated hyphens and trim
    s = re.sub(r"-{2,}", "-", s).strip("-")
    return s

def load_mapping(tsv_path: str) -> dict[str, str]:
    """
    TSV columns: category, count, mapped label
    Returns mapping from original category -> mapped label.
    """
    mapping = {}
    with open(tsv_path, "r", encoding="utf-8") as f:
        rdr = csv.DictReader(f, delimiter="\t")
        for row in rdr:
            src = row["category"].strip()
            dst = row["mapped label"].strip()
            if not src:
                continue
            # if mapped label empty, drop it
            mapping[src] = dst
    return mapping

def load_slug_overrides(tsv_path: str | None) -> dict[str, str]:
    """
    Optional TSV columns: label, substack_slug
    Returns mapping from human label -> authoritative Substack slug.
    """
    if not tsv_path:
        return {}
    overrides = {}
    with open(tsv_path, "r", encoding="utf-8") as f:
        rdr = csv.DictReader(f, delimiter="\t")
        # accept headers like: label, substack_slug
        for row in rdr:
            label = (row.get("label") or "").strip()
            slug  = (row.get("substack_slug") or "").strip()
            if label and slug:
                overrides[label] = slug
    return overrides

def dedupe_preserve_order(pairs):
    seen = set(); out = []
    for lab, sl in pairs:
        key = lab.casefold()
        if key in seen: continue
        seen.add(key); out.append((lab, sl))
    return out

def build_tags_footer(tag_pairs):
    if not tag_pairs: return ""
    parts = [f'<p class="{TAGS_FOOTER_CLASS}"><strong>Tags:</strong><span> </span>']
    for i, (label, slug) in enumerate(tag_pairs):
        if i: parts.append('<span> · </span>')
        parts.append(f'<a href="/t/{slug}" rel="nofollow ugc noopener">{html.escape(label)}</a>')
    parts.append("</p>")
    return "".join(parts)

def format_wp_datetime(s: str) -> str:
    try:
        return datetime.strptime(s, "%Y-%m-%d %H:%M:%S").strftime("%B %d, %Y at %I:%M %p")
    except Exception:
        return s

def normalize_comment_body(raw: str) -> str:
    if raw is None: return ""
    if "<" in raw and ">" in raw: return raw
    return html.escape(raw).replace("\n", "<br/>")

def remove_existing_blocks(html_str: str) -> str:
    html_str = re.sub(
        rf'<p[^>]*class="[^"]*\b{re.escape(TAGS_FOOTER_CLASS)}\b[^"]*"[^>]*>.*?</p>',
        "", html_str, flags=re.DOTALL | re.IGNORECASE)
    html_str = re.sub(
        rf'<div[^>]*class="[^"]*\b{re.escape(COMMENTS_WRAPPER_CLASS)}\b[^"]*"[^>]*>.*?</div>\s*',
        "", html_str, flags=re.DOTALL | re.IGNORECASE)
    return html_str

def extract_final_tags(item: ET.Element, mapping: dict[str, str], slug_overrides: dict[str, str]):
    pairs = []
    for cat in item.findall("category"):
        if cat.get("domain") != "category":
            continue
        label = (cat.text or "").strip()
        if not label:
            continue
        mapped = mapping.get(label, None)
        if mapped is None:
            out_label = label
        elif mapped == "":
            continue  # drop
        else:
            out_label = mapped
        out_slug = slug_overrides.get(out_label) or slugify(out_label)
        pairs.append((out_label, out_slug))
    return dedupe_preserve_order(pairs)

def extract_eligible_comments(item: ET.Element):
    cs = []
    for c in item.findall("wp:comment", namespaces=NS):
        if (c.findtext("wp:comment_approved", default="", namespaces=NS) or "").strip() != "1":
            continue
        if (c.findtext("wp:comment_type", default="", namespaces=NS) or "").strip():
            continue  # pingbacks/trackbacks
        body = c.findtext("wp:comment_content", default="", namespaces=NS)
        if not (body and body.strip()): continue
        cs.append({
            "author": c.findtext("wp:comment_author", default="", namespaces=NS) or "Anonymous",
            "date": c.findtext("wp:comment_date", default="", namespaces=NS) or "",
            "body": body,
        })
    def _k(c):
        try: return datetime.strptime(c["date"], "%Y-%m-%d %H:%M:%S")
        except Exception: return datetime.min
    cs.sort(key=_k)
    return cs

def append_comments_block(content_html: str, comments):
    if not comments: return content_html
    block = [
        '<hr style="margin: 2em 0; border: none; border-top: 1px solid #ddd;"/>',
        f'<div class="{COMMENTS_WRAPPER_CLASS}" style="background-color:#f8f9fa; padding:1.5em; border-radius:6px;">',
        '<h4 style="color:#666;">Comments from Original Post</h4>',
    ]
    for c in comments:
        author = html.escape(c["author"]) if c["author"] else "Anonymous"
        date_s = html.escape(format_wp_datetime(c["date"]))
        body = normalize_comment_body(c["body"])
        block.append('<div style="margin-top:1.25em;">')
        block.append(f'<div style="font-weight:600; color:#333;">{author} <span style="font-size:0.85em; color:#777;">— {date_s}</span></div>')
        block.append(f'<div style="color:#444; margin-top:0.25em;">{body}</div>')
        block.append('</div>')
        block.append('<hr style="margin:1.5em 0; border:none; border-top:1px solid #ccc;"/>')
    # Remove the last trailing <hr/> if present
    if block and block[-1].startswith('<hr'):
        block.pop()
    block.append('</div>')
    return content_html + ("\n" if not content_html.endswith("\n") else "") + "".join(block)

def transform_item(item: ET.Element, mapping: dict[str, str], slug_overrides: dict[str, str]) -> None:
    # final tags
    final_pairs = extract_final_tags(item, mapping, slug_overrides)
    # rebuild tag elements
    for cat in list(item.findall("category")): item.remove(cat)
    for label, slug in final_pairs:
        el = ET.Element("category", {"domain": "post_tag", "nicename": slug})
        el.text = label
        item.append(el)

    # content + footers
    content_el = item.find("content:encoded", namespaces=NS)
    content_html = (content_el.text or "") if content_el is not None else ""
    content_html = remove_existing_blocks(content_html)
    if final_pairs:
        content_html += ("" if content_html.endswith("\n") else "\n") + build_tags_footer(final_pairs)

    comments = extract_eligible_comments(item)
    if comments:
        content_html = append_comments_block(content_html, comments)

    if content_el is None:
        content_el = ET.SubElement(item, f"{{{NS['content']}}}encoded")
    # content_el.text = content_html
    set_cdata(content_el, content_html)

    # Force dc:creator to "seed"
    dc_el = item.find(f"dc:creator", namespaces=NS)
    if dc_el is None:
        dc_el = ET.SubElement(item, f"{{{NS['dc']}}}creator")
    dc_el.text = "seed"

def main():
    ap = argparse.ArgumentParser(description="Transform WordPress WXR for Substack import.")
    ap.add_argument("wxr_in")
    ap.add_argument("mapping_tsv")
    ap.add_argument("wxr_out")
    ap.add_argument("--limit", type=int, default=0, help="Max number of items to emit after filtering")
    ap.add_argument(
        "--filter",
        choices=["none", "both", "tags", "comments", "any"],
        default="none",
        help=("Filter items before limiting: "
              "'both' = require tags AND comments; "
              "'tags' = require tags; 'comments' = require comments; "
              "'any' = tags OR comments; 'none' = no filtering.")
    )
    ap.add_argument(
        "--emit-tag-seed",
        metavar="SEED_WXR_OUT",
        help="Write a minimal WXR with one post per mapped label (to seed Substack tag slugs) and exit."
    )
    ap.add_argument(
        "--slug-override",
        metavar="TSV",
        help="Optional TSV with columns: label, substack_slug. Overrides the slug used for tags and footer links."
    )
    ap.add_argument(
        "--sample-random",
        action="store_true",
        help="If set with --limit, pick a random subset of size LIMIT from the filtered items."
    )
    ap.add_argument(
        "--sample-seed",
        type=int,
        help="Optional seed for --sample-random to make selection reproducible."
    )
    args = ap.parse_args()

    if args.emit_tag_seed:
        emit_tag_seed(args.mapping_tsv, args.emit_tag_seed)
        return

    mapping = load_mapping(args.mapping_tsv)
    slug_overrides = load_slug_overrides(args.slug_override)
    tree = ET.parse(args.wxr_in)
    root = tree.getroot()
    channel = root.find("channel")
    if channel is None:
        raise RuntimeError("No <channel> found.")

    # Replace channel <wp:author> with a seed author block
    for a in list(channel.findall("wp:author", namespaces=NS)):
        channel.remove(a)
    seed_author = ET.SubElement(channel, f"{{{NS['wp']}}}author")
    ET.SubElement(seed_author, f"{{{NS['wp']}}}author_id").text = "1"
    ET.SubElement(seed_author, f"{{{NS['wp']}}}author_login").text = "seed"
    ET.SubElement(seed_author, f"{{{NS['wp']}}}author_email").text = "seed@example.com"
    ET.SubElement(seed_author, f"{{{NS['wp']}}}author_display_name").text = "Seed"

    orig_items = channel.findall("item")
    # Remove all items; we’ll append selected/transformed ones
    for it in list(orig_items): channel.remove(it)

    # Build selection list respecting --filter
    selected = []
    for it in orig_items:
        # inspect without mutation
        tags = extract_final_tags(it, mapping, slug_overrides)
        comments = extract_eligible_comments(it)
        keep = True
        if args.filter == "both":
            keep = bool(tags) and bool(comments)
        elif args.filter == "tags":
            keep = bool(tags)
        elif args.filter == "comments":
            keep = bool(comments)
        elif args.filter == "any":
            keep = bool(tags) or bool(comments)
        # "none" leaves keep=True
        if keep:
            selected.append(it)

    total_items = len(orig_items)
    selected_before_limit = len(selected)

    # Apply limit after filtering (optionally as a random sample)
    if args.limit and args.limit > 0:
        if getattr(args, "sample_random", False) and len(selected) > args.limit:
            if args.sample_seed is not None:
                random.seed(args.sample_seed)
            selected = random.sample(selected, args.limit)
        else:
            selected = selected[:args.limit]

    emitted_count = len(selected)

    # Transform and append
    for it in selected:
        it_copy = ET.fromstring(ET.tostring(it, encoding="utf-8"))
        transform_item(it_copy, mapping, slug_overrides)
        channel.append(it_copy)

    tree.write(args.wxr_out, encoding="utf-8", xml_declaration=True, pretty_print=True)
    # Summary output
    filt = getattr(args, "filter", "none")
    lim = args.limit if args.limit else "none"
    samp = "random" if getattr(args, "sample_random", False) and (args.limit and args.limit > 0) else "head"
    seed = f", seed={args.sample_seed}" if getattr(args, "sample_random", False) and args.sample_seed is not None else ""
    print(f"[transform] Source items: {total_items} | Selected (filter={filt}): {selected_before_limit} | Emitted (limit={lim}, sample={samp}{seed}): {emitted_count} | Wrote: {args.wxr_out}")

if __name__ == "__main__":
    main()
