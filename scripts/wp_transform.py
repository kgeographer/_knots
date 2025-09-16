#!/usr/bin/env python3
# wp_transform.py Transform a WordPress WXR (XML) export for Substack import.
"""
usage:
python scripts/wp_transform.py dump/wordpress_posts/knotty.wordpress.2025-09-14.000.xml \
    work/category_mappings.tsv out/substack_import_sample.xml \
  --filter both --limit 25
"""
from __future__ import annotations
import argparse, csv, html, re
from datetime import datetime
# import xml.etree.ElementTree as ET
from lxml import etree as ET

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

def slugify(label: str) -> str:
    s = label.strip().lower()
    s = re.sub(r"[^\w\s-]", "", s)
    s = re.sub(r"\s+", "-", s).strip("-")
    s = re.sub(r"-{2,}", "-", s)
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

def extract_final_tags(item: ET.Element, mapping: dict[str, str]):
    pairs = []
    for cat in item.findall("category"):
        if cat.get("domain") != "category": continue
        label = (cat.text or "").strip()
        if not label: continue
        mapped = mapping.get(label, None)
        if mapped is None:
            out_label = label
        elif mapped == "":
            continue  # drop
        else:
            out_label = mapped
        pairs.append((out_label, slugify(out_label)))
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
        block.append('<div style="margin-top:0.75em;">')
        block.append(f'<div style="font-weight:600; color:#333;">{author}</div>')
        block.append(f'<div style="font-size:0.85em; color:#777;">{date_s}</div>')
        block.append(f'<div style="color:#444;">{body}</div>')
        block.append('</div>')
    block.append('</div>')
    return content_html + ("\n" if not content_html.endswith("\n") else "") + "".join(block)

def transform_item(item: ET.Element, mapping: dict[str, str]) -> None:
    # final tags
    final_pairs = extract_final_tags(item, mapping)
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
    args = ap.parse_args()

    mapping = load_mapping(args.mapping_tsv)
    tree = ET.parse(args.wxr_in)
    root = tree.getroot()
    channel = root.find("channel")
    if channel is None:
        raise RuntimeError("No <channel> found.")

    orig_items = channel.findall("item")
    # Remove all items; we’ll append selected/transformed ones
    for it in list(orig_items): channel.remove(it)

    # Build selection list respecting --filter
    selected = []
    for it in orig_items:
        # inspect without mutation
        tags = extract_final_tags(it, mapping)
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

    # Apply limit after filtering
    if args.limit and args.limit > 0:
        selected = selected[:args.limit]

    # Transform and append
    for it in selected:
        it_copy = ET.fromstring(ET.tostring(it, encoding="utf-8"))
        transform_item(it_copy, mapping)
        channel.append(it_copy)


    tree.write(args.wxr_out, encoding="utf-8", xml_declaration=True)
    # at the end of main()
    tree.write(args.wxr_out, encoding="utf-8", xml_declaration=True)

if __name__ == "__main__":
    main()
