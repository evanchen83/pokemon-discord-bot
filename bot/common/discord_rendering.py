from __future__ import annotations

import re
from urllib.parse import urlparse
from typing import Optional

import discord

from features.embed_standards import EMBED_PAGE_CHAR_LIMIT


def split_discord_message(text: str, limit: int = EMBED_PAGE_CHAR_LIMIT) -> list[str]:
    text = text.strip()
    if not text:
        return []
    if len(text) <= limit:
        return [text]

    paragraphs = [p for p in text.split("\n\n") if p.strip()]
    chunks: list[str] = []
    current = ""

    def hard_split(block: str) -> list[str]:
        out: list[str] = []
        start = 0
        while start < len(block):
            end = min(start + limit, len(block))
            if end < len(block):
                split_idx = block.rfind("\n", start, end)
                if split_idx <= start:
                    split_idx = block.rfind(" ", start, end)
                if split_idx > start:
                    end = split_idx
            part = block[start:end].strip()
            if part:
                out.append(part)
            start = end
        return out

    for para in paragraphs:
        candidate = para if not current else f"{current}\n\n{para}"
        if len(candidate) <= limit:
            current = candidate
            continue

        if current:
            chunks.append(current)
            current = ""

        if len(para) <= limit:
            current = para
        else:
            chunks.extend(hard_split(para))

    if current:
        chunks.append(current)
    return chunks


def paginate_set_blocks(blocks: list[str], limit: int = EMBED_PAGE_CHAR_LIMIT) -> list[str]:
    if not blocks:
        return []

    pages: list[str] = []
    current = ""
    for block in blocks:
        block = (block or "").strip()
        if not block:
            continue
        candidate = block if not current else f"{current}\n\n{block}"
        if len(candidate) <= limit:
            current = candidate
            continue
        if current:
            pages.append(current)
            current = ""
        if len(block) <= limit:
            current = block
            continue
        pages.extend(split_discord_message(block, limit=limit))
    if current:
        pages.append(current)
    return pages or ["(No collection data)"]


def rarity_rank(rarity: str) -> int:
    r = (rarity or "").strip().lower()
    if not r:
        return 0
    if "special illustration rare" in r:
        return 100
    if "hyper rare" in r:
        return 97
    if "secret rare" in r:
        return 95
    if "ultra rare" in r:
        return 93
    if "illustration rare" in r:
        return 91
    if "double rare" in r:
        return 90
    if "rare holo vstar" in r:
        return 89
    if "rare holo vmax" in r:
        return 88
    if "rare holo v" in r or "rare holo ex" in r:
        return 87
    if "rare holo" in r:
        return 82
    if "rare" in r:
        return 75
    if "uncommon" in r:
        return 45
    if "common" in r:
        return 25
    return 10


_TABLE_SEPARATOR_RE = re.compile(r"^\s*\|?\s*:?-{2,}:?\s*(\|\s*:?-{2,}:?\s*)+\|?\s*$")


def _is_markdown_table_header(line: str, next_line: str) -> bool:
    if "|" not in line:
        return False
    if _TABLE_SEPARATOR_RE.match(next_line):
        return True
    if "|" in next_line:
        tokens = [t.strip() for t in next_line.strip().strip("|").split("|")]
        if tokens and all(t and set(t) <= {"-", ":"} and t.count("-") >= 2 for t in tokens):
            return True
    return False


def _parse_markdown_table_row(line: str) -> list[str]:
    raw = line.strip()
    if raw.startswith("|"):
        raw = raw[1:]
    if raw.endswith("|"):
        raw = raw[:-1]
    return [cell.strip() for cell in raw.split("|")]


def _normalize_table_cell(value: str) -> str:
    v = (value or "").strip()
    v = v.replace("<br>", "\n").replace("<br/>", "\n").replace("<br />", "\n")
    v = re.sub(r"!\[[^\]]*\]\((https?://[^)]+)\)", r"\1", v)
    return v


def _extract_image_url(value: str) -> str:
    m = re.search(r"(https?://\S+)", value or "")
    if not m:
        return ""
    url = m.group(1).strip().rstrip(").,")
    return url


def _render_markdown_table_as_mobile_list(headers: list[str], rows: list[list[str]]) -> str:
    lines: list[str] = []
    image_col_idx = -1
    for i, h in enumerate(headers):
        hl = (h or "").strip().lower()
        if "image" in hl or "url" in hl:
            image_col_idx = i
            break

    for row in rows:
        values = [_normalize_table_cell(v) for v in row]
        left = values[0] if len(values) > 0 else ""
        right = values[1] if len(values) > 1 else ""
        if image_col_idx >= 0 and image_col_idx < len(values):
            image_url = _extract_image_url(values[image_col_idx])
            fields = []
            for idx, header in enumerate(headers):
                if idx == image_col_idx:
                    continue
                cell = values[idx] if idx < len(values) else ""
                if cell:
                    fields.append((header or f"col_{idx+1}", cell))
            if not fields and not image_url:
                continue
            head = fields[0][1] if fields else "Image"
            detail_lines = []
            for hdr, val in fields[1:]:
                detail_lines.append(f"**{hdr}:** {val}")
            block = f"• **{head}**"
            if detail_lines:
                block += "\n" + "\n".join(detail_lines)
            if image_url:
                block += f"\n[[IMG:{image_url}]]"
            lines.append(block)
            continue

        if not left and not right:
            continue
        if left and right:
            lines.append(f"• **{left}**\n{right}")
        elif left:
            lines.append(f"• **{left}**")
        else:
            lines.append(f"• {right}")
    return "\n\n".join(lines)


def _fit_cell(value: str, width: int) -> str:
    clean = re.sub(r"\s+", " ", (value or "").strip())
    if len(clean) <= width:
        return clean.ljust(width)
    if width <= 1:
        return clean[:width]
    return clean[: width - 1] + "…"


def _render_markdown_table_as_pretty_codeblock(
    headers: list[str],
    rows: list[list[str]],
    rows_per_block: int = 20,
    max_col_width: int = 28,
) -> str:
    if not headers:
        return ""

    col_count = len(headers)
    all_rows: list[list[str]] = []
    for row in rows:
        padded = [(row[i] if i < len(row) else "") for i in range(col_count)]
        all_rows.append(padded)

    widths: list[int] = []
    for i, header in enumerate(headers):
        w = min(max_col_width, max(4, len((header or "").strip())))
        for row in all_rows:
            w = min(max_col_width, max(w, len((row[i] or "").strip())))
        widths.append(w)

    def fmt_row(cells: list[str]) -> str:
        return "| " + " | ".join(_fit_cell(cells[i], widths[i]) for i in range(col_count)) + " |"

    sep = "|-" + "-|-".join("-" * widths[i] for i in range(col_count)) + "-|"

    target_chars = max(420, EMBED_PAGE_CHAR_LIMIT - 80)
    header_block = f"{fmt_row(headers)}\n{sep}"
    row_groups: list[tuple[int, int, list[str]]] = []

    start_idx = 0
    current_rows: list[str] = []
    current_chars = len(header_block)
    for idx, row in enumerate(all_rows):
        rendered = fmt_row(row)
        projected = current_chars + 1 + len(rendered)
        row_count = len(current_rows)
        if row_count > 0 and (projected > target_chars or row_count >= rows_per_block):
            row_groups.append((start_idx, idx, current_rows))
            start_idx = idx
            current_rows = []
            current_chars = len(header_block)
        current_rows.append(rendered)
        current_chars += 1 + len(rendered)

    if current_rows:
        row_groups.append((start_idx, len(all_rows), current_rows))

    chunks: list[str] = []
    total_rows = len(all_rows)
    total_blocks = max(1, len(row_groups))
    for block_idx, (start, end, rendered_rows) in enumerate(row_groups, start=1):
        lines = [fmt_row(headers), sep] + rendered_rows
        prefix = f"Table page {block_idx}/{total_blocks} (rows {start + 1}-{end} of {total_rows})"
        chunks.append(prefix + "\n```text\n" + "\n".join(lines) + "\n```")

    return "\n\n".join(chunks)


def _table_should_use_mobile_list(headers: list[str], rows: list[list[str]]) -> bool:
    if any("image" in (h or "").strip().lower() or "url" in (h or "").strip().lower() for h in headers):
        return True
    if len(headers) != 2:
        return False
    header_left = (headers[0] or "").strip().lower()
    header_right = (headers[1] or "").strip().lower()
    if header_left in {"item", "field", "attribute", "topic"} and header_right in {"details", "value", "info", "description"}:
        return True
    long_second_col = 0
    sample = rows[: min(len(rows), 20)]
    for r in sample:
        second = (r[1] if len(r) > 1 else "") or ""
        if len(second.strip()) >= 45:
            long_second_col += 1
    return bool(sample) and (long_second_col / len(sample) >= 0.4)


def format_agent_response_for_discord(text: str) -> str:
    lines = text.strip().splitlines()
    if not lines:
        return text

    out: list[str] = []
    i = 0
    while i < len(lines):
        if i + 1 < len(lines) and _is_markdown_table_header(lines[i], lines[i + 1]):
            headers = _parse_markdown_table_row(lines[i])
            i += 2
            rows: list[list[str]] = []
            while i < len(lines) and "|" in lines[i]:
                rows.append(_parse_markdown_table_row(lines[i]))
                i += 1

            if _table_should_use_mobile_list(headers, rows):
                rendered = _render_markdown_table_as_mobile_list(headers, rows)
            else:
                rendered = _render_markdown_table_as_pretty_codeblock(headers, rows)
            if rendered:
                out.append(rendered)
            continue

        out.append(lines[i])
        i += 1

    compact = "\n".join(out).strip()
    compact = _compact_bullet_label_value_lines(compact)
    return _compress_long_list_blocks(compact)


def _compact_bullet_label_value_lines(text: str) -> str:
    lines = text.splitlines()
    if not lines:
        return text

    out: list[str] = []
    i = 0
    while i < len(lines):
        current = lines[i].rstrip()
        if i + 1 < len(lines):
            nxt = lines[i + 1].strip()
            cur_strip = current.strip()
            cur_body = cur_strip[1:].strip() if cur_strip.startswith("•") else cur_strip
            cur_body_lc = cur_body.lower()
            label_like_no_colon = (
                cur_strip.startswith("•")
                and not cur_strip.endswith(":")
                and len(cur_body) <= 48
                and not any(ch in cur_body for ch in ".!?|")
                and not cur_body_lc.startswith(("why ", "note ", "summary", "overview"))
            )
            if (
                cur_strip.startswith("•")
                and cur_strip.endswith(":")
                and nxt
                and not nxt.startswith("•")
                and not nxt.startswith("```")
                and "|" not in nxt
            ):
                out.append(f"{current} {nxt}")
                i += 2
                continue
            if (
                label_like_no_colon
                and nxt
                and not nxt.startswith("•")
                and not nxt.startswith("```")
                and "|" not in nxt
            ):
                out.append(f"{current}: {nxt}")
                i += 2
                continue
        out.append(current)
        i += 1
    return "\n".join(out).strip()


def _compress_long_list_blocks(text: str, max_bullets: int = 120) -> str:
    lines = text.splitlines()
    if not lines:
        return text

    kept: list[str] = []
    bullet_run: list[str] = []

    def flush_bullets() -> None:
        nonlocal bullet_run
        if not bullet_run:
            return
        if len(bullet_run) > max_bullets:
            kept.extend(bullet_run[:max_bullets])
            kept.append(f"... ({len(bullet_run) - max_bullets} more rows hidden for readability)")
        else:
            kept.extend(bullet_run)
        bullet_run = []

    for line in lines:
        stripped = line.strip()
        if stripped.startswith("• "):
            bullet_run.append(line)
        else:
            flush_bullets()
            kept.append(line)
    flush_bullets()
    return "\n".join(kept).strip()


def build_response_embeds(
    title: str,
    text: str,
    color: discord.Color,
    llm_model: Optional[str],
    question: Optional[str] = None,
) -> list[discord.Embed]:
    image_blocks, text = _extract_image_blocks(text)
    max_image_embeds = 5
    hidden_image_count = max(0, len(image_blocks) - max_image_embeds)
    if hidden_image_count:
        image_blocks = image_blocks[:max_image_embeds]
    chunks = split_discord_message(text, limit=EMBED_PAGE_CHAR_LIMIT) or ["(No response text returned)"]
    _ = llm_model
    footer = "Powered by IBM watsonx Orchestrate"

    embeds: list[discord.Embed] = []
    total = len(chunks)
    q_prefix = ""
    if question:
        q = re.sub(r"\s+", " ", question.strip())
        if len(q) > 90:
            q = q[:87] + "..."
        q_prefix = f"Q: {q}"
    for idx, chunk in enumerate(chunks, start=1):
        base_title = q_prefix or title
        embed_title = base_title if total == 1 else f"{base_title} ({idx}/{total})"
        if len(embed_title) > 250:
            embed_title = embed_title[:247] + "..."
        if idx == 1 and hidden_image_count:
            chunk = f"{chunk}\n\n(Showing first {max_image_embeds} images; {hidden_image_count} more omitted.)"
        embed = discord.Embed(title=embed_title, description=chunk, color=color)
        embed.set_footer(text=footer)
        embeds.append(embed)

    if image_blocks:
        start_idx = 0
        if embeds:
            first_desc, first_url = image_blocks[0]
            if first_desc and not embeds[0].description:
                embeds[0].description = first_desc
            embeds[0].set_image(url=first_url)
            start_idx = 1

        total_images = len(image_blocks)
        for idx, (desc, image_url) in enumerate(image_blocks[start_idx:], start=start_idx + 1):
            base_title = q_prefix or title
            image_title = f"{base_title} • Image {idx}/{total_images}"
            if len(image_title) > 250:
                image_title = image_title[:247] + "..."
            embed = discord.Embed(
                title=image_title,
                description=desc or f"Image {idx}",
                color=color,
            )
            embed.set_image(url=image_url)
            embed.set_footer(text=footer)
            embeds.append(embed)
    return embeds


def _is_image_url(url: str) -> bool:
    try:
        parsed = urlparse(url)
    except Exception:
        return False
    host = (parsed.hostname or "").lower()
    path = (parsed.path or "").lower()
    if "images.pokemontcg.io" in host:
        return True
    if "images.scrydex.com" in host:
        return True
    if path.endswith("/large") or path.endswith("/small"):
        return True
    return path.endswith((".png", ".jpg", ".jpeg", ".webp", ".gif"))


def _extract_image_blocks(text: str) -> tuple[list[tuple[str, str]], str]:
    raw = (text or "").strip()
    if not raw:
        return [], ""

    blocks: list[tuple[str, str]] = []
    seen_urls: set[str] = set()
    kept_lines: list[str] = []
    lines = raw.splitlines()

    for idx, line in enumerate(lines):
        line_work = line

        marker_urls = re.findall(r"\[\[IMG:(https?://[^\]\s]+)\]\]", line_work)
        if marker_urls:
            for url in marker_urls:
                clean_url = url.strip()
                if clean_url and clean_url not in seen_urls and _is_image_url(clean_url):
                    desc = re.sub(r"\[\[IMG:https?://[^\]\s]+\]\]", "", line_work).strip()
                    if not desc and idx > 0:
                        prev = lines[idx - 1].strip()
                        if prev:
                            desc = prev
                    blocks.append((desc, clean_url))
                    seen_urls.add(clean_url)
            line_work = re.sub(r"\[\[IMG:https?://[^\]\s]+\]\]", "", line_work).strip()

        urls = re.findall(r"https?://\S+", line_work)
        image_urls: list[str] = []
        for url in urls:
            clean_url = url.strip().rstrip(").,;")
            if clean_url and _is_image_url(clean_url):
                image_urls.append(clean_url)
        if image_urls:
            desc_line = line_work
            for u in image_urls:
                desc_line = desc_line.replace(u, "")
            desc_line = re.sub(r"\(\s*\)", "", desc_line).strip(" :-")
            if not desc_line and idx > 0:
                prev = lines[idx - 1].strip()
                if prev and not re.search(r"https?://\S+", prev):
                    desc_line = prev
            for u in image_urls:
                if u in seen_urls:
                    continue
                blocks.append((desc_line, u))
                seen_urls.add(u)
            line_work = ""

        if re.search(r"\b(click|open)\b.*\b(url|link)\b", line_work, flags=re.IGNORECASE):
            continue
        if line_work:
            kept_lines.append(line_work)

    cleaned = "\n".join(kept_lines).strip()
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
    return blocks, cleaned
