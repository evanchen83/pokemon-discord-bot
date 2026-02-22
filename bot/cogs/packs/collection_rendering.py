from __future__ import annotations

from typing import Any, Callable


def build_set_blocks(rows: list[Any], rarity_rank: Callable[[str], int]) -> list[str]:
    set_order: list[str] = []
    rows_by_set: dict[str, list[Any]] = {}
    for row in rows:
        if row.set_name not in rows_by_set:
            set_order.append(row.set_name)
            rows_by_set[row.set_name] = []
        rows_by_set[row.set_name].append(row)

    set_blocks: list[str] = []
    for set_name in set_order:
        set_rows = rows_by_set[set_name]
        set_rows.sort(
            key=lambda r: (
                -rarity_rank(r.rarity),
                -(r.copies or 0),
                (r.card_name or "").lower(),
                (r.card_number or ""),
            )
        )
        top_rank = rarity_rank(set_rows[0].rarity) if set_rows else 0
        lines_for_set: list[str] = []
        for row in set_rows:
            num = f" #{row.card_number}" if row.card_number else ""
            dup = f" ({row.copies}x)" if row.copies > 1 else ""
            line = f"• {row.card_name}{num} [{row.rarity}]{dup}"
            if rarity_rank(row.rarity) == top_rank:
                line = f"**{line}**"
            lines_for_set.append(line)
        set_blocks.append(f"**{set_name}:**\n" + "\n".join(lines_for_set))

    return set_blocks
