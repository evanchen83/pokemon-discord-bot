from __future__ import annotations

import csv
import sqlite3
import re
import json
from difflib import SequenceMatcher
from collections import Counter
from functools import lru_cache
from pathlib import Path
from typing import Any

from ibm_watsonx_orchestrate.agent_builder.tools import tool

CARDS_CSV = Path(__file__).resolve().parent / "data" / "cards.csv"
SETS_CSV = Path(__file__).resolve().parent / "data" / "sets.csv"
_SQL_BLOCKLIST_RE = re.compile(
    r"\b(insert|update|delete|drop|alter|create|replace|truncate|attach|detach|pragma|vacuum|reindex|analyze)\b",
    re.IGNORECASE,
)


def _norm(value: str | None) -> str:
    return (value or "").strip().lower()


def _norm_name(value: str | None) -> str:
    """Normalize card names for loose matching (hyphen/space/punctuation-insensitive)."""
    s = _norm(value)
    for ch in ("-", "‐", "‑", "‒", "–", "—", "―", " ", "'", "’", ".", ","):
        s = s.replace(ch, "")
    return s


@lru_cache(maxsize=1)
def _load_sets() -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    with SETS_CSV.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
    return rows


@lru_cache(maxsize=1)
def _load_cards() -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    with CARDS_CSV.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
    return rows


def _match_card(
    row: dict[str, str],
    set_id: str | None,
    set_name: str | None,
    supertype: str | None,
    rarity: str | None,
    type_name: str | None,
) -> bool:
    if set_id:
        if _norm(row.get("id", "").split("-", 1)[0]) != _norm(set_id):
            return False

    if set_name:
        target = _norm(set_name)
        derived_set = _norm(row.get("id", "").split("-", 1)[0])
        # Match against set id prefix as fallback if set_name wasn't projected into card rows.
        if target not in _norm(row.get("set_name", "")) and target != derived_set:
            return False

    if supertype and _norm(row.get("supertype")) != _norm(supertype):
        return False

    if rarity and _norm(row.get("rarity")) != _norm(rarity):
        return False

    if type_name:
        raw_types = _norm(row.get("types", ""))
        if _norm(type_name) not in raw_types:
            return False

    return True


def _card_set_id(row: dict[str, str]) -> str:
    return (row.get("id", "").split("-", 1)[0]).strip()


@lru_cache(maxsize=1)
def _distinct_card_names() -> list[str]:
    names = {(row.get("name") or "").strip() for row in _load_cards()}
    return sorted([n for n in names if n])


def _resolve_set_name_candidates(set_hint: str, max_candidates: int = 5) -> list[str]:
    hint_norm = _norm_name(set_hint)
    if not hint_norm:
        return []
    set_names = _distinct_set_names()
    exact = [n for n in set_names if _norm_name(n) == hint_norm]
    if exact:
        return exact[: max(1, max_candidates)]

    scored: list[tuple[str, float]] = []
    for name in set_names:
        score = _name_similarity_score(hint_norm, _norm_name(name))
        if score >= 0.6:
            scored.append((name, score))
    scored.sort(key=lambda x: (-x[1], x[0]))
    return [n for n, _ in scored[: max(1, max_candidates)]]


def _distinct_card_names_for_set_hint(set_hint: str, max_set_candidates: int = 5) -> tuple[list[str], list[str]]:
    set_candidates = _resolve_set_name_candidates(set_hint, max_candidates=max_set_candidates)
    if not set_candidates:
        return [], []
    set_norms = {_norm_name(s) for s in set_candidates}
    names = {
        (row.get("name") or "").strip()
        for row in _load_cards()
        if _norm_name(row.get("set_name") or "") in set_norms and (row.get("name") or "").strip()
    }
    return sorted(names), set_candidates


@lru_cache(maxsize=1)
def _distinct_set_names() -> list[str]:
    names = {(row.get("name") or "").strip() for row in _load_sets()}
    return sorted([n for n in names if n])


def _name_similarity_score(query_norm: str, candidate_norm: str) -> float:
    if not query_norm or not candidate_norm:
        return 0.0
    if query_norm == candidate_norm:
        return 1.0
    ratio = SequenceMatcher(None, query_norm, candidate_norm).ratio()
    # Boost containment to make partial user queries practical.
    if query_norm in candidate_norm or candidate_norm in query_norm:
        ratio = max(ratio, 0.88)
    return ratio


def _resolve_name_candidates(names: list[str], name_query: str, max_candidates: int) -> dict[str, Any]:
    q_norm = _norm_name(name_query)
    if not q_norm:
        return {
            "query": name_query,
            "query_normalized": q_norm,
            "exact_match": False,
            "exact_names": [],
            "needs_disambiguation": False,
            "candidates": [],
        }

    exact = sorted([n for n in names if _norm_name(n) == q_norm])
    if exact:
        return {
            "query": name_query,
            "query_normalized": q_norm,
            "exact_match": True,
            "exact_names": exact,
            "needs_disambiguation": False,
            "candidates": [{"name": n, "score": 1.0} for n in exact[: max(1, max_candidates)]],
        }

    scored: list[tuple[str, float]] = []
    for n in names:
        score = _name_similarity_score(q_norm, _norm_name(n))
        if score >= 0.62:
            scored.append((n, score))

    scored.sort(key=lambda x: (-x[1], x[0]))
    top = scored[: max(1, max_candidates)]
    return {
        "query": name_query,
        "query_normalized": q_norm,
        "exact_match": False,
        "exact_names": [],
        "needs_disambiguation": bool(top),
        "candidates": [{"name": n, "score": round(s, 4)} for n, s in top],
    }


def _resolution_decision(candidates: list[dict[str, Any]], exact_match: bool) -> tuple[str, float]:
    if exact_match:
        return "auto_select", 1.0
    if not candidates:
        return "clarify", 0.0
    top = float(candidates[0].get("score", 0.0))
    second = float(candidates[1].get("score", 0.0)) if len(candidates) > 1 else 0.0
    margin = top - second
    if top >= 0.92 and margin >= 0.08:
        return "auto_select", top
    if top >= 0.72:
        return "disambiguate", top
    return "clarify", top


def _resolve_card_candidates_with_context(name_query: str, set_hint: str | None, max_candidates: int) -> dict[str, Any]:
    q_norm = _norm_name(name_query)
    if not q_norm:
        return {
            "query": name_query,
            "query_normalized": "",
            "exact_match": False,
            "exact_names": [],
            "needs_disambiguation": False,
            "candidates": [],
            "constraint_set_hint": set_hint or "",
            "constraint_set_matches": [],
            "decision": "clarify",
            "confidence": 0.0,
            "selected": None,
        }

    set_matches: list[str] = []
    allowed_set_norms: set[str] | None = None
    if set_hint and _norm(set_hint):
        set_matches = _resolve_set_name_candidates(set_hint, max_candidates=5)
        if set_matches:
            allowed_set_norms = {_norm_name(s) for s in set_matches}

    # Unique candidates at (card_name, set_id, set_name) grain so disambiguation can be set-aware.
    seen: set[tuple[str, str, str]] = set()
    candidates_raw: list[dict[str, Any]] = []
    exact_names: set[str] = set()
    for row in _load_cards():
        card_name = (row.get("name") or "").strip()
        set_id = (row.get("set_id") or _card_set_id(row)).strip()
        set_name = (row.get("set_name") or "").strip()
        if not card_name:
            continue
        if allowed_set_norms is not None and _norm_name(set_name) not in allowed_set_norms:
            continue
        key = (card_name, set_id, set_name)
        if key in seen:
            continue
        seen.add(key)

        cnorm = _norm_name(card_name)
        if cnorm == q_norm:
            exact_names.add(card_name)
        score = _name_similarity_score(q_norm, cnorm)
        if score < 0.62 and q_norm not in cnorm:
            continue
        candidates_raw.append(
            {
                "card_name": card_name,
                "set_id": set_id,
                "set_name": set_name,
                "score": round(max(score, 0.88 if q_norm in cnorm else score), 4),
            }
        )

    candidates_raw.sort(key=lambda x: (-float(x.get("score", 0.0)), x.get("card_name", ""), x.get("set_name", "")))
    candidates = candidates_raw[: max(1, max_candidates)]
    exact = sorted(exact_names)
    exact_match = bool(exact)
    decision, confidence = _resolution_decision(candidates, exact_match)
    selected = candidates[0] if candidates and decision == "auto_select" else None
    return {
        "query": name_query,
        "query_normalized": q_norm,
        "exact_match": exact_match,
        "exact_names": exact,
        "needs_disambiguation": decision == "disambiguate",
        "candidates": candidates,
        "constraint_set_hint": set_hint or "",
        "constraint_set_matches": set_matches,
        "decision": decision,
        "confidence": round(confidence, 4),
        "selected": selected,
    }


@lru_cache(maxsize=1)
def _db_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    # Expose name normalization to SQL so model queries can match chiyu == chi-yu == chi yu.
    conn.create_function("norm_name", 1, _norm_name)

    conn.execute(
        """
        CREATE TABLE sets (
            id TEXT,
            name TEXT,
            set_id TEXT PRIMARY KEY,
            set_name TEXT,
            series TEXT,
            printed_total INTEGER,
            total INTEGER,
            release_date TEXT,
            updated_at TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE cards (
            id TEXT,
            name TEXT,
            card_id TEXT PRIMARY KEY,
            set_id TEXT,
            set_name TEXT,
            set_series TEXT,
            release_date TEXT,
            updated_at TEXT,
            card_name TEXT,
            supertype TEXT,
            subtypes TEXT,
            types TEXT,
            rarity TEXT,
            card_number TEXT,
            hp TEXT,
            artist TEXT,
            flavor_text TEXT,
            regulation_mark TEXT,
            legal_unlimited TEXT,
            legal_expanded TEXT,
            legal_standard TEXT,
            evolves_from TEXT,
            evolves_to TEXT,
            abilities_json TEXT,
            attacks_json TEXT,
            rules_json TEXT,
            weaknesses_json TEXT,
            resistances_json TEXT,
            retreat_cost_json TEXT,
            converted_retreat_cost TEXT,
            national_pokedex_numbers_json TEXT,
            tcgplayer_url TEXT,
            cardmarket_url TEXT,
            image_large TEXT
        )
        """
    )

    with SETS_CSV.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            conn.execute(
                """
                INSERT INTO sets(id, name, set_id, set_name, series, printed_total, total, release_date, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    (row.get("id") or "").strip(),
                    (row.get("name") or "").strip(),
                    (row.get("id") or "").strip(),
                    (row.get("name") or "").strip(),
                    (row.get("series") or "").strip(),
                    int((row.get("printed_total") or "0").strip() or 0),
                    int((row.get("total") or "0").strip() or 0),
                    (row.get("release_date") or "").strip(),
                    (row.get("updated_at") or "").strip(),
                ),
            )

    with CARDS_CSV.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        set_lookup: dict[str, dict[str, str]] = {
            (s.get("id") or "").strip(): s for s in _load_sets() if isinstance(s, dict) and s.get("id")
        }
        for row in reader:
            set_id = ((row.get("set_id") or "").strip() or _card_set_id(row))
            set_row = set_lookup.get(set_id, {})
            payload = {}
            raw_payload = row.get("payload_json") or ""
            if raw_payload:
                try:
                    payload = json.loads(raw_payload)
                except Exception:
                    payload = {}
            images = payload.get("images") if isinstance(payload, dict) else {}
            image_large = ""
            if isinstance(images, dict):
                image_large = str(images.get("large") or "")
            set_obj = payload.get("set") if isinstance(payload, dict) else {}
            legalities = payload.get("legalities") if isinstance(payload, dict) else {}
            tcgplayer = payload.get("tcgplayer") if isinstance(payload, dict) else {}
            cardmarket = payload.get("cardmarket") if isinstance(payload, dict) else {}
            conn.execute(
                """
                INSERT INTO cards(
                    id, name, card_id, set_id, set_name, set_series, release_date, updated_at,
                    card_name, supertype, subtypes, types, rarity, card_number, hp, artist,
                    flavor_text, regulation_mark, legal_unlimited, legal_expanded, legal_standard,
                    evolves_from, evolves_to, abilities_json, attacks_json, rules_json,
                    weaknesses_json, resistances_json, retreat_cost_json, converted_retreat_cost,
                    national_pokedex_numbers_json, tcgplayer_url, cardmarket_url, image_large
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    (row.get("id") or "").strip(),
                    (row.get("name") or "").strip(),
                    (row.get("id") or "").strip(),
                    set_id,
                    (
                        row.get("set_name")
                        or (set_obj.get("name") if isinstance(set_obj, dict) else "")
                        or set_row.get("name", "")
                        or ""
                    ).strip(),
                    (
                        row.get("set_series")
                        or (set_obj.get("series") if isinstance(set_obj, dict) else "")
                        or set_row.get("series", "")
                        or ""
                    ).strip(),
                    (
                        row.get("set_release_date")
                        or row.get("release_date")
                        or (set_obj.get("releaseDate") if isinstance(set_obj, dict) else "")
                        or set_row.get("release_date", "")
                        or ""
                    ).strip(),
                    (
                        row.get("set_updated_at")
                        or row.get("updated_at")
                        or (set_obj.get("updatedAt") if isinstance(set_obj, dict) else "")
                        or set_row.get("updated_at", "")
                        or ""
                    ).strip(),
                    (row.get("name") or "").strip(),
                    (row.get("supertype") or "").strip(),
                    (row.get("subtypes") or "").strip(),
                    (row.get("types") or "").strip(),
                    (row.get("rarity") or "").strip(),
                    (row.get("number") or "").strip(),
                    (row.get("hp") or "").strip(),
                    (row.get("artist") or "").strip(),
                    (payload.get("flavorText") if isinstance(payload, dict) else "") or "",
                    (payload.get("regulationMark") if isinstance(payload, dict) else "") or "",
                    (legalities.get("unlimited") if isinstance(legalities, dict) else "") or "",
                    (legalities.get("expanded") if isinstance(legalities, dict) else "") or "",
                    (legalities.get("standard") if isinstance(legalities, dict) else "") or "",
                    (payload.get("evolvesFrom") if isinstance(payload, dict) else "") or "",
                    json.dumps((payload.get("evolvesTo") if isinstance(payload, dict) else []) or [], ensure_ascii=False),
                    json.dumps((payload.get("abilities") if isinstance(payload, dict) else []) or [], ensure_ascii=False),
                    json.dumps((payload.get("attacks") if isinstance(payload, dict) else []) or [], ensure_ascii=False),
                    json.dumps((payload.get("rules") if isinstance(payload, dict) else []) or [], ensure_ascii=False),
                    json.dumps((payload.get("weaknesses") if isinstance(payload, dict) else []) or [], ensure_ascii=False),
                    json.dumps((payload.get("resistances") if isinstance(payload, dict) else []) or [], ensure_ascii=False),
                    json.dumps((payload.get("retreatCost") if isinstance(payload, dict) else []) or [], ensure_ascii=False),
                    str((payload.get("convertedRetreatCost") if isinstance(payload, dict) else "") or ""),
                    json.dumps((payload.get("nationalPokedexNumbers") if isinstance(payload, dict) else []) or [], ensure_ascii=False),
                    (tcgplayer.get("url") if isinstance(tcgplayer, dict) else "") or "",
                    (cardmarket.get("url") if isinstance(cardmarket, dict) else "") or "",
                    image_large,
                ),
            )

    conn.execute("CREATE INDEX idx_cards_set_id ON cards(set_id)")
    conn.execute("CREATE INDEX idx_cards_name ON cards(card_name)")
    conn.execute("CREATE INDEX idx_cards_name_alias ON cards(name)")
    conn.execute("CREATE INDEX idx_cards_types ON cards(types)")
    conn.execute("CREATE INDEX idx_sets_name_alias ON sets(name)")
    conn.commit()
    return conn


def _validate_sql_query(query: str) -> tuple[bool, str]:
    q = (query or "").strip()
    if not q:
        return False, "Query is empty."
    if len(q) > 6000:
        return False, "Query is too long."

    normalized = q.lower().lstrip()
    if not (normalized.startswith("select") or normalized.startswith("with")):
        return False, "Only SELECT/ WITH read-only queries are allowed."

    if _SQL_BLOCKLIST_RE.search(q):
        return False, "Query contains blocked SQL keywords."

    # Prevent multi-statement execution.
    q_no_trailing = q.rstrip().rstrip(";").strip()
    if ";" in q_no_trailing:
        return False, "Only a single SQL statement is allowed."

    return True, ""


def _schema_help_payload() -> dict[str, Any]:
    return {
        "tables": {
            "cards": [
                "id (alias of card_id)",
                "name (alias of card_name)",
                "card_id",
                "set_id",
                "set_name",
                "set_series",
                "release_date",
                "updated_at",
                "card_name",
                "supertype",
                "subtypes",
                "types",
                "rarity",
                "card_number",
                "hp",
                "artist",
                "flavor_text",
                "regulation_mark",
                "legal_unlimited",
                "legal_expanded",
                "legal_standard",
                "evolves_from",
                "evolves_to",
                "abilities_json",
                "attacks_json",
                "rules_json",
                "weaknesses_json",
                "resistances_json",
                "retreat_cost_json",
                "converted_retreat_cost",
                "national_pokedex_numbers_json",
                "tcgplayer_url",
                "cardmarket_url",
                "image_large",
            ],
            "sets": [
                "id (alias of set_id)",
                "name (alias of set_name)",
                "set_id",
                "set_name",
                "series",
                "printed_total",
                "total",
                "release_date",
                "updated_at",
            ],
        },
        "common_column_mappings": {
            "name": "cards.card_name or sets.set_name",
            "id": "cards.card_id or sets.set_id",
            "number": "cards.card_number",
            "set.name": "sets.set_name",
            "set.id": "sets.set_id",
        },
        "examples": [
            "SELECT COUNT(*) AS pikachu_cards FROM cards WHERE LOWER(card_name) LIKE '%pikachu%';",
            "SELECT COUNT(*) AS chiyu_cards FROM cards WHERE norm_name(card_name) LIKE '%' || norm_name('chiyu') || '%';",
            "SELECT s.set_name, s.set_id, COUNT(*) AS fire_cards FROM cards c JOIN sets s ON c.set_id=s.set_id WHERE LOWER(c.types) LIKE '%fire%' GROUP BY s.set_id, s.set_name ORDER BY fire_cards DESC;",
            "SELECT card_name, set_id, image_large FROM cards WHERE LOWER(card_name) LIKE '%pikachu%' AND image_large <> '' LIMIT 10;",
            "SELECT card_name, set_name, legal_standard FROM cards WHERE LOWER(card_name)='charizard' ORDER BY release_date DESC LIMIT 20;",
            "SELECT card_name, attacks_json FROM cards WHERE card_id='base1-4';",
        ],
    }


@tool(name="pokemon_tcg_count_total_cards", description="Return total number of Pokemon TCG cards in local CSV data.")
def pokemon_tcg_count_total_cards() -> dict[str, Any]:
    """Count all cards in the local Pokemon TCG cards CSV.

    Returns:
        dict: Contains total_cards as an integer.
    """
    cards = _load_cards()
    return {"total_cards": len(cards)}


@tool(name="pokemon_tcg_count_total_sets", description="Return total number of Pokemon TCG sets in local CSV data.")
def pokemon_tcg_count_total_sets() -> dict[str, Any]:
    """Count all sets in the local Pokemon TCG sets CSV.

    Returns:
        dict: Contains total_sets as an integer.
    """
    sets_ = _load_sets()
    return {"total_sets": len(sets_)}


@tool(
    name="pokemon_tcg_count_cards",
    description="Count cards by optional filters (set_id, set_name, supertype, rarity, type_name).",
)
def pokemon_tcg_count_cards(
    set_id: str | None = None,
    set_name: str | None = None,
    supertype: str | None = None,
    rarity: str | None = None,
    type_name: str | None = None,
) -> dict[str, Any]:
    """Count cards using optional filters.

    Args:
        set_id: Set id prefix (for example: "base1", "sv4").
        set_name: Set name hint (best-effort).
        supertype: Exact supertype value (for example: "Pokémon", "Trainer").
        rarity: Exact rarity value.
        type_name: Type token to match in the card types list (for example: "Fire").

    Returns:
        dict: Filter echo and matching count.
    """
    cards = _load_cards()
    count = 0
    for row in cards:
        if _match_card(row, set_id, set_name, supertype, rarity, type_name):
            count += 1
    return {
        "filters": {
            "set_id": set_id,
            "set_name": set_name,
            "supertype": supertype,
            "rarity": rarity,
            "type_name": type_name,
        },
        "matching_cards": count,
    }


@tool(name="pokemon_tcg_top_sets_by_card_count", description="Return top sets ordered by card count.")
def pokemon_tcg_top_sets_by_card_count(limit: int = 10) -> dict[str, Any]:
    """List top sets by number of cards in cards CSV.

    Args:
        limit: Number of sets to return.

    Returns:
        dict: Top sets with card counts.
    """
    cards = _load_cards()
    counts: Counter[str] = Counter()
    for row in cards:
        sid = row.get("id", "").split("-", 1)[0].strip()
        if sid:
            counts[sid] += 1

    set_name_by_id = {s.get("id", ""): s.get("name", "") for s in _load_sets()}
    top = counts.most_common(max(1, limit))
    return {
        "limit": max(1, limit),
        "results": [
            {
                "set_id": sid,
                "set_name": set_name_by_id.get(sid, ""),
                "card_count": cnt,
            }
            for sid, cnt in top
        ],
    }


@tool(
    name="pokemon_tcg_sets_by_card_count",
    description="Return sets ordered by card count, optionally limited (full list when limit<=0).",
)
def pokemon_tcg_sets_by_card_count(limit: int = 0) -> dict[str, Any]:
    """List sets by card count, descending.

    Args:
        limit: Max rows to return. Use 0 or negative for full list.

    Returns:
        dict: Ordered set counts and whether result is truncated.
    """
    cards = _load_cards()
    counts: Counter[str] = Counter()
    for row in cards:
        sid = _card_set_id(row)
        if sid:
            counts[sid] += 1

    set_name_by_id = {s.get("id", ""): s.get("name", "") for s in _load_sets()}
    ordered = sorted(counts.items(), key=lambda x: (-x[1], x[0]))
    total_rows = len(ordered)
    if limit and limit > 0:
        ordered = ordered[:limit]

    return {
        "requested_limit": limit,
        "returned_rows": len(ordered),
        "total_rows": total_rows,
        "is_truncated": len(ordered) < total_rows,
        "results": [
            {
                "rank": idx + 1,
                "set_id": sid,
                "set_name": set_name_by_id.get(sid, ""),
                "card_count": cnt,
            }
            for idx, (sid, cnt) in enumerate(ordered)
        ],
    }


@tool(name="pokemon_tcg_count_sets_by_series", description="Count sets grouped by series.")
def pokemon_tcg_count_sets_by_series() -> dict[str, Any]:
    """Count number of sets per series.

    Returns:
        dict: Series counts sorted descending.
    """
    counts: Counter[str] = Counter()
    for row in _load_sets():
        series = (row.get("series") or "Unknown").strip() or "Unknown"
        counts[series] += 1

    ordered = sorted(counts.items(), key=lambda x: (-x[1], x[0]))
    return {
        "results": [{"series": series, "set_count": count} for series, count in ordered]
    }


@tool(
    name="pokemon_tcg_count_cards_by_type_per_set",
    description="Count cards of a given type grouped by set.",
)
def pokemon_tcg_count_cards_by_type_per_set(type_name: str, limit: int = 40) -> dict[str, Any]:
    """Count cards matching a type and group by set.

    Args:
        type_name: Card type token (for example: "Fire", "Water").
        limit: Max number of set rows to return.

    Returns:
        dict: Per-set counts sorted descending.
    """
    counts: Counter[str] = Counter()
    for row in _load_cards():
        raw_types = _norm(row.get("types", ""))
        if _norm(type_name) and _norm(type_name) in raw_types:
            sid = _card_set_id(row)
            if sid:
                counts[sid] += 1

    set_name_by_id = {s.get("id", ""): s.get("name", "") for s in _load_sets()}
    ordered = sorted(counts.items(), key=lambda x: (-x[1], x[0]))[: max(1, limit)]
    return {
        "type_name": type_name,
        "set_count": len(counts),
        "results": [
            {
                "set_id": sid,
                "set_name": set_name_by_id.get(sid, ""),
                "card_count": cnt,
            }
            for sid, cnt in ordered
        ],
    }


@tool(
    name="pokemon_tcg_count_cards_by_name",
    description="Count cards by name search (contains or exact).",
)
def pokemon_tcg_count_cards_by_name(name_query: str, exact: bool = False) -> dict[str, Any]:
    """Count cards matching a name query.

    Args:
        name_query: Card name text to match (for example: "Pikachu").
        exact: If true, require exact card name match. Otherwise substring match.

    Returns:
        dict: Total matching cards and top matching names.
    """
    needle = _norm_name(name_query)
    if not needle:
        return {"name_query": name_query, "exact": exact, "matching_cards": 0, "top_names": []}

    name_counts: Counter[str] = Counter()
    for row in _load_cards():
        card_name = (row.get("name") or "").strip()
        card_name_norm = _norm_name(card_name)
        if not card_name_norm:
            continue

        matched = card_name_norm == needle if exact else needle in card_name_norm
        if matched:
            name_counts[card_name] += 1

    top_names = sorted(name_counts.items(), key=lambda x: (-x[1], x[0]))[:25]
    return {
        "name_query": name_query,
        "exact": exact,
        "matching_cards": sum(name_counts.values()),
        "distinct_name_count": len(name_counts),
        "top_names": [{"name": name, "card_count": cnt} for name, cnt in top_names],
    }


@tool(
    name="pokemon_tcg_resolve_entity_name",
    description="Resolve card/set name entities deterministically: exact match first, otherwise closest candidates for disambiguation.",
)
def pokemon_tcg_resolve_entity_name(
    name_query: str,
    entity_type: str = "auto",
    set_hint: str | None = None,
    max_candidates: int = 5,
) -> dict[str, Any]:
    """Resolve a user-provided entity name against card/set names.

    Args:
        name_query: Raw user-provided entity text.
        entity_type: One of auto, card, set.
        set_hint: Optional set-name hint used to constrain card candidates.
        max_candidates: Max candidate names to return.

    Returns:
        dict: Exact-match decision and candidate list for disambiguation.
    """
    et = _norm(entity_type)
    if et not in {"auto", "card", "set"}:
        et = "auto"
    max_n = max(1, min(int(max_candidates), 10))

    constrained_card_names: list[str] = []
    matched_set_names: list[str] = []
    if set_hint and _norm(set_hint):
        constrained_card_names, matched_set_names = _distinct_card_names_for_set_hint(set_hint)
    card_names = constrained_card_names if constrained_card_names else _distinct_card_names()
    card_res = _resolve_card_candidates_with_context(name_query, set_hint, max_n)
    set_res = _resolve_name_candidates(_distinct_set_names(), name_query, max_n)
    # Ensure fields are present for cards even when unconstrained helper path is used.
    card_res["constraint_set_hint"] = set_hint or ""
    card_res["constraint_set_matches"] = matched_set_names or card_res.get("constraint_set_matches", [])

    if et == "card":
        return {"entity_type": "card", **card_res}
    if et == "set":
        decision, confidence = _resolution_decision(set_res.get("candidates", []), bool(set_res.get("exact_match")))
        selected = set_res.get("candidates", [None])[0] if (set_res.get("candidates") and decision == "auto_select") else None
        return {"entity_type": "set", **set_res, "decision": decision, "confidence": round(confidence, 4), "selected": selected}

    # auto: prefer any exact match; otherwise choose richer candidate set by top score.
    if card_res["exact_match"] and not set_res["exact_match"]:
        return {"entity_type": "card", **card_res}
    if set_res["exact_match"] and not card_res["exact_match"]:
        decision, confidence = _resolution_decision(set_res.get("candidates", []), bool(set_res.get("exact_match")))
        selected = set_res.get("candidates", [None])[0] if (set_res.get("candidates") and decision == "auto_select") else None
        return {"entity_type": "set", **set_res, "decision": decision, "confidence": round(confidence, 4), "selected": selected}
    if card_res["exact_match"] and set_res["exact_match"]:
        return {"entity_type": "card", **card_res}

    card_top = card_res["candidates"][0]["score"] if card_res["candidates"] else 0.0
    set_top = set_res["candidates"][0]["score"] if set_res["candidates"] else 0.0
    if card_top >= set_top:
        return {"entity_type": "card", **card_res}
    decision, confidence = _resolution_decision(set_res.get("candidates", []), bool(set_res.get("exact_match")))
    selected = set_res.get("candidates", [None])[0] if (set_res.get("candidates") and decision == "auto_select") else None
    return {"entity_type": "set", **set_res, "decision": decision, "confidence": round(confidence, 4), "selected": selected}


@tool(
    name="pokemon_tcg_sql_schema",
    description="Describe available SQL tables/columns and example query patterns for dynamic analytics.",
)
def pokemon_tcg_sql_schema() -> dict[str, Any]:
    """Provide schema details for SQL analytics over Pokemon TCG CSV data."""
    payload = _schema_help_payload()
    payload["notes"] = [
            "cards.set_id joins to sets.set_id",
            "cards.types and cards.subtypes are serialized list strings; use LIKE for containment",
            "Only SELECT and WITH queries are supported",
        ]
    return payload


@tool(
    name="pokemon_tcg_sql_query",
    description="Run read-only SQL (SELECT/WITH) against local cards/sets tables for dynamic counts and grouped analytics.",
)
def pokemon_tcg_sql_query(query: str, max_rows: int = 200) -> dict[str, Any]:
    """Run a safe read-only SQL query over in-memory SQLite tables.

    Args:
        query: SQL SELECT/WITH statement.
        max_rows: Maximum rows to return (1-1000).

    Returns:
        dict: Query results with columns and rows.
    """
    ok, reason = _validate_sql_query(query)
    if not ok:
        return {"ok": False, "error": reason}

    limit = max(1, min(int(max_rows), 1000))
    q = (query or "").strip().rstrip(";").strip()
    wrapped = f"SELECT * FROM ({q}) AS q LIMIT {limit + 1}"

    conn = _db_conn()
    try:
        cur = conn.execute(wrapped)
        rows_raw = cur.fetchall()
        columns = [d[0] for d in (cur.description or [])]
    except sqlite3.OperationalError as exc:
        return {
            "ok": False,
            "error": str(exc),
            "hint": "Query references invalid SQL syntax/columns. Use cards.* and sets.* schema below.",
            "schema_help": _schema_help_payload(),
        }

    truncated = len(rows_raw) > limit
    rows_raw = rows_raw[:limit]
    rows = [{col: row[col] for col in columns} for row in rows_raw]

    return {
        "ok": True,
        "max_rows": limit,
        "returned_rows": len(rows),
        "truncated": truncated,
        "columns": columns,
        "rows": rows,
    }
