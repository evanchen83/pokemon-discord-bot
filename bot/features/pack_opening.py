from __future__ import annotations

import csv
import logging
import random
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SetCard:
    card_id: str
    name: str
    rarity: str
    number: str
    image_url: str


@dataclass(frozen=True)
class SetMeta:
    set_id: str
    name: str
    series: str
    release_date: str
    ptcgo_code: str
    image_logo: str


class PackService:
    def __init__(self, base_dir: Path):
        self.data_dir: Optional[Path] = None
        self.load_error: str = ""
        self.sets_by_id: dict[str, SetMeta] = {}
        self.cards_by_set: dict[str, list[SetCard]] = {}
        try:
            self.data_dir, self.sets_by_id, self.cards_by_set = _load_pack_catalog(base_dir)
        except FileNotFoundError as exc:
            self.load_error = str(exc)
            logger.warning("Pack data unavailable: %s", exc)

    @property
    def is_available(self) -> bool:
        return bool(self.sets_by_id)

    def autocomplete_sets(self, current: str, limit: int = 25) -> list[SetMeta]:
        needle = _norm_text(current)
        records = sorted(self.sets_by_id.values(), key=lambda s: (s.release_date, s.name), reverse=True)
        out: list[SetMeta] = []
        for rec in records:
            if needle and needle not in _norm_text(rec.name) and needle not in _norm_text(rec.set_id):
                continue
            out.append(rec)
            if len(out) >= limit:
                break
        return out

    def get_set(self, set_name_or_id: str) -> Optional[SetMeta]:
        key = _norm_text(set_name_or_id)
        if not key:
            return None
        by_id = self.sets_by_id.get(set_name_or_id.strip())
        if by_id:
            return by_id
        for record in self.sets_by_id.values():
            if _norm_text(record.set_id) == key or _norm_text(record.name) == key:
                return record
        return None

    def open_pack(self, set_id: str) -> list[SetCard]:
        return _simulate_pack(self.cards_by_set.get(set_id, []))


def rarity_bucket(rarity: str) -> str:
    r = _norm_text(rarity)
    super_markers = (
        "ultra rare",
        "special illustration rare",
        "illustration rare",
        "secret rare",
        "hyper rare",
        "double rare",
        "shiny rare",
        "black white rare",
        "ace spec",
    )
    if any(marker in r for marker in super_markers):
        return "super_rare"
    if "rare" in r:
        return "rare"
    return "normal"


def format_pull_lines(cards: list[SetCard]) -> str:
    if not cards:
        return "None this pack."
    lines = []
    for card in cards:
        num = f" #{card.number}" if card.number else ""
        lines.append(f"• {card.name}{num} ({card.rarity})")
    joined = "\n".join(lines)
    return joined if len(joined) <= 1024 else joined[:1021] + "..."


def _load_pack_catalog(base_dir: Path) -> tuple[Path, dict[str, SetMeta], dict[str, list[SetCard]]]:
    data_dir = _resolve_catalog_dir(base_dir)
    sets_path = data_dir / "sets.csv"
    cards_path = data_dir / "cards.csv"
    sets_by_id: dict[str, SetMeta] = {}
    cards_by_set: dict[str, list[SetCard]] = {}

    with sets_path.open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            set_id = (row.get("id") or "").strip()
            if not set_id:
                continue
            sets_by_id[set_id] = SetMeta(
                set_id=set_id,
                name=(row.get("name") or "").strip() or set_id,
                series=(row.get("series") or "").strip() or "Unknown",
                release_date=(row.get("release_date") or "").strip(),
                ptcgo_code=(row.get("ptcgo_code") or "").strip(),
                image_logo=(row.get("image_logo") or "").strip(),
            )

    with cards_path.open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            set_id = (row.get("set_id") or "").strip()
            card_id = (row.get("id") or "").strip()
            name = (row.get("name") or "").strip()
            if not set_id or not card_id or not name:
                continue
            cards_by_set.setdefault(set_id, []).append(
                SetCard(
                    card_id=card_id,
                    name=name,
                    rarity=(row.get("rarity") or "").strip() or "Unknown",
                    number=(row.get("number") or "").strip(),
                    image_url=(row.get("image_large") or "").strip(),
                )
            )

    return data_dir, sets_by_id, cards_by_set


def _resolve_catalog_dir(preferred: Path) -> Path:
    repo_root = Path(__file__).resolve().parents[2]
    candidates = [
        preferred,
        repo_root / "data" / "pokemontcg" / "csv",
        repo_root / "wxo" / "tools" / "data",
        Path("/app/data/pokemontcg/csv"),
        Path("/app/wxo/tools/data"),
    ]
    seen: set[str] = set()
    for candidate in candidates:
        key = str(candidate.resolve()) if candidate.exists() else str(candidate)
        if key in seen:
            continue
        seen.add(key)
        if (candidate / "sets.csv").exists() and (candidate / "cards.csv").exists():
            return candidate
    raise FileNotFoundError(
        "Could not find pack CSVs. Expected sets.csv and cards.csv in one of: "
        + ", ".join(str(p) for p in candidates)
    )


def _norm_text(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "").strip().lower())


def _pick_unique_cards(pool: list[SetCard], count: int, used: set[str]) -> list[SetCard]:
    candidates = [c for c in pool if c.card_id not in used]
    if not candidates or count <= 0:
        return []
    if len(candidates) <= count:
        return candidates
    return random.sample(candidates, count)


def _simulate_pack(cards: list[SetCard]) -> list[SetCard]:
    if not cards:
        return []
    normals = [c for c in cards if rarity_bucket(c.rarity) == "normal"]
    rares = [c for c in cards if rarity_bucket(c.rarity) == "rare"]
    super_rares = [c for c in cards if rarity_bucket(c.rarity) == "super_rare"]
    used: set[str] = set()
    pulled: list[SetCard] = []

    include_super = bool(super_rares) and random.random() < 0.25
    target_super = 1 if include_super else 0
    target_rare = 3 if include_super else 4
    target_normal = 6

    for group, count in ((normals, target_normal), (rares, target_rare), (super_rares, target_super)):
        picks = _pick_unique_cards(group, count, used)
        pulled.extend(picks)
        used.update(c.card_id for c in picks)

    if len(pulled) < 10:
        filler = _pick_unique_cards(cards, 10 - len(pulled), used)
        pulled.extend(filler)

    random.shuffle(pulled)
    return pulled[:10]
