#!/usr/bin/env python3
"""Resumable exporter from official PokemonTCG bulk data repo.

Source: https://github.com/PokemonTCG/pokemon-tcg-data
"""

from __future__ import annotations

import argparse
import csv
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

REPO_URL = "https://github.com/PokemonTCG/pokemon-tcg-data.git"

CARDS_COLUMNS = [
    "set_id",
    "set_name",
    "set_series",
    "set_printed_total",
    "set_total",
    "set_ptcgo_code",
    "set_release_date",
    "set_updated_at",
    "id",
    "name",
    "supertype",
    "subtypes",
    "types",
    "rarity",
    "number",
    "hp",
    "artist",
    "flavor_text",
    "regulation_mark",
    "evolves_from",
    "evolves_to",
    "national_pokedex_numbers",
    "abilities",
    "attacks",
    "rules",
    "weaknesses",
    "resistances",
    "retreat_cost",
    "converted_retreat_cost",
    "legal_unlimited",
    "legal_expanded",
    "legal_standard",
    "tcgplayer_url",
    "cardmarket_url",
    "image_large",
    "payload_json",
]

SETS_COLUMNS = [
    "id",
    "name",
    "series",
    "printed_total",
    "total",
    "release_date",
    "updated_at",
    "ptcgo_code",
    "legal_unlimited",
    "legal_expanded",
    "legal_standard",
    "image_symbol",
    "image_logo",
    "payload_json",
]

VALUE_COLUMNS = ["value"]


def run(cmd: list[str], cwd: Path | None = None) -> None:
    proc = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True)
    if proc.returncode != 0:
        raise RuntimeError(f"Command failed: {' '.join(cmd)}\n{proc.stderr.strip()}")


def ensure_repo(cache_dir: Path) -> Path:
    repo_dir = cache_dir / "pokemon-tcg-data"
    cache_dir.mkdir(parents=True, exist_ok=True)

    if not repo_dir.exists():
        run(["git", "clone", "--depth", "1", REPO_URL, str(repo_dir)])
    else:
        run(["git", "fetch", "origin", "master", "--depth", "1"], cwd=repo_dir)
        run(["git", "reset", "--hard", "origin/master"], cwd=repo_dir)
    return repo_dir


def ensure_csv(path: Path, columns: list[str]) -> None:
    if path.exists():
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()


def append_csv(path: Path, columns: list[str], rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    with path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writerows(rows)


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def dump_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def export(repo_dir: Path, out_dir: Path, force_restart: bool) -> None:
    state_dir = out_dir / "state"
    csv_dir = out_dir / "csv"
    state_path = state_dir / "github_export_state.json"

    cards_csv = csv_dir / "cards.csv"
    sets_csv = csv_dir / "sets.csv"
    types_csv = csv_dir / "types.csv"
    subtypes_csv = csv_dir / "subtypes.csv"
    supertypes_csv = csv_dir / "supertypes.csv"
    rarities_csv = csv_dir / "rarities.csv"

    if force_restart:
        for p in [cards_csv, sets_csv, types_csv, subtypes_csv, supertypes_csv, rarities_csv, state_path]:
            if p.exists():
                p.unlink()

    ensure_csv(cards_csv, CARDS_COLUMNS)
    ensure_csv(sets_csv, SETS_COLUMNS)
    ensure_csv(types_csv, VALUE_COLUMNS)
    ensure_csv(subtypes_csv, VALUE_COLUMNS)
    ensure_csv(supertypes_csv, VALUE_COLUMNS)
    ensure_csv(rarities_csv, VALUE_COLUMNS)

    state_dir.mkdir(parents=True, exist_ok=True)
    state: dict[str, Any]
    if state_path.exists():
        state = load_json(state_path)
    else:
        state = {
            "processed_set_files": [],
            "cards_written": 0,
            "sets_written": 0,
            "completed": False,
        }

    if state.get("completed"):
        print("[github-export] already completed, skipping.")
        return

    sets_path = repo_dir / "sets" / "en.json"
    sets_data = load_json(sets_path)
    if not isinstance(sets_data, list):
        raise RuntimeError("Unexpected sets/en.json format")
    set_by_id: dict[str, dict[str, Any]] = {
        str(s.get("id", "")).strip(): s for s in sets_data if isinstance(s, dict) and s.get("id")
    }

    if state.get("sets_written", 0) == 0:
        rows = []
        for s in sets_data:
            rows.append(
                {
                    "id": s.get("id", ""),
                    "name": s.get("name", ""),
                    "series": s.get("series", ""),
                    "printed_total": s.get("printedTotal", ""),
                    "total": s.get("total", ""),
                    "release_date": s.get("releaseDate", ""),
                    "updated_at": s.get("updatedAt", ""),
                    "ptcgo_code": s.get("ptcgoCode", ""),
                    "legal_unlimited": (s.get("legalities", {}) or {}).get("unlimited", ""),
                    "legal_expanded": (s.get("legalities", {}) or {}).get("expanded", ""),
                    "legal_standard": (s.get("legalities", {}) or {}).get("standard", ""),
                    "image_symbol": (s.get("images", {}) or {}).get("symbol", ""),
                    "image_logo": (s.get("images", {}) or {}).get("logo", ""),
                    "payload_json": dump_json(s),
                }
            )
        append_csv(sets_csv, SETS_COLUMNS, rows)
        state["sets_written"] = len(rows)
        state_path.write_text(json.dumps(state, indent=2), encoding="utf-8")
        print(f"[github-export] sets.csv rows={len(rows)}")

    card_files = sorted((repo_dir / "cards" / "en").glob("*.json"))
    done = set(state.get("processed_set_files", []))
    types: set[str] = set()
    subtypes: set[str] = set()
    supertypes: set[str] = set()
    rarities: set[str] = set()

    # If rerun, rebuild facets from existing cards CSV may be expensive;
    # keep it simple by deriving from all card files once at end.

    for idx, file_path in enumerate(card_files, start=1):
        if file_path.name in done:
            continue

        cards = load_json(file_path)
        if not isinstance(cards, list):
            raise RuntimeError(f"Unexpected format in {file_path}")

        rows = []
        for c in cards:
            set_obj = c.get("set", {}) if isinstance(c, dict) else {}
            card_id = c.get("id", "") if isinstance(c, dict) else ""
            derived_set_id = str(card_id).split("-", 1)[0] if card_id else ""
            if not isinstance(set_obj, dict) or not set_obj:
                set_obj = set_by_id.get(derived_set_id, {})
            legal = c.get("legalities", {}) if isinstance(c, dict) else {}
            images = c.get("images", {}) if isinstance(c, dict) else {}
            tcgplayer = c.get("tcgplayer", {}) if isinstance(c, dict) else {}
            cardmarket = c.get("cardmarket", {}) if isinstance(c, dict) else {}
            rows.append(
                {
                    "set_id": (set_obj.get("id", "") if isinstance(set_obj, dict) else "") or derived_set_id,
                    "set_name": set_obj.get("name", "") if isinstance(set_obj, dict) else "",
                    "set_series": set_obj.get("series", "") if isinstance(set_obj, dict) else "",
                    "set_printed_total": set_obj.get("printedTotal", "") if isinstance(set_obj, dict) else "",
                    "set_total": set_obj.get("total", "") if isinstance(set_obj, dict) else "",
                    "set_ptcgo_code": set_obj.get("ptcgoCode", "") if isinstance(set_obj, dict) else "",
                    "set_release_date": set_obj.get("releaseDate", "") if isinstance(set_obj, dict) else "",
                    "set_updated_at": set_obj.get("updatedAt", "") if isinstance(set_obj, dict) else "",
                    "id": c.get("id", "") if isinstance(c, dict) else "",
                    "name": c.get("name", "") if isinstance(c, dict) else "",
                    "supertype": c.get("supertype", "") if isinstance(c, dict) else "",
                    "subtypes": dump_json(c.get("subtypes", []) if isinstance(c, dict) else []),
                    "types": dump_json(c.get("types", []) if isinstance(c, dict) else []),
                    "rarity": c.get("rarity", "") if isinstance(c, dict) else "",
                    "number": c.get("number", "") if isinstance(c, dict) else "",
                    "hp": c.get("hp", "") if isinstance(c, dict) else "",
                    "artist": c.get("artist", "") if isinstance(c, dict) else "",
                    "flavor_text": c.get("flavorText", "") if isinstance(c, dict) else "",
                    "regulation_mark": c.get("regulationMark", "") if isinstance(c, dict) else "",
                    "evolves_from": c.get("evolvesFrom", "") if isinstance(c, dict) else "",
                    "evolves_to": dump_json(c.get("evolvesTo", []) if isinstance(c, dict) else []),
                    "national_pokedex_numbers": dump_json(
                        c.get("nationalPokedexNumbers", []) if isinstance(c, dict) else []
                    ),
                    "abilities": dump_json(c.get("abilities", []) if isinstance(c, dict) else []),
                    "attacks": dump_json(c.get("attacks", []) if isinstance(c, dict) else []),
                    "rules": dump_json(c.get("rules", []) if isinstance(c, dict) else []),
                    "weaknesses": dump_json(c.get("weaknesses", []) if isinstance(c, dict) else []),
                    "resistances": dump_json(c.get("resistances", []) if isinstance(c, dict) else []),
                    "retreat_cost": dump_json(c.get("retreatCost", []) if isinstance(c, dict) else []),
                    "converted_retreat_cost": c.get("convertedRetreatCost", "") if isinstance(c, dict) else "",
                    "legal_unlimited": legal.get("unlimited", "") if isinstance(legal, dict) else "",
                    "legal_expanded": legal.get("expanded", "") if isinstance(legal, dict) else "",
                    "legal_standard": legal.get("standard", "") if isinstance(legal, dict) else "",
                    "tcgplayer_url": tcgplayer.get("url", "") if isinstance(tcgplayer, dict) else "",
                    "cardmarket_url": cardmarket.get("url", "") if isinstance(cardmarket, dict) else "",
                    "image_large": images.get("large", "") if isinstance(images, dict) else "",
                    "payload_json": dump_json(c),
                }
            )
            if isinstance(c, dict):
                st = c.get("supertype")
                if st:
                    supertypes.add(str(st))
                rarity = c.get("rarity")
                if rarity:
                    rarities.add(str(rarity))
                for t in c.get("types", []) or []:
                    types.add(str(t))
                for t in c.get("subtypes", []) or []:
                    subtypes.add(str(t))

        append_csv(cards_csv, CARDS_COLUMNS, rows)
        state["cards_written"] = int(state.get("cards_written", 0)) + len(rows)
        state.setdefault("processed_set_files", []).append(file_path.name)
        state_path.write_text(json.dumps(state, indent=2), encoding="utf-8")
        print(f"[github-export] {idx}/{len(card_files)} {file_path.name}: +{len(rows)} cards (total={state['cards_written']})")

    # Build facets from all card files to ensure complete values, even across resumes.
    for file_path in card_files:
        cards = load_json(file_path)
        for c in cards:
            if not isinstance(c, dict):
                continue
            st = c.get("supertype")
            if st:
                supertypes.add(str(st))
            rarity = c.get("rarity")
            if rarity:
                rarities.add(str(rarity))
            for t in c.get("types", []) or []:
                types.add(str(t))
            for t in c.get("subtypes", []) or []:
                subtypes.add(str(t))

    for csv_path, values in [
        (types_csv, sorted(types)),
        (subtypes_csv, sorted(subtypes)),
        (supertypes_csv, sorted(supertypes)),
        (rarities_csv, sorted(rarities)),
    ]:
        with csv_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=VALUE_COLUMNS)
            writer.writeheader()
            for v in values:
                writer.writerow({"value": v})

    state["completed"] = True
    state_path.write_text(json.dumps(state, indent=2), encoding="utf-8")
    print("[github-export] completed.")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Export PokemonTCG bulk JSON data from GitHub to CSV")
    p.add_argument("--out-dir", default="data/pokemontcg", help="Output directory")
    p.add_argument("--cache-dir", default=".cache", help="Where to store cloned data repo")
    p.add_argument("--force-restart", action="store_true", help="Rebuild all CSV outputs from scratch")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    out_dir = Path(args.out_dir)
    cache_dir = Path(args.cache_dir)

    try:
        repo_dir = ensure_repo(cache_dir)
        export(repo_dir, out_dir, force_restart=args.force_restart)
    except Exception as exc:
        print(str(exc), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
