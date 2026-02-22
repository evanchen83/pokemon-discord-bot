from __future__ import annotations

import concurrent.futures
import csv
import io
import logging
from pathlib import Path

import requests
from PIL import Image

from .catch_roll import MAX_RANDOM_POKEMON_ID
from .image_rendering import fallback_sprite
from .pagination import SpriteRateLimitError

logger = logging.getLogger(__name__)

SPRITE_URL = "https://raw.githubusercontent.com/PokeAPI/sprites/master/sprites/pokemon/other/official-artwork/{pokemon_id}.png"
SPRITE_URL_FALLBACK = "https://raw.githubusercontent.com/PokeAPI/sprites/master/sprites/pokemon/{pokemon_id}.png"
POKEAPI_POKEMON_INDEX_URL = "https://pokeapi.co/api/v2/pokemon?limit=1302"
POKEAPI_POKEMON_CSV_URL = "https://raw.githubusercontent.com/PokeAPI/pokeapi/master/data/v2/csv/pokemon.csv"
POKEAPI_POKEMON_TYPES_CSV_URL = "https://raw.githubusercontent.com/PokeAPI/pokeapi/master/data/v2/csv/pokemon_types.csv"
POKEAPI_TYPES_CSV_URL = "https://raw.githubusercontent.com/PokeAPI/pokeapi/master/data/v2/csv/types.csv"


class SpriteRepository:
    def __init__(self, cache_root: Path):
        self.sprite_cache_dir = cache_root / "sprites"
        self.sprite_cache_dir.mkdir(parents=True, exist_ok=True)
        self.thumb_cache_dir = cache_root / "sprites-thumb"
        self.thumb_cache_dir.mkdir(parents=True, exist_ok=True)
        self._name_by_id: dict[int, str] = {}
        self._name_index_loaded = False
        self._primary_type_by_id: dict[int, str] = {}
        self._type_index_loaded = False

    def load_resized_sprite(self, pokemon_id: int, size: int) -> Image.Image:
        thumb_file = self.thumb_cache_dir / f"{pokemon_id}_{size}.png"
        if thumb_file.exists():
            try:
                with Image.open(thumb_file) as img:
                    return img.convert("RGBA")
            except Exception:
                logger.warning("Failed reading cached sprite thumbnail for id=%s size=%s", pokemon_id, size)

        sprite = self.load_sprite(pokemon_id)
        resized = sprite.resize((size, size), Image.Resampling.LANCZOS)
        try:
            resized.save(thumb_file, format="PNG", optimize=True, compress_level=9)
        except Exception:
            logger.debug("Failed writing sprite thumbnail cache for id=%s size=%s", pokemon_id, size)
        return resized

    def prefetch_sprites(self, pokemon_ids: list[int]) -> None:
        unique_ids = sorted({int(pokemon_id) for pokemon_id in pokemon_ids if int(pokemon_id) > 0})
        missing = [pokemon_id for pokemon_id in unique_ids if not (self.sprite_cache_dir / f"{pokemon_id}.png").exists()]
        if not missing:
            return

        first_error: Exception | None = None
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(8, max(1, len(missing)))) as pool:
            futures = [pool.submit(self.load_sprite, pokemon_id) for pokemon_id in missing]
            for fut in concurrent.futures.as_completed(futures):
                try:
                    img = fut.result()
                    img.close()
                except SpriteRateLimitError as exc:
                    first_error = exc
                    break
                except Exception as exc:
                    first_error = first_error or exc
        if isinstance(first_error, SpriteRateLimitError):
            raise first_error

    def load_sprite(self, pokemon_id: int) -> Image.Image:
        cache_file = self.sprite_cache_dir / f"{pokemon_id}.png"
        if cache_file.exists():
            try:
                with Image.open(cache_file) as img:
                    return img.convert("RGBA")
            except Exception:
                logger.warning("Failed reading cached sprite for id=%s; re-downloading", pokemon_id)

        primary_url = SPRITE_URL.format(pokemon_id=pokemon_id)
        fallback_url = SPRITE_URL_FALLBACK.format(pokemon_id=pokemon_id)
        try:
            resp = requests.get(primary_url, timeout=8)
            if self.is_rate_limited_response(resp):
                raise SpriteRateLimitError("Sprite host rate limit encountered")
            if resp.status_code >= 400:
                fallback_resp = requests.get(fallback_url, timeout=8)
                if self.is_rate_limited_response(fallback_resp):
                    raise SpriteRateLimitError("Sprite host rate limit encountered")
                fallback_resp.raise_for_status()
                resp = fallback_resp
            else:
                resp.raise_for_status()
            cache_file.write_bytes(resp.content)
            with Image.open(io.BytesIO(resp.content)) as img:
                return img.convert("RGBA")
        except SpriteRateLimitError:
            raise
        except Exception:
            logger.warning("Failed downloading sprite for id=%s", pokemon_id, exc_info=True)
            return fallback_sprite()

    def resolve_names(self, pokemon_ids: set[int]) -> dict[int, str]:
        self.ensure_name_index()
        out: dict[int, str] = {}
        for pokemon_id in pokemon_ids:
            out[int(pokemon_id)] = self._name_by_id.get(int(pokemon_id), f"Pokemon #{int(pokemon_id)}")
        return out

    def ensure_name_index(self) -> None:
        if self._name_index_loaded:
            return
        self._name_index_loaded = True
        try:
            resp = requests.get(POKEAPI_POKEMON_INDEX_URL, timeout=10)
            resp.raise_for_status()
            data = resp.json() if resp.text else {}
            results = data.get("results", [])
            if not isinstance(results, list):
                return
            for idx, rec in enumerate(results, start=1):
                if idx > MAX_RANDOM_POKEMON_ID:
                    break
                if not isinstance(rec, dict):
                    continue
                name = str(rec.get("name") or "").strip()
                if name:
                    self._name_by_id[idx] = self.display_name(name)
        except Exception:
            logger.warning("Failed loading Pokemon names from PokeAPI; falling back to ids.", exc_info=True)

    def resolve_primary_types(self, pokemon_ids: set[int]) -> dict[int, str]:
        self.ensure_type_index()
        out: dict[int, str] = {}
        for pokemon_id in pokemon_ids:
            out[int(pokemon_id)] = self._primary_type_by_id.get(int(pokemon_id), "Unknown")
        return out

    def ensure_type_index(self) -> None:
        if self._type_index_loaded:
            return
        self._type_index_loaded = True

        try:
            pokemon_csv = self.fetch_csv_rows(POKEAPI_POKEMON_CSV_URL)
            pokemon_types_csv = self.fetch_csv_rows(POKEAPI_POKEMON_TYPES_CSV_URL)
            types_csv = self.fetch_csv_rows(POKEAPI_TYPES_CSV_URL)
        except Exception:
            logger.warning("Failed loading type metadata from PokeAPI CSVs.", exc_info=True)
            return

        type_name_by_id: dict[int, str] = {}
        for row in types_csv:
            try:
                type_id = int(row.get("id", "0") or 0)
            except Exception:
                continue
            identifier = str(row.get("identifier") or "").strip()
            if type_id > 0 and identifier:
                type_name_by_id[type_id] = self.display_name(identifier)

        default_pokemon_id_by_species: dict[int, int] = {}
        for row in pokemon_csv:
            try:
                is_default = int(row.get("is_default", "0") or 0)
                pokemon_id = int(row.get("id", "0") or 0)
                species_id = int(row.get("species_id", "0") or 0)
            except Exception:
                continue
            if is_default == 1 and pokemon_id > 0 and species_id > 0 and species_id <= MAX_RANDOM_POKEMON_ID:
                default_pokemon_id_by_species[species_id] = pokemon_id

        primary_type_by_pokemon_id: dict[int, str] = {}
        for row in pokemon_types_csv:
            try:
                pokemon_id = int(row.get("pokemon_id", "0") or 0)
                type_id = int(row.get("type_id", "0") or 0)
                slot = int(row.get("slot", "0") or 0)
            except Exception:
                continue
            if slot != 1:
                continue
            type_name = type_name_by_id.get(type_id)
            if pokemon_id > 0 and type_name:
                primary_type_by_pokemon_id[pokemon_id] = type_name

        for species_id, pokemon_id in default_pokemon_id_by_species.items():
            self._primary_type_by_id[species_id] = primary_type_by_pokemon_id.get(pokemon_id, "Unknown")

    @staticmethod
    def fetch_csv_rows(url: str) -> list[dict[str, str]]:
        resp = requests.get(url, timeout=12)
        resp.raise_for_status()
        text = resp.text if resp.text else ""
        if not text.strip():
            return []
        reader = csv.DictReader(io.StringIO(text))
        return [dict(row) for row in reader if isinstance(row, dict)]

    @staticmethod
    def display_name(raw_name: str) -> str:
        return raw_name.replace("-", " ").replace("_", " ").title()

    @staticmethod
    def is_rate_limited_response(resp: requests.Response) -> bool:
        if resp.status_code == 429:
            return True
        if resp.status_code == 403 and resp.headers.get("X-RateLimit-Remaining") == "0":
            return True
        return False
