from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

import psycopg

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class CaughtPokemonRecord:
    pokemon_id: int
    catches: int


class PokemonCatchHistoryStore:
    def __init__(self, dsn: Optional[str]):
        self.dsn = (dsn or "").strip()
        self.is_available = False
        self.error_text = ""
        if not self.dsn:
            self.error_text = "PACK_PG_DSN is not configured."
            logger.warning("Pokemon catch history disabled: %s", self.error_text)
            return
        try:
            self._init_schema()
            self.is_available = True
            logger.info("Pokemon catch history store is ready.")
        except Exception as exc:
            self.error_text = str(exc)
            logger.exception("Pokemon catch history initialization failed.")

    def _connect(self) -> psycopg.Connection:
        return psycopg.connect(self.dsn)

    def _init_schema(self) -> None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS pokemon_catches (
                        user_id TEXT NOT NULL,
                        pokemon_id INTEGER NOT NULL,
                        catches INTEGER NOT NULL DEFAULT 0,
                        first_caught_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        last_caught_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        PRIMARY KEY (user_id, pokemon_id)
                    );
                    """
                )
                cur.execute(
                    "CREATE INDEX IF NOT EXISTS idx_pokemon_catches_user_last ON pokemon_catches(user_id, last_caught_at DESC);"
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS catch_command_usage (
                        user_id TEXT NOT NULL,
                        day_start_date DATE NOT NULL,
                        uses INTEGER NOT NULL DEFAULT 0,
                        PRIMARY KEY (user_id, day_start_date)
                    );
                    """
                )
            conn.commit()

    def consume_catch_command_slot(self, *, user_id: int, day_start_utc: datetime, daily_limit: int) -> tuple[bool, int]:
        if not self.is_available:
            raise RuntimeError("Pokemon catch history DB is unavailable.")
        if daily_limit < 1:
            return False, 0
        day_date = day_start_utc.astimezone(timezone.utc).date()
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO catch_command_usage (user_id, day_start_date, uses)
                    VALUES (%s, %s, 1)
                    ON CONFLICT (user_id, day_start_date)
                    DO UPDATE
                    SET uses = catch_command_usage.uses + 1
                    WHERE catch_command_usage.uses < %s
                    RETURNING uses;
                    """,
                    (str(user_id), day_date, int(daily_limit)),
                )
                row = cur.fetchone()
                if row:
                    conn.commit()
                    return True, int(row[0] or 0)

                cur.execute(
                    """
                    SELECT uses
                    FROM catch_command_usage
                    WHERE user_id = %s AND day_start_date = %s;
                    """,
                    (str(user_id), day_date),
                )
                current = cur.fetchone()
            conn.commit()
        return False, int((current or [daily_limit])[0] or daily_limit)

    def save_catches(self, *, user_id: int, pokemon_ids: list[int]) -> None:
        if not self.is_available:
            raise RuntimeError("Pokemon catch history DB is unavailable.")
        if not pokemon_ids:
            return

        counts_by_id: dict[int, int] = {}
        for pokemon_id in pokemon_ids:
            if pokemon_id <= 0:
                continue
            counts_by_id[pokemon_id] = counts_by_id.get(pokemon_id, 0) + 1

        if not counts_by_id:
            return

        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.executemany(
                    """
                    INSERT INTO pokemon_catches (user_id, pokemon_id, catches, first_caught_at, last_caught_at)
                    VALUES (%s, %s, %s, NOW(), NOW())
                    ON CONFLICT (user_id, pokemon_id)
                    DO UPDATE
                    SET catches = pokemon_catches.catches + EXCLUDED.catches,
                        last_caught_at = NOW();
                    """,
                    [(str(user_id), pokemon_id, catches) for pokemon_id, catches in counts_by_id.items()],
                )
            conn.commit()

    def get_user_collection(self, *, user_id: int, limit: int = 30) -> list[CaughtPokemonRecord]:
        if not self.is_available:
            raise RuntimeError("Pokemon catch history DB is unavailable.")
        capped_limit = max(1, min(limit, 120))
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT pokemon_id, catches
                    FROM pokemon_catches
                    WHERE user_id = %s
                    ORDER BY last_caught_at DESC, pokemon_id ASC
                    LIMIT %s;
                    """,
                    (str(user_id), capped_limit),
                )
                rows = cur.fetchall()
        return [CaughtPokemonRecord(pokemon_id=int(pid), catches=int(catches)) for pid, catches in rows]

    def list_user_collection(self, *, user_id: int, max_species: int = 5000) -> list[CaughtPokemonRecord]:
        if not self.is_available:
            raise RuntimeError("Pokemon catch history DB is unavailable.")
        capped_limit = max(1, min(max_species, 10000))
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT pokemon_id, catches
                    FROM pokemon_catches
                    WHERE user_id = %s
                    ORDER BY last_caught_at DESC, pokemon_id ASC
                    LIMIT %s;
                    """,
                    (str(user_id), capped_limit),
                )
                rows = cur.fetchall()
        return [CaughtPokemonRecord(pokemon_id=int(pid), catches=int(catches)) for pid, catches in rows]

    def get_user_collection_totals(self, *, user_id: int) -> tuple[int, int]:
        if not self.is_available:
            raise RuntimeError("Pokemon catch history DB is unavailable.")
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT COUNT(*)::INT AS species_count, COALESCE(SUM(catches), 0)::INT AS total_catches
                    FROM pokemon_catches
                    WHERE user_id = %s;
                    """,
                    (str(user_id),),
                )
                row = cur.fetchone()
        if not row:
            return 0, 0
        return int(row[0] or 0), int(row[1] or 0)
