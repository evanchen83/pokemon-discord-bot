from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

import psycopg

from features.pack_opening import SetCard, SetMeta

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class CollectionCardRecord:
    set_name: str
    card_name: str
    rarity: str
    card_number: str
    copies: int


class PackHistoryStore:
    def __init__(self, dsn: Optional[str]):
        self.dsn = (dsn or "").strip()
        self.is_available = False
        self.error_text = ""
        if not self.dsn:
            self.error_text = "PACK_PG_DSN is not configured."
            logger.warning("Pack history disabled: %s", self.error_text)
            return
        try:
            self._init_schema()
            self.is_available = True
            logger.info("Pack history store is ready.")
        except Exception as exc:
            self.error_text = str(exc)
            logger.exception("Pack history store initialization failed.")

    def _connect(self) -> psycopg.Connection:
        return psycopg.connect(self.dsn)

    def _init_schema(self) -> None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS pack_openings (
                        pack_id UUID PRIMARY KEY,
                        user_id TEXT NOT NULL,
                        channel_id TEXT NOT NULL,
                        set_id TEXT NOT NULL,
                        set_name TEXT NOT NULL,
                        opened_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    );
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS pack_cards (
                        id BIGSERIAL PRIMARY KEY,
                        pack_id UUID NOT NULL REFERENCES pack_openings(pack_id) ON DELETE CASCADE,
                        card_id TEXT NOT NULL,
                        card_name TEXT NOT NULL,
                        rarity TEXT NOT NULL,
                        card_number TEXT,
                        image_url TEXT
                    );
                    """
                )
                cur.execute(
                    "CREATE INDEX IF NOT EXISTS idx_pack_openings_user_opened ON pack_openings(user_id, opened_at DESC);"
                )
                cur.execute("CREATE INDEX IF NOT EXISTS idx_pack_cards_pack ON pack_cards(pack_id);")
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS open_pack_command_usage (
                        user_id TEXT NOT NULL,
                        day_start_date DATE NOT NULL,
                        uses INTEGER NOT NULL DEFAULT 0,
                        PRIMARY KEY (user_id, day_start_date)
                    );
                    """
                )
            conn.commit()

    def consume_open_pack_command_slot(
        self,
        *,
        user_id: int,
        day_start_utc: datetime,
        daily_limit: int,
    ) -> tuple[bool, int]:
        if not self.is_available:
            raise RuntimeError("Pack history DB is unavailable.")
        if daily_limit < 1:
            return False, 0
        day_date = day_start_utc.astimezone(timezone.utc).date()
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO open_pack_command_usage (user_id, day_start_date, uses)
                    VALUES (%s, %s, 1)
                    ON CONFLICT (user_id, day_start_date)
                    DO UPDATE
                    SET uses = open_pack_command_usage.uses + 1
                    WHERE open_pack_command_usage.uses < %s
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
                    FROM open_pack_command_usage
                    WHERE user_id = %s AND day_start_date = %s;
                    """,
                    (str(user_id), day_date),
                )
                current = cur.fetchone()
            conn.commit()
        return False, int((current or [daily_limit])[0] or daily_limit)

    def save_pack_opening(
        self,
        *,
        user_id: int,
        channel_id: int,
        set_meta: SetMeta,
        cards: list[SetCard],
    ) -> str:
        if not self.is_available:
            raise RuntimeError("Pack history DB is unavailable.")
        pack_id = str(uuid.uuid4())
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO pack_openings (pack_id, user_id, channel_id, set_id, set_name)
                    VALUES (%s, %s, %s, %s, %s);
                    """,
                    (pack_id, str(user_id), str(channel_id), set_meta.set_id, set_meta.name),
                )
                cur.executemany(
                    """
                    INSERT INTO pack_cards (pack_id, card_id, card_name, rarity, card_number, image_url)
                    VALUES (%s, %s, %s, %s, %s, %s);
                    """,
                    [
                        (
                            pack_id,
                            card.card_id,
                            card.name,
                            card.rarity,
                            card.number or None,
                            card.image_url or None,
                        )
                        for card in cards
                    ],
                )
            conn.commit()
        return pack_id

    def get_collection_grouped_by_set(self, *, user_id: int, max_sets: int = 20) -> list[CollectionCardRecord]:
        if not self.is_available:
            raise RuntimeError("Pack history DB is unavailable.")
        capped_sets = max(1, min(max_sets, 50))
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    WITH ranked_sets AS (
                        SELECT
                            set_name,
                            MAX(opened_at) AS last_opened
                        FROM pack_openings
                        WHERE user_id = %s
                        GROUP BY set_name
                        ORDER BY MAX(opened_at) DESC
                        LIMIT %s
                    )
                    SELECT
                        po.set_name,
                        pc.card_name,
                        pc.rarity,
                        COALESCE(pc.card_number, '') AS card_number,
                        COUNT(*)::INT AS copies,
                        rs.last_opened
                    FROM pack_cards pc
                    JOIN pack_openings po ON po.pack_id = pc.pack_id
                    JOIN ranked_sets rs ON rs.set_name = po.set_name
                    WHERE po.user_id = %s
                    GROUP BY po.set_name, pc.card_name, pc.rarity, COALESCE(pc.card_number, ''), rs.last_opened
                    ORDER BY rs.last_opened DESC, po.set_name ASC, copies DESC, pc.card_name ASC;
                    """,
                    (str(user_id), capped_sets, str(user_id)),
                )
                rows = cur.fetchall()
        out: list[CollectionCardRecord] = []
        for set_name, card_name, rarity, card_number, copies, _last_opened in rows:
            out.append(
                CollectionCardRecord(
                    set_name=set_name,
                    card_name=card_name,
                    rarity=rarity,
                    card_number=card_number or "",
                    copies=int(copies),
                )
            )
        return out

    def count_pack_openings_since(self, *, user_id: int, since: datetime) -> int:
        if not self.is_available:
            raise RuntimeError("Pack history DB is unavailable.")
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT COUNT(*)::INT
                    FROM pack_openings
                    WHERE user_id = %s AND opened_at >= %s;
                    """,
                    (str(user_id), since),
                )
                row = cur.fetchone()
        if not row:
            return 0
        return int(row[0] or 0)
