from __future__ import annotations

from typing import Optional

import psycopg


class ThreadStore:
    def __init__(self, dsn: Optional[str], ttl_seconds: int):
        self.dsn = (dsn or "").strip()
        self.ttl_seconds = ttl_seconds
        if not self.dsn:
            raise RuntimeError("THREAD_PG_DSN (or PACK_PG_DSN) is required for thread state storage.")
        self._init_schema()

    def _connect(self) -> psycopg.Connection:
        return psycopg.connect(self.dsn)

    def _init_schema(self) -> None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT pg_advisory_lock(hashtext('thread_state_schema_init'));")
                try:
                    cur.execute(
                        """
                        CREATE TABLE IF NOT EXISTS thread_state (
                            user_id TEXT NOT NULL,
                            channel_id TEXT NOT NULL,
                            thread_id TEXT NOT NULL,
                            last_activity_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                            PRIMARY KEY (user_id, channel_id)
                        );
                        """
                    )
                    cur.execute(
                        "CREATE INDEX IF NOT EXISTS idx_thread_state_activity ON thread_state(last_activity_at);"
                    )
                finally:
                    cur.execute("SELECT pg_advisory_unlock(hashtext('thread_state_schema_init'));")
            conn.commit()

    def get_valid_thread_id(self, user_id: int, channel_id: int) -> Optional[str]:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT thread_id
                    FROM thread_state
                    WHERE user_id = %s
                      AND channel_id = %s
                      AND last_activity_at >= (NOW() - (%s * INTERVAL '1 second'));
                    """,
                    (str(user_id), str(channel_id), self.ttl_seconds),
                )
                row = cur.fetchone()
                if row:
                    return str(row[0])
                cur.execute(
                    "DELETE FROM thread_state WHERE user_id = %s AND channel_id = %s;",
                    (str(user_id), str(channel_id)),
                )
            conn.commit()
        return None

    def upsert(self, user_id: int, channel_id: int, thread_id: str) -> None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO thread_state (user_id, channel_id, thread_id, last_activity_at)
                    VALUES (%s, %s, %s, NOW())
                    ON CONFLICT (user_id, channel_id)
                    DO UPDATE
                    SET thread_id = EXCLUDED.thread_id,
                        last_activity_at = NOW();
                    """,
                    (str(user_id), str(channel_id), thread_id),
                )
            conn.commit()

    def touch(self, user_id: int, channel_id: int) -> None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE thread_state
                    SET last_activity_at = NOW()
                    WHERE user_id = %s AND channel_id = %s;
                    """,
                    (str(user_id), str(channel_id)),
                )
            conn.commit()

    def clear(self, user_id: int, channel_id: int) -> None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM thread_state WHERE user_id = %s AND channel_id = %s;",
                    (str(user_id), str(channel_id)),
                )
            conn.commit()
