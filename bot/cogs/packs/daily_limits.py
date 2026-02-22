from __future__ import annotations

from datetime import datetime


class OpenPackFallbackLimiter:
    def __init__(self) -> None:
        self._open_pack_daily_usage_fallback: dict[int, tuple[int, int]] = {}

    async def consume_slot(self, *, user_id: int, day_start_utc: datetime, daily_limit: int) -> tuple[bool, int]:
        if daily_limit < 1:
            return False, 0
        day_key = int(day_start_utc.timestamp() // 86400)
        fallback = self._open_pack_daily_usage_fallback.get(user_id)
        if not fallback or fallback[0] != day_key:
            self._open_pack_daily_usage_fallback[user_id] = (day_key, 1)
            return True, 1
        current_uses = fallback[1]
        if current_uses >= daily_limit:
            return False, current_uses
        next_uses = current_uses + 1
        self._open_pack_daily_usage_fallback[user_id] = (day_key, next_uses)
        return True, next_uses
