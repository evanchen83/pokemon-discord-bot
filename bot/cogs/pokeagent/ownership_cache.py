from __future__ import annotations

import time


class ResponseOwnershipCache:
    def __init__(self) -> None:
        self._response_owner_by_message_id: dict[int, tuple[int, int, int]] = {}

    def remember(self, message_id: int, owner_user_id: int, channel_id: int) -> None:
        now = int(time.time())
        self._response_owner_by_message_id[int(message_id)] = (int(owner_user_id), int(channel_id), now)
        if len(self._response_owner_by_message_id) <= 2000:
            return
        cutoff = now - (6 * 60 * 60)
        stale = [mid for mid, (_, _, ts) in self._response_owner_by_message_id.items() if ts < cutoff]
        for mid in stale:
            self._response_owner_by_message_id.pop(mid, None)
        while len(self._response_owner_by_message_id) > 1800:
            oldest = min(self._response_owner_by_message_id.items(), key=lambda x: x[1][2])[0]
            self._response_owner_by_message_id.pop(oldest, None)

    def get(self, message_id: int) -> tuple[int, int, int] | None:
        return self._response_owner_by_message_id.get(int(message_id))
