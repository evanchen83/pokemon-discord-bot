from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional

OPEN_PACK_DAILY_LIMIT = 5
MY_CARDS_DEFAULT_SET_LIMIT = 20
CATCH_DAILY_LIMIT = 25


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _env_bool(name: str, default: bool) -> bool:
    raw = (os.getenv(name) or "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "y", "on"}


def _env_optional_int(name: str) -> Optional[int]:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return None
    try:
        value = int(raw)
    except ValueError:
        raise RuntimeError(f"{name} must be an integer")
    if value < 1:
        raise RuntimeError(f"{name} must be >= 1")
    return value


def _env_int_list(name: str) -> Optional[list[int]]:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return None
    values: list[int] = []
    for part in raw.split(","):
        p = part.strip()
        if not p:
            continue
        try:
            values.append(int(p))
        except ValueError:
            raise RuntimeError(f"{name} must be a comma-separated integer list")
    if not values:
        return None
    return values


@dataclass
class Settings:
    discord_bot_token: str
    discord_guild_id: Optional[int]
    discord_sync_commands: bool
    wxo_agent_name: str
    wxo_agent_id: Optional[str]
    wxo_base_url: Optional[str]
    wxo_api_key: Optional[str]
    wxo_local_username: str
    wxo_local_password: str
    wxo_tenant_id: Optional[str]
    wxo_tenant_name: str
    thread_ttl_seconds: int
    pack_pg_dsn: Optional[str]
    thread_pg_dsn: Optional[str]
    discord_shard_count: Optional[int]
    discord_shard_ids: Optional[list[int]]
    discord_sync_leader: bool
    metrics_enabled: bool
    metrics_port: int
    catch_daily_limit: int

    @staticmethod
    def from_env() -> "Settings":
        token = os.getenv("DISCORD_BOT_TOKEN", "").strip()
        if not token:
            raise RuntimeError("Missing DISCORD_BOT_TOKEN")

        guild_id_raw = (os.getenv("DISCORD_GUILD_ID") or "").strip()
        guild_id: Optional[int] = None
        if guild_id_raw:
            try:
                guild_id = int(guild_id_raw)
            except ValueError:
                raise RuntimeError("DISCORD_GUILD_ID must be an integer")
        sync_commands = _env_bool("DISCORD_SYNC_COMMANDS", False)

        agent_name = os.getenv("WO_AGENT_NAME", "pokemon_tcg_agent").strip() or "pokemon_tcg_agent"
        agent_id = (os.getenv("WO_AGENT_ID") or "").strip() or None
        wxo_base_url = (os.getenv("WO_RUNTIME_INSTANCE") or os.getenv("WO_INSTANCE") or "").strip() or None
        wxo_api_key = (os.getenv("WO_RUNTIME_API_KEY") or os.getenv("WO_API_KEY") or "").strip() or None
        ttl_minutes = _env_int("THREAD_TTL_MINUTES", 10)
        pack_pg_dsn = (os.getenv("PACK_PG_DSN") or os.getenv("DATABASE_URL") or "").strip() or None
        thread_pg_dsn = (os.getenv("THREAD_PG_DSN") or "").strip() or pack_pg_dsn
        shard_count = _env_optional_int("DISCORD_SHARD_COUNT")
        shard_ids = _env_int_list("DISCORD_SHARD_IDS")
        if shard_count is not None and shard_ids:
            out_of_range = [sid for sid in shard_ids if sid < 0 or sid >= shard_count]
            if out_of_range:
                raise RuntimeError(
                    f"DISCORD_SHARD_IDS contains IDs outside [0, {shard_count - 1}]: {out_of_range}"
                )
        default_sync_leader = True
        if shard_ids:
            default_sync_leader = 0 in shard_ids
        sync_leader = _env_bool("DISCORD_SYNC_LEADER", default_sync_leader)
        metrics_enabled = _env_bool("METRICS_ENABLED", True)
        metrics_port = _env_int("METRICS_PORT", 9108)
        catch_daily_limit = max(1, _env_int("CATCH_DAILY_LIMIT", CATCH_DAILY_LIMIT))

        return Settings(
            discord_bot_token=token,
            discord_guild_id=guild_id,
            discord_sync_commands=sync_commands,
            wxo_agent_name=agent_name,
            wxo_agent_id=agent_id,
            wxo_base_url=wxo_base_url,
            wxo_api_key=wxo_api_key,
            wxo_local_username=(os.getenv("WO_LOCAL_USERNAME") or "wxo.archer@ibm.com").strip(),
            wxo_local_password=(os.getenv("WO_LOCAL_PASSWORD") or "watsonx").strip(),
            wxo_tenant_id=(os.getenv("WO_TENANT_ID") or "").strip() or None,
            wxo_tenant_name=(os.getenv("WO_TENANT_NAME") or "wxo-dev").strip(),
            thread_ttl_seconds=max(60, ttl_minutes * 60),
            pack_pg_dsn=pack_pg_dsn,
            thread_pg_dsn=thread_pg_dsn,
            discord_shard_count=shard_count,
            discord_shard_ids=shard_ids,
            discord_sync_leader=sync_leader,
            metrics_enabled=metrics_enabled,
            metrics_port=metrics_port,
            catch_daily_limit=catch_daily_limit,
        )
