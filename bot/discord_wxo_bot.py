from __future__ import annotations

import asyncio
import logging
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlparse
import re

import discord
import psycopg
import requests
from discord import app_commands
from discord.ext import commands
from dotenv import load_dotenv

from ibm_watsonx_orchestrate.client.agents.agent_client import AgentClient
from ibm_watsonx_orchestrate.client.chat.run_client import RunClient
from ibm_watsonx_orchestrate.client.threads.threads_client import ThreadsClient
from ibm_watsonx_orchestrate.client.utils import instantiate_client, is_local_dev
from features.metrics import BotMetrics
from features.embed_standards import EMBED_PAGE_CHAR_LIMIT
from features.pokemon_catch_history import PokemonCatchHistoryStore
from features.pack_history import PackHistoryStore
from features.pack_opening import PackService


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("discord_wxo_bot")

OPEN_PACK_DAILY_LIMIT = 5
MY_CARDS_DEFAULT_SET_LIMIT = 20
CATCH_DAILY_LIMIT = 25


def _is_local_wxo_url(base_url: Optional[str]) -> bool:
    if not base_url:
        return False
    if is_local_dev(base_url):
        return True
    try:
        host = (urlparse(base_url).hostname or "").lower()
    except Exception:
        return False
    return host in {"localhost", "127.0.0.1", "::1", "host.docker.internal"}


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
                # Avoid DDL races when multiple shard containers initialize simultaneously.
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


class WXOChatClient:
    def __init__(
        self,
        agent_name: str,
        wxo_agent_id: Optional[str] = None,
        wxo_base_url: Optional[str] = None,
        wxo_api_key: Optional[str] = None,
        wxo_local_username: str = "wxo.archer@ibm.com",
        wxo_local_password: str = "watsonx",
        wxo_tenant_id: Optional[str] = None,
        wxo_tenant_name: str = "wxo-dev",
    ):
        self.agent_name = agent_name
        self.wxo_base_url = (wxo_base_url or "").rstrip("/")
        self.wxo_api_key = wxo_api_key
        self.wxo_local_username = wxo_local_username
        self.wxo_local_password = wxo_local_password
        self.wxo_tenant_id = wxo_tenant_id
        self.wxo_tenant_name = wxo_tenant_name
        self.local_token: Optional[str] = None
        self.agent_llm: Optional[str] = None
        self.local_mode = _is_local_wxo_url(self.wxo_base_url)
        self.cloud_iam_mode = bool(self.wxo_base_url and self.wxo_api_key and not self.local_mode)
        self.api_prefix = "/api/v1" if self.local_mode else "/v1"

        # In cloud/local explicit URL mode we call raw HTTP endpoints directly.
        # For local mode, auth is JWT via /auth/token.
        if self.cloud_iam_mode or self.local_mode:
            self.agent_client = None
            self.run_client = None
            self.threads_client = None
        else:
            self.agent_client = self._make_client(AgentClient, wxo_base_url, wxo_api_key, self.local_mode)
            self.run_client = self._make_client(RunClient, wxo_base_url, wxo_api_key, self.local_mode)
            self.threads_client = self._make_client(ThreadsClient, wxo_base_url, wxo_api_key, self.local_mode)
        self.agent_id = wxo_agent_id or self._resolve_agent_id(agent_name)

    @staticmethod
    def _make_client(client_cls: type, wxo_base_url: Optional[str], wxo_api_key: Optional[str], local_mode: bool):
        # If explicit env credentials are provided (recommended for Docker), use those.
        if wxo_base_url and wxo_api_key and local_mode:
            return client_cls(base_url=wxo_base_url, api_key=wxo_api_key, is_local=True)

        # Fallback to local ADK active-env auth files (works outside container).
        return instantiate_client(client_cls)

    def _build_url(self, path: str) -> str:
        if not self.wxo_base_url:
            raise RuntimeError("Missing WXO base URL")
        if not path.startswith("/"):
            path = f"/{path}"
        return f"{self.wxo_base_url}{path}"

    def _local_login(self) -> None:
        token_url = self._build_url(f"{self.api_prefix}/auth/token")
        form = {"username": self.wxo_local_username, "password": self.wxo_local_password}
        first = requests.post(token_url, data=form, timeout=60)
        first.raise_for_status()
        first_token = first.json().get("access_token")
        if not first_token:
            raise RuntimeError("Local WXO auth token missing access_token")

        headers = {"Authorization": f"Bearer {first_token}", "Accept": "application/json"}
        tenant_id = self.wxo_tenant_id
        if not tenant_id:
            tenants_resp = requests.get(self._build_url(f"{self.api_prefix}/tenants"), headers=headers, timeout=60)
            tenants_resp.raise_for_status()
            tenants = tenants_resp.json() if tenants_resp.text else []
            if not isinstance(tenants, list) or not tenants:
                raise RuntimeError("No tenants returned by local WXO")

            target = None
            for tenant in tenants:
                if isinstance(tenant, dict) and tenant.get("name") == self.wxo_tenant_name:
                    target = tenant
                    break
            if target is None:
                target = tenants[0] if isinstance(tenants[0], dict) else None
            tenant_id = str((target or {}).get("id", "")).strip()
            if not tenant_id:
                raise RuntimeError("Could not resolve tenant id for local WXO")
            self.wxo_tenant_id = tenant_id

        scoped = requests.post(f"{token_url}?tenant_id={tenant_id}", data=form, timeout=60)
        scoped.raise_for_status()
        scoped_token = scoped.json().get("access_token")
        if not scoped_token:
            raise RuntimeError("Scoped local WXO token missing access_token")
        self.local_token = str(scoped_token)

    def _http_headers(self) -> dict[str, str]:
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        if self.local_mode:
            if not self.local_token:
                self._local_login()
            headers["Authorization"] = f"Bearer {self.local_token}"
        elif self.cloud_iam_mode:
            if not self.wxo_api_key:
                raise RuntimeError("Missing WXO API key for cloud IAM mode")
            headers["IAM-API_KEY"] = self.wxo_api_key
        return headers

    def _http_get(self, path: str, params: Optional[dict[str, Any]] = None) -> Any:
        if not self.wxo_base_url:
            raise RuntimeError("Missing WXO base URL")
        url = self._build_url(path)
        headers = self._http_headers()
        response = requests.get(
            url,
            headers=headers,
            params=params,
            timeout=60,
        )
        if self.local_mode and response.status_code in {401, 403}:
            self.local_token = None
            headers = self._http_headers()
            response = requests.get(url, headers=headers, params=params, timeout=60)
        response.raise_for_status()
        return response.json() if response.text else {}

    def _http_post(self, path: str, payload: dict[str, Any]) -> Any:
        if not self.wxo_base_url:
            raise RuntimeError("Missing WXO base URL")
        url = self._build_url(path)
        headers = self._http_headers()
        response = requests.post(
            url,
            headers=headers,
            json=payload,
            timeout=120,
        )
        if self.local_mode and response.status_code in {401, 403}:
            self.local_token = None
            headers = self._http_headers()
            response = requests.post(url, headers=headers, json=payload, timeout=120)
        response.raise_for_status()
        return response.json() if response.text else {}

    def _orchestrate_path(self, suffix: str) -> str:
        if not suffix.startswith("/"):
            suffix = f"/{suffix}"
        return f"{self.api_prefix}/orchestrate{suffix}"

    def _resolve_agent_id(self, name: str) -> str:
        if self.cloud_iam_mode or self.local_mode:
            agents = self._http_get(self._orchestrate_path("/agents"), params={"names": name, "include_hidden": "true"})
        else:
            agents = self.agent_client.get_draft_by_name(name)
        if not agents:
            if self.cloud_iam_mode or self.local_mode:
                available = self._http_get(self._orchestrate_path("/agents"), params={"include_hidden": "true"})
                names = []
                if isinstance(available, list):
                    names = [str(a.get("name", "")) for a in available if isinstance(a, dict) and a.get("name")]
                sample = ", ".join(names[:12]) if names else "(none)"
                raise RuntimeError(
                    f"WXO agent '{name}' not found in configured cloud instance. "
                    f"Set WO_AGENT_NAME to an existing name, or set WO_AGENT_ID directly. "
                    f"Sample available agents: {sample}"
                )
            raise RuntimeError(f"WXO agent '{name}' not found")

        agent_id = agents[0].get("id")
        if not agent_id:
            raise RuntimeError(f"WXO agent '{name}' has no id")
        self.agent_llm = str(agents[0].get("llm") or "").strip() or None
        return str(agent_id)

    def _extract_assistant_text(self, thread_id: str, fallback_message_id: Optional[str] = None) -> str:
        if self.local_mode:
            data = self._http_get(f"{self.api_prefix}/threads/{thread_id}/messages")
        elif self.cloud_iam_mode:
            data = self._http_get(self._orchestrate_path(f"/threads/{thread_id}/messages"))
        else:
            data = self.threads_client.get_thread_messages(thread_id)
        messages: list[dict[str, Any]] = []

        if isinstance(data, list):
            messages = [m for m in data if isinstance(m, dict)]
        elif isinstance(data, dict):
            raw = data.get("data", data.get("messages", []))
            if isinstance(raw, list):
                messages = [m for m in raw if isinstance(m, dict)]

        if fallback_message_id:
            for msg in reversed(messages):
                if str(msg.get("id")) == str(fallback_message_id) and msg.get("role") == "assistant":
                    text = self._message_to_text(msg)
                    if text:
                        return text

        for msg in reversed(messages):
            if msg.get("role") == "assistant":
                text = self._message_to_text(msg)
                if text:
                    return text

        return "I couldn't read an assistant response from WXO for this run."

    @staticmethod
    def _message_to_text(msg: dict[str, Any]) -> str:
        content = msg.get("content")
        if isinstance(content, str):
            return content.strip()

        if isinstance(content, list):
            text_parts: list[str] = []
            for item in content:
                if isinstance(item, str):
                    text_parts.append(item)
                elif isinstance(item, dict):
                    if isinstance(item.get("text"), str):
                        text_parts.append(item["text"])
                    elif item.get("response_type") == "text" and isinstance(item.get("text"), str):
                        text_parts.append(item["text"])
            return "\n".join([p.strip() for p in text_parts if p and p.strip()]).strip()

        return ""

    def ask(self, prompt: str, thread_id: Optional[str]) -> tuple[str, str]:
        if self.cloud_iam_mode or self.local_mode:
            payload: dict[str, Any] = {
                "message": {"role": "user", "content": prompt},
                "agent_id": self.agent_id,
                "capture_logs": False,
            }
            if thread_id:
                payload["thread_id"] = thread_id
            run = self._http_post(self._orchestrate_path("/runs"), payload)
        else:
            run = self.run_client.create_run(
                message=prompt,
                agent_id=self.agent_id,
                thread_id=thread_id,
                capture_logs=False,
            )

        run_id = str(run.get("run_id", ""))
        next_thread_id = str(run.get("thread_id", thread_id or ""))
        if not run_id:
            raise RuntimeError("WXO run did not return run_id")
        if not next_thread_id:
            raise RuntimeError("WXO run did not return thread_id")

        if self.cloud_iam_mode or self.local_mode:
            status: dict[str, Any] = {}
            for _ in range(90):
                status = self._http_get(self._orchestrate_path(f"/runs/{run_id}"))
                run_status = str(status.get("status", "")).lower()
                if run_status in {"completed", "failed", "cancelled"}:
                    break
                time.sleep(2)
        else:
            status = self.run_client.wait_for_run_completion(run_id=run_id, poll_interval=2, max_retries=90)
        run_status = str(status.get("status", "")).lower()
        if run_status != "completed":
            err = status.get("error") or f"Run ended with status={run_status}"
            raise RuntimeError(str(err))

        assistant_text = self._extract_assistant_text(
            thread_id=next_thread_id,
            fallback_message_id=status.get("message_id"),
        )
        return next_thread_id, assistant_text


class PokemonBot(commands.AutoShardedBot):
    def __init__(
        self,
        settings: Settings,
        thread_store: ThreadStore,
        wxo: Optional[WXOChatClient],
        pack_history: PackHistoryStore,
        pokemon_catch_history: PokemonCatchHistoryStore,
    ):
        intents = discord.Intents.default()
        intents.message_content = True
        super().__init__(
            command_prefix=commands.when_mentioned_or("$"),
            intents=intents,
            shard_count=settings.discord_shard_count,
            shard_ids=settings.discord_shard_ids,
        )
        self.settings = settings
        self.thread_store = thread_store
        self.wxo = wxo
        self.pack_history = pack_history
        self.pokemon_catch_history = pokemon_catch_history
        self._commands_synced = False
        self.embed_color = discord.Color.from_rgb(17, 105, 188)
        data_csv_dir = Path(__file__).resolve().parent.parent / "data" / "pokemontcg" / "csv"
        self.pack_service = PackService(data_csv_dir)
        self.wxo_unavailable_text = "Orchestrate services are currently unavailable. `/pokeagent` is temporarily disabled."
        self.pack_history_unavailable_text = "Your card collection is currently unavailable."
        self.pokemon_catch_history_unavailable_text = "Your Pokemon collection is currently unavailable."
        self.open_pack_daily_limit = OPEN_PACK_DAILY_LIMIT
        self.my_cards_default_set_limit = MY_CARDS_DEFAULT_SET_LIMIT
        self.catch_daily_limit = settings.catch_daily_limit
        self.metrics = BotMetrics(settings.metrics_enabled, settings.metrics_port)
        if settings.metrics_enabled:
            logger.info("Metrics enabled on port %s", settings.metrics_port)
        self._wxo_reconnect_lock = asyncio.Lock()
        self._wxo_last_init_attempt_epoch = 0.0
        self._wxo_init_retry_cooldown_seconds = 20.0

    def record_command_metric(self, *, command: str, outcome: str, started_at: float) -> None:
        elapsed = time.perf_counter() - started_at
        self.metrics.record_command(command=command, outcome=outcome, duration_seconds=elapsed)

    async def ensure_wxo_available(self, *, force: bool = False) -> bool:
        if self.wxo is not None:
            return True

        now = time.time()
        if not force and (now - self._wxo_last_init_attempt_epoch) < self._wxo_init_retry_cooldown_seconds:
            return False

        async with self._wxo_reconnect_lock:
            if self.wxo is not None:
                return True
            now = time.time()
            if not force and (now - self._wxo_last_init_attempt_epoch) < self._wxo_init_retry_cooldown_seconds:
                return False
            self._wxo_last_init_attempt_epoch = now

            try:
                self.wxo = await asyncio.to_thread(
                    WXOChatClient,
                    self.settings.wxo_agent_name,
                    self.settings.wxo_agent_id,
                    self.settings.wxo_base_url,
                    self.settings.wxo_api_key,
                    self.settings.wxo_local_username,
                    self.settings.wxo_local_password,
                    self.settings.wxo_tenant_id,
                    self.settings.wxo_tenant_name,
                )
                logger.info("WXO client became available after runtime retry.")
                return True
            except Exception:
                logger.exception("Runtime WXO initialization attempt failed.")
                self.wxo = None
                return False

    async def setup_hook(self) -> None:
        from cogs.admin_cog import AdminCog
        from cogs.packs_cog import PacksCog
        from cogs.pcbox_cog import PcBoxCog
        from cogs.pokeagent_cog import PokeAgentCog

        await self.add_cog(AdminCog(self))
        await self.add_cog(PokeAgentCog(self))
        await self.add_cog(PacksCog(self))
        await self.add_cog(PcBoxCog(self))

    async def sync_app_commands(
        self,
        *,
        guild: Optional[discord.abc.Snowflake] = None,
        copy_global_to_guild: bool = False,
        clear_guild: bool = False,
    ) -> list[app_commands.AppCommand]:
        if guild is not None:
            if clear_guild:
                self.tree.clear_commands(guild=guild)
            if copy_global_to_guild:
                # Dev helper: mirror current global commands into a specific guild for fast iteration.
                self.tree.copy_global_to(guild=guild)
            return await self.tree.sync(guild=guild)
        return await self.tree.sync()

    async def on_ready(self) -> None:
        logger.info(
            "Discord bot connected as %s (shard_ids=%s, shard_count=%s)",
            self.user,
            self.shard_ids,
            self.shard_count,
        )
        if self._commands_synced:
            return
        if not self.settings.discord_sync_commands:
            logger.info("Skipping startup slash-command sync (DISCORD_SYNC_COMMANDS=false). Use the owner sync command.")
            return
        if not self.settings.discord_sync_leader:
            logger.info("Skipping startup slash-command sync on non-leader shard container.")
            return

        try:
            if self.settings.discord_guild_id:
                guild = discord.Object(id=self.settings.discord_guild_id)
                synced = await self.sync_app_commands(guild=guild, copy_global_to_guild=True)
                logger.info(
                    "Synced guild slash commands for %s: %s",
                    self.settings.discord_guild_id,
                    [cmd.name for cmd in synced],
                )
            else:
                synced = await self.sync_app_commands()
                logger.info("Synced global slash commands: %s", [cmd.name for cmd in synced])
            self._commands_synced = True
        except Exception:
            logger.exception("Failed to sync slash commands")

    def format_agent_response_for_discord(self, text: str) -> str:
        return _format_agent_response_for_discord(text)

    def build_response_embeds(
        self,
        *,
        title: str,
        text: str,
        color: discord.Color,
        llm_model: Optional[str],
        question: Optional[str] = None,
    ) -> list[discord.Embed]:
        return _build_response_embeds(
            title=title,
            text=text,
            color=color,
            llm_model=llm_model,
            question=question,
        )

    def make_embed_pager(self, *, embeds: list[discord.Embed], owner_user_id: int) -> "_EmbedPagerView":
        return _EmbedPagerView(embeds=embeds, owner_user_id=owner_user_id)

    def rarity_rank(self, rarity: str) -> int:
        return _rarity_rank(rarity)

    def paginate_set_blocks(self, blocks: list[str], limit: int = 1700) -> list[str]:
        return _paginate_set_blocks(blocks, limit=limit)

    async def _run_agent_query(
        self,
        user_id: int,
        channel_id: int,
        question: str,
        force_new_thread: bool = False,
    ) -> str:
        if not await self.ensure_wxo_available():
            raise RuntimeError("Orchestrate services are currently unavailable.")
        if self.wxo is None:
            raise RuntimeError("Orchestrate services are currently unavailable.")
        prior_thread_id = None if force_new_thread else self.thread_store.get_valid_thread_id(user_id, channel_id)
        effective_question = _rewrite_question_for_known_tool_gaps(question)
        next_thread_id, response = await asyncio.to_thread(self.wxo.ask, effective_question, prior_thread_id)
        self.thread_store.upsert(user_id, channel_id, next_thread_id)
        return response


def _split_discord_message(text: str, limit: int = EMBED_PAGE_CHAR_LIMIT) -> list[str]:
    text = text.strip()
    if not text:
        return []
    if len(text) <= limit:
        return [text]

    # Prefer paragraph-aware splitting so table page blocks + code fences stay intact.
    paragraphs = [p for p in text.split("\n\n") if p.strip()]
    chunks: list[str] = []
    current = ""

    def hard_split(block: str) -> list[str]:
        out: list[str] = []
        start = 0
        while start < len(block):
            end = min(start + limit, len(block))
            if end < len(block):
                split_idx = block.rfind("\n", start, end)
                if split_idx <= start:
                    split_idx = block.rfind(" ", start, end)
                if split_idx > start:
                    end = split_idx
            part = block[start:end].strip()
            if part:
                out.append(part)
            start = end
        return out

    for para in paragraphs:
        candidate = para if not current else f"{current}\n\n{para}"
        if len(candidate) <= limit:
            current = candidate
            continue

        if current:
            chunks.append(current)
            current = ""

        if len(para) <= limit:
            current = para
        else:
            chunks.extend(hard_split(para))

    if current:
        chunks.append(current)
    return chunks


def _paginate_set_blocks(blocks: list[str], limit: int = EMBED_PAGE_CHAR_LIMIT) -> list[str]:
    if not blocks:
        return []

    pages: list[str] = []
    current = ""
    for block in blocks:
        block = (block or "").strip()
        if not block:
            continue
        candidate = block if not current else f"{current}\n\n{block}"
        if len(candidate) <= limit:
            current = candidate
            continue
        if current:
            pages.append(current)
            current = ""
        if len(block) <= limit:
            current = block
            continue
        # If a single set block is huge, split it safely.
        pages.extend(_split_discord_message(block, limit=limit))
    if current:
        pages.append(current)
    return pages or ["(No collection data)"]


def _rarity_rank(rarity: str) -> int:
    r = (rarity or "").strip().lower()
    if not r:
        return 0
    if "special illustration rare" in r:
        return 100
    if "hyper rare" in r:
        return 97
    if "secret rare" in r:
        return 95
    if "ultra rare" in r:
        return 93
    if "illustration rare" in r:
        return 91
    if "double rare" in r:
        return 90
    if "rare holo vstar" in r:
        return 89
    if "rare holo vmax" in r:
        return 88
    if "rare holo v" in r or "rare holo ex" in r:
        return 87
    if "rare holo" in r:
        return 82
    if "rare" in r:
        return 75
    if "uncommon" in r:
        return 45
    if "common" in r:
        return 25
    return 10


_TABLE_SEPARATOR_RE = re.compile(r"^\s*\|?\s*:?-{2,}:?\s*(\|\s*:?-{2,}:?\s*)+\|?\s*$")


def _is_markdown_table_header(line: str, next_line: str) -> bool:
    if "|" not in line:
        return False
    if _TABLE_SEPARATOR_RE.match(next_line):
        return True
    # Fallback: some model outputs produce looser separators (e.g., `---|---|---` without surrounding spacing).
    if "|" in next_line:
        tokens = [t.strip() for t in next_line.strip().strip("|").split("|")]
        if tokens and all(t and set(t) <= {"-", ":"} and t.count("-") >= 2 for t in tokens):
            return True
    return False


def _parse_markdown_table_row(line: str) -> list[str]:
    raw = line.strip()
    if raw.startswith("|"):
        raw = raw[1:]
    if raw.endswith("|"):
        raw = raw[:-1]
    return [cell.strip() for cell in raw.split("|")]


def _normalize_table_cell(value: str) -> str:
    v = (value or "").strip()
    v = v.replace("<br>", "\n").replace("<br/>", "\n").replace("<br />", "\n")
    # Strip markdown image syntax to just URL for better Discord/mobile readability in embeds.
    v = re.sub(r"!\[[^\]]*\]\((https?://[^)]+)\)", r"\1", v)
    return v


def _extract_image_url(value: str) -> str:
    m = re.search(r"(https?://\S+)", value or "")
    if not m:
        return ""
    url = m.group(1).strip().rstrip(").,")
    return url


def _render_markdown_table_as_mobile_list(headers: list[str], rows: list[list[str]]) -> str:
    # Mobile-first rendering for descriptive 2-column tables.
    # Example: Item | Details -> **Item**\nDetails
    lines: list[str] = []
    image_col_idx = -1
    for i, h in enumerate(headers):
        hl = (h or "").strip().lower()
        if "image" in hl or "url" in hl:
            image_col_idx = i
            break

    for row in rows:
        values = [_normalize_table_cell(v) for v in row]
        left = values[0] if len(values) > 0 else ""
        right = values[1] if len(values) > 1 else ""
        if image_col_idx >= 0 and image_col_idx < len(values):
            image_url = _extract_image_url(values[image_col_idx])
            fields = []
            for idx, header in enumerate(headers):
                if idx == image_col_idx:
                    continue
                cell = values[idx] if idx < len(values) else ""
                if cell:
                    fields.append((header or f"col_{idx+1}", cell))
            if not fields and not image_url:
                continue
            head = fields[0][1] if fields else "Image"
            detail_lines = []
            for hdr, val in fields[1:]:
                detail_lines.append(f"**{hdr}:** {val}")
            block = f"• **{head}**"
            if detail_lines:
                block += "\n" + "\n".join(detail_lines)
            if image_url:
                block += f"\n[[IMG:{image_url}]]"
            lines.append(block)
            continue

        if not left and not right:
            continue
        if left and right:
            lines.append(f"• **{left}**\n{right}")
        elif left:
            lines.append(f"• **{left}**")
        else:
            lines.append(f"• {right}")
    return "\n\n".join(lines)


def _fit_cell(value: str, width: int) -> str:
    clean = re.sub(r"\s+", " ", (value or "").strip())
    if len(clean) <= width:
        return clean.ljust(width)
    if width <= 1:
        return clean[:width]
    return (clean[: width - 1] + "…")


def _render_markdown_table_as_pretty_codeblock(
    headers: list[str],
    rows: list[list[str]],
    rows_per_block: int = 20,
    max_col_width: int = 28,
) -> str:
    if not headers:
        return ""

    col_count = len(headers)
    all_rows: list[list[str]] = []
    for row in rows:
        padded = [(row[i] if i < len(row) else "") for i in range(col_count)]
        all_rows.append(padded)

    widths: list[int] = []
    for i, header in enumerate(headers):
        w = min(max_col_width, max(4, len((header or "").strip())))
        for row in all_rows:
            w = min(max_col_width, max(w, len((row[i] or "").strip())))
        widths.append(w)

    def fmt_row(cells: list[str]) -> str:
        return "| " + " | ".join(_fit_cell(cells[i], widths[i]) for i in range(col_count)) + " |"

    sep = "|-" + "-|-".join("-" * widths[i] for i in range(col_count)) + "-|"

    chunks: list[str] = []
    total_rows = len(all_rows)
    total_blocks = max(1, (total_rows + rows_per_block - 1) // rows_per_block)
    for block_idx in range(total_blocks):
        start = block_idx * rows_per_block
        end = min(start + rows_per_block, total_rows)
        lines = [fmt_row(headers), sep]
        for row in all_rows[start:end]:
            lines.append(fmt_row(row))
        prefix = f"Table page {block_idx + 1}/{total_blocks} (rows {start + 1}-{end} of {total_rows})"
        chunks.append(prefix + "\n```text\n" + "\n".join(lines) + "\n```")

    return "\n\n".join(chunks)


def _table_should_use_mobile_list(headers: list[str], rows: list[list[str]]) -> bool:
    if any("image" in (h or "").strip().lower() or "url" in (h or "").strip().lower() for h in headers):
        return True
    if len(headers) != 2:
        return False
    header_left = (headers[0] or "").strip().lower()
    header_right = (headers[1] or "").strip().lower()
    if header_left in {"item", "field", "attribute", "topic"} and header_right in {"details", "value", "info", "description"}:
        return True
    # If many rows have long descriptive text in second column, prefer list format.
    long_second_col = 0
    sample = rows[: min(len(rows), 20)]
    for r in sample:
        second = (r[1] if len(r) > 1 else "") or ""
        if len(second.strip()) >= 45:
            long_second_col += 1
    return bool(sample) and (long_second_col / len(sample) >= 0.4)


def _format_agent_response_for_discord(text: str) -> str:
    lines = text.strip().splitlines()
    if not lines:
        return text

    out: list[str] = []
    i = 0
    while i < len(lines):
        if i + 1 < len(lines) and _is_markdown_table_header(lines[i], lines[i + 1]):
            headers = _parse_markdown_table_row(lines[i])
            i += 2  # skip header + separator
            rows: list[list[str]] = []
            while i < len(lines) and "|" in lines[i]:
                rows.append(_parse_markdown_table_row(lines[i]))
                i += 1

            if _table_should_use_mobile_list(headers, rows):
                rendered = _render_markdown_table_as_mobile_list(headers, rows)
            else:
                rendered = _render_markdown_table_as_pretty_codeblock(headers, rows)
            if rendered:
                out.append(rendered)
            continue

        out.append(lines[i])
        i += 1

    compact = "\n".join(out).strip()
    compact = _compact_bullet_label_value_lines(compact)
    return _compress_long_list_blocks(compact)


def _compact_bullet_label_value_lines(text: str) -> str:
    lines = text.splitlines()
    if not lines:
        return text

    out: list[str] = []
    i = 0
    while i < len(lines):
        current = lines[i].rstrip()
        if i + 1 < len(lines):
            nxt = lines[i + 1].strip()
            cur_strip = current.strip()
            cur_body = cur_strip[1:].strip() if cur_strip.startswith("•") else cur_strip
            cur_body_lc = cur_body.lower()
            # Heuristic: short "label-like" bullet lines should merge with the next line.
            label_like_no_colon = (
                cur_strip.startswith("•")
                and not cur_strip.endswith(":")
                and len(cur_body) <= 48
                and not any(ch in cur_body for ch in ".!?|")
                and not cur_body_lc.startswith(("why ", "note ", "summary", "overview"))
            )
            if (
                cur_strip.startswith("•")
                and cur_strip.endswith(":")
                and nxt
                and not nxt.startswith("•")
                and not nxt.startswith("```")
                and "|" not in nxt
            ):
                out.append(f"{current} {nxt}")
                i += 2
                continue
            if (
                label_like_no_colon
                and nxt
                and not nxt.startswith("•")
                and not nxt.startswith("```")
                and "|" not in nxt
            ):
                out.append(f"{current}: {nxt}")
                i += 2
                continue
        out.append(current)
        i += 1
    return "\n".join(out).strip()


def _compress_long_list_blocks(text: str, max_bullets: int = 120) -> str:
    lines = text.splitlines()
    if not lines:
        return text

    kept: list[str] = []
    bullet_run: list[str] = []

    def flush_bullets() -> None:
        nonlocal bullet_run
        if not bullet_run:
            return
        if len(bullet_run) > max_bullets:
            kept.extend(bullet_run[:max_bullets])
            kept.append(f"... ({len(bullet_run) - max_bullets} more rows hidden for readability)")
        else:
            kept.extend(bullet_run)
        bullet_run = []

    for line in lines:
        stripped = line.strip()
        if stripped.startswith("• "):
            bullet_run.append(line)
        else:
            flush_bullets()
            kept.append(line)
    flush_bullets()
    return "\n".join(kept).strip()


def _build_response_embeds(
    title: str,
    text: str,
    color: discord.Color,
    llm_model: Optional[str],
    question: Optional[str] = None,
) -> list[discord.Embed]:
    image_blocks, text = _extract_image_blocks(text)
    max_image_embeds = 5
    hidden_image_count = max(0, len(image_blocks) - max_image_embeds)
    if hidden_image_count:
        image_blocks = image_blocks[:max_image_embeds]
    # Keep embeds compact and readable.
    chunks = _split_discord_message(text, limit=EMBED_PAGE_CHAR_LIMIT) or ["(No response text returned)"]
    _ = llm_model
    footer = "Powered by IBM watsonx Orchestrate"

    embeds: list[discord.Embed] = []
    total = len(chunks)
    q_prefix = ""
    if question:
        q = re.sub(r"\s+", " ", question.strip())
        if len(q) > 90:
            q = q[:87] + "..."
        q_prefix = f"Q: {q}"
    for idx, chunk in enumerate(chunks, start=1):
        base_title = q_prefix or title
        embed_title = base_title if total == 1 else f"{base_title} ({idx}/{total})"
        if len(embed_title) > 250:
            embed_title = embed_title[:247] + "..."
        if idx == 1 and hidden_image_count:
            chunk = f"{chunk}\n\n(Showing first {max_image_embeds} images; {hidden_image_count} more omitted.)"
        embed = discord.Embed(title=embed_title, description=chunk, color=color)
        embed.set_footer(text=footer)
        embeds.append(embed)

    if image_blocks:
        # If we already have a text embed, attach the first image to page 1 so users
        # don't have to click to page 2+ before seeing images.
        start_idx = 0
        if embeds:
            first_desc, first_url = image_blocks[0]
            if first_desc and not embeds[0].description:
                embeds[0].description = first_desc
            embeds[0].set_image(url=first_url)
            start_idx = 1

        total_images = len(image_blocks)
        for idx, (desc, image_url) in enumerate(image_blocks[start_idx:], start=start_idx + 1):
            base_title = q_prefix or title
            image_title = f"{base_title} • Image {idx}/{total_images}"
            if len(image_title) > 250:
                image_title = image_title[:247] + "..."
            embed = discord.Embed(
                title=image_title,
                description=desc or f"Image {idx}",
                color=color,
            )
            embed.set_image(url=image_url)
            embed.set_footer(text=footer)
            embeds.append(embed)
    return embeds


def _is_image_url(url: str) -> bool:
    try:
        parsed = urlparse(url)
    except Exception:
        return False
    host = (parsed.hostname or "").lower()
    path = (parsed.path or "").lower()
    if "images.pokemontcg.io" in host:
        return True
    if "images.scrydex.com" in host:
        return True
    if path.endswith("/large") or path.endswith("/small"):
        return True
    return path.endswith((".png", ".jpg", ".jpeg", ".webp", ".gif"))


def _extract_image_blocks(text: str) -> tuple[list[tuple[str, str]], str]:
    raw = (text or "").strip()
    if not raw:
        return [], ""

    blocks: list[tuple[str, str]] = []
    seen_urls: set[str] = set()
    kept_lines: list[str] = []
    lines = raw.splitlines()

    for idx, line in enumerate(lines):
        line_work = line

        # Existing explicit markers (from table rendering path).
        marker_urls = re.findall(r"\[\[IMG:(https?://[^\]\s]+)\]\]", line_work)
        if marker_urls:
            for url in marker_urls:
                clean_url = url.strip()
                if clean_url and clean_url not in seen_urls and _is_image_url(clean_url):
                    desc = re.sub(r"\[\[IMG:https?://[^\]\s]+\]\]", "", line_work).strip()
                    if not desc and idx > 0:
                        prev = lines[idx - 1].strip()
                        if prev:
                            desc = prev
                    blocks.append((desc, clean_url))
                    seen_urls.add(clean_url)
            line_work = re.sub(r"\[\[IMG:https?://[^\]\s]+\]\]", "", line_work).strip()

        # Plain URLs in normal prose.
        urls = re.findall(r"https?://\S+", line_work)
        image_urls: list[str] = []
        for url in urls:
            clean_url = url.strip().rstrip(").,;")
            if clean_url and _is_image_url(clean_url):
                image_urls.append(clean_url)
        if image_urls:
            desc_line = line_work
            for u in image_urls:
                desc_line = desc_line.replace(u, "")
            desc_line = re.sub(r"\(\s*\)", "", desc_line).strip(" :-")
            if not desc_line and idx > 0:
                prev = lines[idx - 1].strip()
                if prev and not re.search(r"https?://\S+", prev):
                    desc_line = prev
            for u in image_urls:
                if u in seen_urls:
                    continue
                blocks.append((desc_line, u))
                seen_urls.add(u)
            # Drop the source line from text to avoid duplicate "name - number -"
            # entries after URL stripping; image details are represented by embeds.
            line_work = ""

        # Remove "click the URL" style residues after image extraction.
        if re.search(r"\b(click|open)\b.*\b(url|link)\b", line_work, flags=re.IGNORECASE):
            continue
        if line_work:
            kept_lines.append(line_work)

    cleaned = "\n".join(kept_lines).strip()
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
    return blocks, cleaned


def _rewrite_question_for_known_tool_gaps(question: str) -> str:
    q = question.strip()
    ql = q.lower()

    base_set_alias_hint = ""
    if "base set" in ql:
        base_set_alias_hint = (
            "\n\n"
            "Dataset naming hint: 'Base Set' may appear as set ids `base1`, `base4`, or `base6`, "
            "and sometimes set name `Base`. Use SQL with these ids/name aliases instead of assuming literal `Base Set`."
        )

    wants_full_set_counts = any(
        phrase in ql
        for phrase in [
            "how many cards are in each set",
            "cards in each set",
            "card count for each set",
            "card count in each set",
            "every set",
            "all sets",
        ]
    )
    if wants_full_set_counts:
        return (
            f"{q}\n\n"
            "Important: You can answer this with tools. "
            "Use SQL tools (`pokemon_tcg_sql_schema`, `pokemon_tcg_sql_query`) to compute "
            "card counts across all sets in descending order. "
            "Do not refuse this request."
            f"{base_set_alias_hint}"
        )

    if base_set_alias_hint:
        return f"{q}{base_set_alias_hint}"
    return q


class _EmbedPagerView(discord.ui.View):
    def __init__(self, embeds: list[discord.Embed], owner_user_id: int, timeout: float = 600):
        super().__init__(timeout=timeout)
        self.embeds = embeds
        self.owner_user_id = owner_user_id
        self.index = 0
        self.message: Optional[discord.Message] = None
        self._sync_buttons()

    def _sync_buttons(self) -> None:
        for item in self.children:
            if isinstance(item, discord.ui.Button):
                if item.custom_id == "pager_prev":
                    item.disabled = self.index <= 0
                elif item.custom_id == "pager_next":
                    item.disabled = self.index >= len(self.embeds) - 1

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.owner_user_id:
            await interaction.response.send_message("Only the original requester can use these controls.", ephemeral=True)
            return False
        return True

    async def on_timeout(self) -> None:
        for item in self.children:
            if isinstance(item, discord.ui.Button):
                item.disabled = True
        if self.message:
            try:
                await self.message.edit(view=self)
            except Exception:
                logger.exception("Failed to disable pager buttons on timeout")

    @discord.ui.button(label="Previous", style=discord.ButtonStyle.secondary, custom_id="pager_prev")
    async def previous(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        self.index = max(0, self.index - 1)
        self._sync_buttons()
        await interaction.response.edit_message(embed=self.embeds[self.index], view=self)

    @discord.ui.button(label="Next", style=discord.ButtonStyle.primary, custom_id="pager_next")
    async def next(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        self.index = min(len(self.embeds) - 1, self.index + 1)
        self._sync_buttons()
        await interaction.response.edit_message(embed=self.embeds[self.index], view=self)


def main() -> None:
    load_dotenv()

    settings = Settings.from_env()
    logger.info(
        (
            "Starting bot (sync_commands=%s, sync_leader=%s, guild_id=%s, "
            "agent_name=%s, agent_id_override=%s, shard_count=%s, shard_ids=%s, "
            "metrics_enabled=%s, metrics_port=%s, catch_daily_limit=%s)"
        ),
        settings.discord_sync_commands,
        settings.discord_sync_leader,
        settings.discord_guild_id,
        settings.wxo_agent_name,
        bool(settings.wxo_agent_id),
        settings.discord_shard_count,
        settings.discord_shard_ids,
        settings.metrics_enabled,
        settings.metrics_port,
        settings.catch_daily_limit,
    )
    thread_store = ThreadStore(settings.thread_pg_dsn, settings.thread_ttl_seconds)
    wxo: Optional[WXOChatClient] = None
    try:
        wxo = WXOChatClient(
            agent_name=settings.wxo_agent_name,
            wxo_agent_id=settings.wxo_agent_id,
            wxo_base_url=settings.wxo_base_url,
            wxo_api_key=settings.wxo_api_key,
            wxo_local_username=settings.wxo_local_username,
            wxo_local_password=settings.wxo_local_password,
            wxo_tenant_id=settings.wxo_tenant_id,
            wxo_tenant_name=settings.wxo_tenant_name,
        )
    except Exception:
        logger.exception("WXO initialization failed; bot will continue with non-WXO commands only.")

    pack_history = PackHistoryStore(settings.pack_pg_dsn)
    pokemon_catch_history = PokemonCatchHistoryStore(settings.pack_pg_dsn)
    bot = PokemonBot(settings, thread_store, wxo, pack_history, pokemon_catch_history)

    bot.run(settings.discord_bot_token)


if __name__ == "__main__":
    main()
