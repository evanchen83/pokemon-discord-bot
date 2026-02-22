from __future__ import annotations

import asyncio
import logging
import time
from pathlib import Path
from typing import Optional

import discord
from discord import app_commands
from discord.ext import commands
from dotenv import load_dotenv

from common.config import MY_CARDS_DEFAULT_SET_LIMIT, OPEN_PACK_DAILY_LIMIT, Settings
from common.discord_rendering import (
    build_response_embeds,
    format_agent_response_for_discord,
    paginate_set_blocks,
    rarity_rank,
)
from common.metrics import BotMetrics
from common.ui_views import EmbedPagerView
from features.pack_history import PackHistoryStore
from features.pack_opening import PackService
from features.pokemon_catch_history import PokemonCatchHistoryStore
from infrastructure.thread_store import ThreadStore
from infrastructure.wxo_client import WXOChatClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("discord_wxo_bot")


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
        from cogs.admin.cog import AdminCog
        from cogs.packs.cog import PacksCog
        from cogs.pcbox.cog import PcBoxCog
        from cogs.pokeagent.cog import PokeAgentCog

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
        return format_agent_response_for_discord(text)

    def build_response_embeds(
        self,
        *,
        title: str,
        text: str,
        color: discord.Color,
        llm_model: Optional[str],
        question: Optional[str] = None,
    ) -> list[discord.Embed]:
        return build_response_embeds(title=title, text=text, color=color, llm_model=llm_model, question=question)

    def make_embed_pager(self, *, embeds: list[discord.Embed], owner_user_id: int) -> EmbedPagerView:
        return EmbedPagerView(embeds=embeds, owner_user_id=owner_user_id)

    def rarity_rank(self, rarity: str) -> int:
        return rarity_rank(rarity)

    def paginate_set_blocks(self, blocks: list[str], limit: int = 1700) -> list[str]:
        return paginate_set_blocks(blocks, limit=limit)

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
        next_thread_id, response = await asyncio.to_thread(self.wxo.ask, question, prior_thread_id)
        self.thread_store.upsert(user_id, channel_id, next_thread_id)
        return response


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
