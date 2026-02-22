from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

import discord
from discord import app_commands


@runtime_checkable
class PokemonBotProtocol(Protocol):
    user: discord.ClientUser | None
    embed_color: discord.Color
    wxo: Any
    wxo_unavailable_text: str
    pack_history_unavailable_text: str
    pokemon_catch_history_unavailable_text: str
    open_pack_daily_limit: int
    my_cards_default_set_limit: int
    catch_daily_limit: int
    pack_service: Any
    pack_history: Any
    pokemon_catch_history: Any
    metrics: Any

    async def ensure_wxo_available(self, *, force: bool = False) -> bool: ...

    async def sync_app_commands(
        self,
        *,
        guild: discord.abc.Snowflake | None = None,
        copy_global_to_guild: bool = False,
        clear_guild: bool = False,
    ) -> list[app_commands.AppCommand]: ...

    async def _run_agent_query(
        self,
        user_id: int,
        channel_id: int,
        question: str,
        force_new_thread: bool = False,
    ) -> str: ...

    def record_command_metric(self, *, command: str, outcome: str, started_at: float) -> None: ...

    def format_agent_response_for_discord(self, text: str) -> str: ...

    def build_response_embeds(
        self,
        *,
        title: str,
        text: str,
        color: discord.Color,
        llm_model: str | None,
        question: str | None = None,
    ) -> list[discord.Embed]: ...

    def make_embed_pager(self, *, embeds: list[discord.Embed], owner_user_id: int) -> discord.ui.View: ...

    def rarity_rank(self, rarity: str) -> int: ...

    def paginate_set_blocks(self, blocks: list[str], limit: int = 1700) -> list[str]: ...
