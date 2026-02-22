from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING, Optional

import discord
from discord import app_commands
from discord.ext import commands

from .ownership_cache import ResponseOwnershipCache
from .query_flow import build_agent_error_embed

if TYPE_CHECKING:
    from common.types import PokemonBotProtocol

logger = logging.getLogger(__name__)


class PokeAgentCog(commands.Cog):
    def __init__(self, bot: "PokemonBotProtocol"):
        self.bot = bot
        self._owners = ResponseOwnershipCache()

    @app_commands.command(name="pokeagent", description="Chat with the pokemon_tcg_agent")
    @app_commands.describe(question="Your question for the Pokemon TCG agent")
    async def pokeagent(self, interaction: discord.Interaction, question: str) -> None:
        started_at = time.perf_counter()
        outcome = "success"
        try:
            if not await self.bot.ensure_wxo_available():
                outcome = "unavailable"
                await interaction.response.send_message(self.bot.wxo_unavailable_text, ephemeral=True)
                return
            if interaction.channel_id is None:
                outcome = "invalid_context"
                await interaction.response.send_message("This command must be used in a channel.", ephemeral=True)
                return

            await interaction.response.defer(thinking=True)

            try:
                response = await self.bot._run_agent_query(interaction.user.id, interaction.channel_id, question)
            except Exception as exc:
                outcome = "error"
                logger.exception("Failed to run WXO agent")
                await interaction.followup.send(embed=build_agent_error_embed(exc))
                return

            pretty_response = self.bot.format_agent_response_for_discord(response)
            embeds = self.bot.build_response_embeds(
                title="Pokemon TCG Agent",
                text=pretty_response if pretty_response.strip() else "(No response text returned)",
                color=self.bot.embed_color,
                llm_model=self.bot.wxo.agent_llm,
                question=question,
            )
            if len(embeds) == 1:
                sent = await interaction.followup.send(embed=embeds[0], wait=True)
                self._owners.remember(sent.id, interaction.user.id, interaction.channel_id)
                return

            pager = self.bot.make_embed_pager(embeds=embeds, owner_user_id=interaction.user.id)
            msg = await interaction.followup.send(embed=embeds[0], view=pager, wait=True)
            pager.message = msg
            self._owners.remember(msg.id, interaction.user.id, interaction.channel_id)
        finally:
            self.bot.record_command_metric(command="pokeagent", outcome=outcome, started_at=started_at)

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message) -> None:
        if message.author.bot or self.bot.user is None or message.channel is None:
            return

        question: Optional[str] = None
        force_new_thread = False
        if message.reference and message.reference.message_id:
            ref = self._owners.get(int(message.reference.message_id))
            if ref is not None:
                ref_owner, ref_channel, _ = ref
                if ref_channel == message.channel.id:
                    if ref_owner != message.author.id:
                        await message.reply(
                            "Please ask your own question by mentioning me, so I can use your own thread context.",
                            mention_author=False,
                        )
                        return
                    question = (message.content or "").strip()

        if question is None and self.bot.user in message.mentions:
            raw = (message.content or "")
            raw = raw.replace(f"<@{self.bot.user.id}>", "").replace(f"<@!{self.bot.user.id}>", "")
            question = raw.strip()
            force_new_thread = True

        if not question:
            return
        if not await self.bot.ensure_wxo_available():
            sent = await message.reply(self.bot.wxo_unavailable_text, mention_author=False)
            self._owners.remember(sent.id, message.author.id, message.channel.id)
            return

        async with message.channel.typing():
            try:
                response = await self.bot._run_agent_query(
                    message.author.id,
                    message.channel.id,
                    question,
                    force_new_thread=force_new_thread,
                )
            except Exception as exc:
                logger.exception("Failed to run WXO agent from message")
                sent_err = await message.reply(embed=build_agent_error_embed(exc), mention_author=False)
                self._owners.remember(sent_err.id, message.author.id, message.channel.id)
                return

        pretty_response = self.bot.format_agent_response_for_discord(response)
        embeds = self.bot.build_response_embeds(
            title="Pokemon TCG Agent",
            text=pretty_response if pretty_response.strip() else "(No response text returned)",
            color=self.bot.embed_color,
            llm_model=self.bot.wxo.agent_llm,
            question=question,
        )
        if len(embeds) == 1:
            sent = await message.reply(embed=embeds[0], mention_author=False)
            self._owners.remember(sent.id, message.author.id, message.channel.id)
            return

        pager = self.bot.make_embed_pager(embeds=embeds, owner_user_id=message.author.id)
        sent = await message.reply(embed=embeds[0], view=pager, mention_author=False)
        pager.message = sent
        self._owners.remember(sent.id, message.author.id, message.channel.id)
