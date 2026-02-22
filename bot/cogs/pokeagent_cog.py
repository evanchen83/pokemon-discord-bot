from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING, Optional

import discord
from discord import app_commands
from discord.ext import commands

if TYPE_CHECKING:
    from discord_wxo_bot import PokemonBot

logger = logging.getLogger(__name__)


class PokeAgentCog(commands.Cog):
    def __init__(self, bot: "PokemonBot"):
        self.bot = bot
        # bot_message_id -> (owner_user_id, channel_id, created_epoch)
        self._response_owner_by_message_id: dict[int, tuple[int, int, int]] = {}

    def _remember_response_message(self, message_id: int, owner_user_id: int, channel_id: int) -> None:
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
                err = discord.Embed(
                    title="Pokemon TCG Agent",
                    description=f"Agent request failed: `{exc}`",
                    color=discord.Color.red(),
                )
                err.set_footer(text="Powered by IBM watsonx Orchestrate")
                await interaction.followup.send(embed=err)
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
                self._remember_response_message(sent.id, interaction.user.id, interaction.channel_id)
                return

            pager = self.bot.make_embed_pager(embeds=embeds, owner_user_id=interaction.user.id)
            msg = await interaction.followup.send(embed=embeds[0], view=pager, wait=True)
            pager.message = msg
            self._remember_response_message(msg.id, interaction.user.id, interaction.channel_id)
        finally:
            self.bot.record_command_metric(command="pokeagent", outcome=outcome, started_at=started_at)

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message) -> None:
        if message.author.bot or self.bot.user is None or message.channel is None:
            return

        question: Optional[str] = None
        force_new_thread = False
        ref_owner: Optional[int] = None
        if message.reference and message.reference.message_id:
            ref = self._response_owner_by_message_id.get(int(message.reference.message_id))
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
            # Fresh @mention always starts a new WXO thread for this user/channel.
            force_new_thread = True

        if not question:
            return
        if not await self.bot.ensure_wxo_available():
            sent = await message.reply(self.bot.wxo_unavailable_text, mention_author=False)
            self._remember_response_message(sent.id, message.author.id, message.channel.id)
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
                err = discord.Embed(
                    title="Pokemon TCG Agent",
                    description=f"Agent request failed: `{exc}`",
                    color=discord.Color.red(),
                )
                err.set_footer(text="Powered by IBM watsonx Orchestrate")
                sent_err = await message.reply(embed=err, mention_author=False)
                self._remember_response_message(sent_err.id, message.author.id, message.channel.id)
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
            self._remember_response_message(sent.id, message.author.id, message.channel.id)
            return

        pager = self.bot.make_embed_pager(embeds=embeds, owner_user_id=message.author.id)
        sent = await message.reply(embed=embeds[0], view=pager, mention_author=False)
        pager.message = sent
        self._remember_response_message(sent.id, message.author.id, message.channel.id)
