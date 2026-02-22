from __future__ import annotations

import asyncio
import random
from typing import TYPE_CHECKING, Literal, Optional

import discord
from discord import app_commands
from discord.ext import commands

if TYPE_CHECKING:
    from discord_wxo_bot import PokemonBot


class AdminCog(commands.Cog):
    GRANTPOKEMON_ALLOWED_USERNAME = "chewychiyu"

    def __init__(self, bot: "PokemonBot"):
        self.bot = bot

    @commands.command(name="sync")
    @commands.is_owner()
    async def sync_commands(self, ctx: commands.Context, scope: Optional[Literal["global", "guild", "copy", "clear"]] = None) -> None:
        """
        Owner-only app-command sync helper.

        Usage (mention-prefix):
        - @Bot sync            -> global sync
        - @Bot sync global     -> global sync
        - @Bot sync guild      -> sync current guild only
        - @Bot sync copy       -> copy global commands to current guild, then sync guild
        - @Bot sync clear      -> clear current guild commands, then sync guild
        """
        mode = (scope or "global").lower()

        if mode == "global":
            synced = await self.bot.sync_app_commands()
            await ctx.send(f"Synced {len(synced)} global app commands.")
            return

        if ctx.guild is None:
            await ctx.send("Guild scope requires running this command in a guild.")
            return

        guild = discord.Object(id=ctx.guild.id)
        if mode == "guild":
            synced = await self.bot.sync_app_commands(guild=guild)
            await ctx.send(f"Synced {len(synced)} guild app commands for `{ctx.guild.id}`.")
            return
        if mode == "copy":
            synced = await self.bot.sync_app_commands(guild=guild, copy_global_to_guild=True)
            await ctx.send(f"Copied globals and synced {len(synced)} guild app commands for `{ctx.guild.id}`.")
            return
        if mode == "clear":
            synced = await self.bot.sync_app_commands(guild=guild, clear_guild=True)
            await ctx.send(f"Cleared guild overrides and synced {len(synced)} guild app commands for `{ctx.guild.id}`.")
            return

        await ctx.send("Invalid scope. Use one of: `global`, `guild`, `copy`, `clear`.")

    @commands.hybrid_command(name="grantpokemon", description="Grant random Pokemon to a user for testing")
    @app_commands.describe(user="User to grant pokemon to (defaults to yourself)", count="Number of random catches to grant")
    async def grant_pokemon(self, ctx: commands.Context, user: Optional[discord.User] = None, count: int = 25) -> None:
        if (ctx.author.name or "").lower() != self.GRANTPOKEMON_ALLOWED_USERNAME:
            if ctx.interaction is not None:
                await ctx.send("You are not allowed to use this command.", ephemeral=True)
            else:
                await ctx.send("You are not allowed to use this command.")
            return
        if count < 1 or count > 2000:
            await ctx.send("Count must be between 1 and 2000.")
            return
        if not self.bot.pokemon_catch_history.is_available:
            await ctx.send(self.bot.pokemon_catch_history_unavailable_text)
            return

        target_user = user or ctx.author
        pokemon_ids = [random.randint(1, 1025) for _ in range(count)]
        try:
            await asyncio.to_thread(
                self.bot.pokemon_catch_history.save_catches,
                user_id=target_user.id,
                pokemon_ids=pokemon_ids,
            )
        except Exception:
            await ctx.send("Failed to grant pokemon right now. Please try again.")
            return

        unique_species = len(set(pokemon_ids))
        await ctx.send(
            f"Granted **{count}** random Pokemon catches to {target_user.mention} "
            f"across **{unique_species}** species."
        )
