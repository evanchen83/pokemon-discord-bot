from __future__ import annotations

import asyncio
import io
import logging
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import TYPE_CHECKING

import discord
from discord import app_commands
from discord.ext import commands

from .catch_roll import ROLL_ROWS, roll_row, resolve_catches
from .image_rendering import POKEBOX_PAGE_SIZE, build_box_image_from_ids, build_roll_image
from .pagination import PokeboxPagerView, SpriteRateLimitError, build_pokebox_pages, region_order
from .sprites import SpriteRepository

if TYPE_CHECKING:
    from common.types import PokemonBotProtocol

logger = logging.getLogger(__name__)


class PcBoxCog(commands.Cog):
    def __init__(self, bot: "PokemonBotProtocol"):
        self.bot = bot
        cache_root = Path(__file__).resolve().parent.parent.parent / ".cache"
        self.sprites = SpriteRepository(cache_root)

    @commands.hybrid_command(name="catch", description="Roll a Pokemon catch board")
    async def catch(self, ctx: commands.Context) -> None:
        started_at = time.perf_counter()
        outcome = "success"
        try:
            if not self.bot.pokemon_catch_history.is_available:
                outcome = "unavailable"
                await ctx.send(self.bot.pokemon_catch_history_unavailable_text)
                return
            if ctx.interaction is not None:
                await ctx.defer()

            now_utc = datetime.now(timezone.utc)
            day_start_utc = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)
            next_reset_utc = day_start_utc + timedelta(days=1)
            next_reset_unix = int(next_reset_utc.timestamp())
            try:
                allowed, used_today = await asyncio.to_thread(
                    self.bot.pokemon_catch_history.consume_catch_command_slot,
                    user_id=ctx.author.id,
                    day_start_utc=day_start_utc,
                    daily_limit=self.bot.catch_daily_limit,
                )
            except Exception:
                outcome = "error"
                logger.exception("Failed reading catch usage for user=%s", ctx.author.id)
                await ctx.send(self.bot.pokemon_catch_history_unavailable_text)
                return
            if not allowed:
                outcome = "rate_limited"
                await ctx.send(
                    (
                        f"You have reached your daily catch limit "
                        f"({self.bot.catch_daily_limit}/{self.bot.catch_daily_limit}). "
                        f"Next refresh: <t:{next_reset_unix}:F> (<t:{next_reset_unix}:R>)."
                    )
                )
                return

            rolled = [roll_row() for _ in range(ROLL_ROWS)]
            catch_ids, won_rows = resolve_catches(rolled)

            if catch_ids:
                try:
                    await asyncio.to_thread(
                        self.bot.pokemon_catch_history.save_catches,
                        user_id=ctx.author.id,
                        pokemon_ids=catch_ids,
                    )
                except Exception:
                    outcome = "error"
                    logger.exception("Failed saving catches for user=%s", ctx.author.id)
                    await ctx.send(self.bot.pokemon_catch_history_unavailable_text)
                    return

            ids_for_name_lookup = {pokemon_id for row in rolled for pokemon_id, _tier in row}
            ids_for_name_lookup.update(catch_ids)
            names = await asyncio.to_thread(self.sprites.resolve_names, ids_for_name_lookup)

            try:
                sprite_ids = [pokemon_id for row in rolled for pokemon_id, _tier in row]
                await asyncio.to_thread(self.sprites.prefetch_sprites, sprite_ids)
                image_bytes = await asyncio.to_thread(
                    build_roll_image,
                    rolled,
                    won_rows,
                    self.sprites.load_resized_sprite,
                )
            except SpriteRateLimitError:
                outcome = "rate_limited"
                await ctx.send("The sprite host is rate-limiting right now. Please try `/catch` again soon.")
                return
            except Exception:
                outcome = "error"
                logger.exception("Failed to build catch roll image for user=%s", ctx.author.id)
                await ctx.send("I couldn't render the catch board right now. Please try again in a moment.")
                return

            description = "No catch this roll."
            if catch_ids:
                caught_names = [names.get(pid, f"Pokemon #{pid}") for pid in catch_ids]
                if len(caught_names) == 1:
                    description = f"You caught **{caught_names[0]}**."
                else:
                    description = "You caught " + ", ".join(f"**{name}**" for name in caught_names) + "."

            filename = "catch-roll.png"
            image_file = discord.File(io.BytesIO(image_bytes), filename=filename)
            embed = discord.Embed(title="Pokemon Catch Roll", description=description, color=self.bot.embed_color)
            embed.set_image(url=f"attachment://{filename}")
            catches_left_today = max(0, self.bot.catch_daily_limit - used_today)
            embed.set_footer(
                text=(
                    f"Use $pokebox or /pokebox to view your collection | "
                    f"Catches left today: {catches_left_today}/{self.bot.catch_daily_limit}"
                )
            )
            await ctx.send(embed=embed, file=image_file)
        finally:
            self.bot.record_command_metric(command="catch", outcome=outcome, started_at=started_at)

    @commands.hybrid_command(name="pokebox", description="Show Pokemon you have already caught")
    @app_commands.describe(sort_by="Sort order")
    @app_commands.choices(
        sort_by=[
            app_commands.Choice(name="Recent", value="recent"),
            app_commands.Choice(name="Pokedex ID", value="id"),
            app_commands.Choice(name="Name", value="name"),
            app_commands.Choice(name="Region", value="region"),
            app_commands.Choice(name="Type", value="type"),
        ]
    )
    async def pokebox(
        self,
        ctx: commands.Context,
        sort_by: str = "recent",
    ) -> None:
        started_at = time.perf_counter()
        outcome = "success"
        try:
            if not self.bot.pokemon_catch_history.is_available:
                outcome = "unavailable"
                await ctx.send(self.bot.pokemon_catch_history_unavailable_text)
                return
            if ctx.interaction is not None:
                await ctx.defer()

            raw_sort = (sort_by or "recent").strip().lower()
            sort_key = raw_sort
            if sort_key not in {"recent", "id", "name", "region", "type"}:
                outcome = "invalid_input"
                await ctx.send("Invalid sort_by. Use one of: `recent`, `id`, `name`, `region`, `type`.")
                return

            try:
                records = await asyncio.to_thread(
                    self.bot.pokemon_catch_history.list_user_collection,
                    user_id=ctx.author.id,
                    max_species=10000,
                )
                species_total, catches_total = await asyncio.to_thread(
                    self.bot.pokemon_catch_history.get_user_collection_totals,
                    user_id=ctx.author.id,
                )
            except Exception:
                outcome = "error"
                logger.exception("Failed reading collection for user=%s", ctx.author.id)
                await ctx.send(self.bot.pokemon_catch_history_unavailable_text)
                return

            if not records:
                outcome = "empty"
                await ctx.send("Your PokeBox is empty. Use `$catch` or `/catch` first.")
                return

            sorted_ids = [record.pokemon_id for record in records]
            names: dict[int, str] = {}
            primary_types: dict[int, str] = {}

            if sort_key == "id":
                sorted_ids.sort(key=lambda pokemon_id: pokemon_id)
            elif sort_key == "name":
                names = await asyncio.to_thread(self.sprites.resolve_names, set(sorted_ids))
                sorted_ids.sort(key=lambda pokemon_id: (names.get(pokemon_id, f"Pokemon {pokemon_id}"), pokemon_id))
            elif sort_key == "region":
                sorted_ids.sort(key=lambda pokemon_id: region_order(pokemon_id))
            elif sort_key == "type":
                primary_types = await asyncio.to_thread(self.sprites.resolve_primary_types, set(sorted_ids))
                sorted_ids.sort(key=lambda pokemon_id: (primary_types.get(pokemon_id, "Unknown"), pokemon_id))

            pages = build_pokebox_pages(sorted_ids=sorted_ids, sort_key=sort_key, primary_types=primary_types)
            if not pages:
                outcome = "empty"
                await ctx.send("Your PokeBox is empty. Use `$catch` or `/catch` first.")
                return

            warm_ids: list[int] = []
            for page in pages[:3]:
                warm_ids.extend(page.pokemon_ids)
            warm_count = min(len(warm_ids), POKEBOX_PAGE_SIZE * 3)
            if warm_count > 0:
                await asyncio.to_thread(self.sprites.prefetch_sprites, warm_ids[:warm_count])

            pager = PokeboxPagerView(
                cog=self,
                owner_user_id=ctx.author.id,
                pages=pages,
                sort_key=sort_key,
                species_total=species_total,
                catches_total=catches_total,
                names=names,
                primary_types=primary_types,
            )
            try:
                embed, image_file = await pager.render_current_page()
            except SpriteRateLimitError:
                outcome = "rate_limited"
                await ctx.send(
                    "Your collection is saved, but sprite rendering is temporarily rate-limited. Please try again soon."
                )
                return
            except Exception:
                outcome = "error"
                logger.exception("Failed to build PokeBox image for user=%s", ctx.author.id)
                await ctx.send("I couldn't build your PokeBox image right now. Please try again in a moment.")
                return

            sent = await ctx.send(embed=embed, file=image_file, view=pager)
            pager.message = sent
        finally:
            self.bot.record_command_metric(command="pokebox", outcome=outcome, started_at=started_at)

    def _build_box_image_from_ids(self, pokemon_ids: list[int], box_title: str = "POKEBOX", theme_type: str | None = None) -> bytes:
        self.sprites.prefetch_sprites(pokemon_ids)
        return build_box_image_from_ids(
            pokemon_ids,
            box_title=box_title,
            theme_type=theme_type,
            load_resized_sprite=self.sprites.load_resized_sprite,
        )
