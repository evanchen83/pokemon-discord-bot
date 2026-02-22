from __future__ import annotations

import asyncio
import concurrent.futures
import csv
import io
import logging
import random
import time
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

import discord
import requests
from discord import app_commands
from discord.ext import commands
from PIL import Image, ImageDraw

if TYPE_CHECKING:
    from discord_wxo_bot import PokemonBot

logger = logging.getLogger(__name__)

MAX_RANDOM_POKEMON_ID = 1025
BOX_COLUMNS = 6
CELL_SIZE = 96
SPRITE_SIZE = 80
BOX_PADDING = 12
HEADER_HEIGHT = 28
ROLL_ROWS = 5
ROLL_COLUMNS = 3
ROLL_CELL_SIZE = 112
ROLL_MARKER_WIDTH = 34
POKEBOX_PAGE_SIZE = 24
ROW_JACKPOT_CHANCE = 0.20
ROW_PAIR_CHANCE = 0.35
POKEAPI_POKEMON_INDEX_URL = "https://pokeapi.co/api/v2/pokemon?limit=1302"
SPRITE_URL = "https://raw.githubusercontent.com/PokeAPI/sprites/master/sprites/pokemon/other/official-artwork/{pokemon_id}.png"
SPRITE_URL_FALLBACK = "https://raw.githubusercontent.com/PokeAPI/sprites/master/sprites/pokemon/{pokemon_id}.png"
POKEAPI_POKEMON_CSV_URL = "https://raw.githubusercontent.com/PokeAPI/pokeapi/master/data/v2/csv/pokemon.csv"
POKEAPI_POKEMON_TYPES_CSV_URL = "https://raw.githubusercontent.com/PokeAPI/pokeapi/master/data/v2/csv/pokemon_types.csv"
POKEAPI_TYPES_CSV_URL = "https://raw.githubusercontent.com/PokeAPI/pokeapi/master/data/v2/csv/types.csv"

RARITY_TIERS: tuple[tuple[str, float], ...] = (
    ("common", 0.72),
    ("uncommon", 0.2),
    ("rare", 0.07),
    ("legendary", 0.01),
)

LEGENDARY_POOL = {
    144,
    145,
    146,
    150,
    151,
    243,
    244,
    245,
    249,
    250,
    251,
    377,
    378,
    379,
    380,
    381,
    382,
    383,
    384,
    385,
    386,
    480,
    481,
    482,
    483,
    484,
    485,
    486,
    487,
    488,
    489,
    490,
    491,
    492,
    493,
    494,
    638,
    639,
    640,
    641,
    642,
    643,
    644,
    645,
    646,
    647,
    648,
    649,
    716,
    717,
    718,
    719,
    720,
    721,
    785,
    786,
    787,
    788,
    789,
    790,
    791,
    792,
    800,
    801,
    802,
    807,
    808,
    809,
    888,
    889,
    890,
    891,
    892,
    893,
    894,
    895,
    896,
    897,
    898,
    905,
    1001,
    1002,
    1003,
    1004,
    1007,
    1008,
    1014,
    1015,
    1024,
    1025,
}


class SpriteRateLimitError(RuntimeError):
    """Raised when the upstream sprite host indicates rate limiting."""


@dataclass(frozen=True)
class _PokeboxPage:
    pokemon_ids: list[int]
    title: str
    theme_type: str | None = None


class _PokeboxPagerView(discord.ui.View):
    def __init__(
        self,
        *,
        cog: "PcBoxCog",
        owner_user_id: int,
        pages: list[_PokeboxPage],
        sort_key: str,
        species_total: int,
        catches_total: int,
        names: dict[int, str],
        primary_types: dict[int, str],
        timeout: float = 600,
    ):
        super().__init__(timeout=timeout)
        self.cog = cog
        self.owner_user_id = int(owner_user_id)
        self.pages = list(pages)
        self.sort_key = sort_key
        self.species_total = species_total
        self.catches_total = catches_total
        self.names = names
        self.primary_types = primary_types
        self.page_index = 0
        self.message: discord.Message | None = None
        self._render_lock = asyncio.Lock()
        self._sync_buttons()

    @property
    def total_pages(self) -> int:
        return max(1, len(self.pages))

    def _current_page(self) -> _PokeboxPage:
        return self.pages[self.page_index]

    def _sync_buttons(self) -> None:
        for item in self.children:
            if isinstance(item, discord.ui.Button):
                if item.custom_id == "pokebox_prev":
                    item.disabled = self.page_index <= 0
                elif item.custom_id == "pokebox_next":
                    item.disabled = self.page_index >= self.total_pages - 1

    async def render_current_page(self) -> tuple[discord.Embed, discord.File]:
        async with self._render_lock:
            page = self._current_page()
            page_ids = page.pokemon_ids
            image_bytes = await asyncio.to_thread(
                self.cog._build_box_image_from_ids,
                page_ids,
                page.title,
                page.theme_type,
            )

        filename = "pokebox.png"
        image_file = discord.File(io.BytesIO(image_bytes), filename=filename)
        page_number = self.page_index + 1
        embed = discord.Embed(
            title=page.title,
            description=(
                f"Showing **{len(page_ids)}** of **{self.species_total}** species you have caught.\n"
                f"Total catches: **{self.catches_total}**"
            ),
            color=self.cog.bot.embed_color,
        )
        embed.set_image(url=f"attachment://{filename}")
        embed.set_footer(text=f"Sort: {self.sort_key} | Page {page_number}/{self.total_pages} | Sprites: PokeAPI")

        return embed, image_file

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.owner_user_id:
            await interaction.response.send_message("Only the original requester can use these controls.", ephemeral=True)
            return False
        return True

    async def on_timeout(self) -> None:
        for item in self.children:
            if isinstance(item, discord.ui.Button):
                item.disabled = True
        if self.message is not None:
            try:
                await self.message.edit(view=self)
            except Exception:
                logger.exception("Failed to disable pokebox pager buttons on timeout")

    @discord.ui.button(label="Previous", style=discord.ButtonStyle.secondary, custom_id="pokebox_prev")
    async def previous(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        if self.page_index <= 0:
            await interaction.response.defer()
            return
        self.page_index -= 1
        self._sync_buttons()
        await interaction.response.defer()
        try:
            embed, image_file = await self.render_current_page()
        except SpriteRateLimitError:
            await interaction.followup.send(
                "Sprite rendering is temporarily rate-limited. Please try again soon.",
                ephemeral=True,
            )
            return
        await interaction.message.edit(embed=embed, attachments=[image_file], view=self)

    @discord.ui.button(label="Next", style=discord.ButtonStyle.primary, custom_id="pokebox_next")
    async def next(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        if self.page_index >= self.total_pages - 1:
            await interaction.response.defer()
            return
        self.page_index += 1
        self._sync_buttons()
        await interaction.response.defer()
        try:
            embed, image_file = await self.render_current_page()
        except SpriteRateLimitError:
            await interaction.followup.send(
                "Sprite rendering is temporarily rate-limited. Please try again soon.",
                ephemeral=True,
            )
            return
        await interaction.message.edit(embed=embed, attachments=[image_file], view=self)


class PcBoxCog(commands.Cog):
    def __init__(self, bot: "PokemonBot"):
        self.bot = bot
        self.sprite_cache_dir = Path(__file__).resolve().parent.parent / ".cache" / "sprites"
        self.sprite_cache_dir.mkdir(parents=True, exist_ok=True)
        self.thumb_cache_dir = Path(__file__).resolve().parent.parent / ".cache" / "sprites-thumb"
        self.thumb_cache_dir.mkdir(parents=True, exist_ok=True)
        self._name_by_id: dict[int, str] = {}
        self._name_index_loaded = False
        self._primary_type_by_id: dict[int, str] = {}
        self._type_index_loaded = False

    @commands.hybrid_command(name="catch", description="Roll a Pokemon catch board")
    async def catch(self, ctx: commands.Context["PokemonBot"]) -> None:
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

            rolled = [self._roll_row() for _ in range(ROLL_ROWS)]
            catch_ids, won_rows = self._resolve_catches(rolled)

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
            names = await asyncio.to_thread(self._resolve_names, ids_for_name_lookup)

            try:
                image_bytes = await asyncio.to_thread(self._build_roll_image, rolled, won_rows)
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
    @app_commands.describe(
        sort_by="Sort order",
    )
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
        ctx: commands.Context["PokemonBot"],
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
                names = await asyncio.to_thread(self._resolve_names, set(sorted_ids))
                sorted_ids.sort(key=lambda pokemon_id: (names.get(pokemon_id, f"Pokemon {pokemon_id}"), pokemon_id))
            elif sort_key == "region":
                sorted_ids.sort(key=lambda pokemon_id: (self._region_order(pokemon_id), pokemon_id))
            elif sort_key == "type":
                primary_types = await asyncio.to_thread(self._resolve_primary_types, set(sorted_ids))
                sorted_ids.sort(key=lambda pokemon_id: (primary_types.get(pokemon_id, "Unknown"), pokemon_id))

            pages = self._build_pokebox_pages(sorted_ids=sorted_ids, sort_key=sort_key, primary_types=primary_types)
            if not pages:
                outcome = "empty"
                await ctx.send("Your PokeBox is empty. Use `$catch` or `/catch` first.")
                return

            # Warm sprite cache for first few pages so navigation feels instant.
            warm_ids: list[int] = []
            for page in pages[:3]:
                warm_ids.extend(page.pokemon_ids)
            warm_count = min(len(warm_ids), POKEBOX_PAGE_SIZE * 3)
            if warm_count > 0:
                await asyncio.to_thread(self._prefetch_sprites, warm_ids[:warm_count])

            pager = _PokeboxPagerView(
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

    def _build_pokebox_pages(
        self,
        *,
        sorted_ids: list[int],
        sort_key: str,
        primary_types: dict[int, str],
    ) -> list[_PokeboxPage]:
        if sort_key not in {"type", "region"}:
            return [
                _PokeboxPage(
                    pokemon_ids=sorted_ids[idx : idx + POKEBOX_PAGE_SIZE],
                    title="Your PokeBox",
                )
                for idx in range(0, len(sorted_ids), POKEBOX_PAGE_SIZE)
            ]

        if sort_key == "region":
            by_region: dict[str, list[int]] = {}
            for pokemon_id in sorted_ids:
                region = self._region_for_id(pokemon_id)
                by_region.setdefault(region, []).append(pokemon_id)

            region_order = [
                "Kanto",
                "Johto",
                "Hoenn",
                "Sinnoh",
                "Unova",
                "Kalos",
                "Alola",
                "Galar",
                "Paldea",
                "Unknown",
            ]
            pages: list[_PokeboxPage] = []
            for region_name in region_order:
                ids = by_region.get(region_name, [])
                if not ids:
                    continue
                for chunk_start in range(0, len(ids), POKEBOX_PAGE_SIZE):
                    chunk = ids[chunk_start : chunk_start + POKEBOX_PAGE_SIZE]
                    part = (chunk_start // POKEBOX_PAGE_SIZE) + 1
                    title = f"{region_name} PokeBox"
                    if len(ids) > POKEBOX_PAGE_SIZE:
                        title = f"{region_name} PokeBox (Part {part})"
                    pages.append(_PokeboxPage(pokemon_ids=chunk, title=title))
            return pages

        by_type: dict[str, list[int]] = {}
        for pokemon_id in sorted_ids:
            t = primary_types.get(pokemon_id, "Unknown")
            by_type.setdefault(t, []).append(pokemon_id)

        type_order = sorted(by_type.keys(), key=lambda type_name: type_name.lower())
        pages: list[_PokeboxPage] = []
        for type_name in type_order:
            ids = by_type[type_name]
            for chunk_start in range(0, len(ids), POKEBOX_PAGE_SIZE):
                chunk = ids[chunk_start : chunk_start + POKEBOX_PAGE_SIZE]
                part = (chunk_start // POKEBOX_PAGE_SIZE) + 1
                title = f"{type_name} PokeBox"
                if len(ids) > POKEBOX_PAGE_SIZE:
                    title = f"{type_name} PokeBox (Part {part})"
                pages.append(_PokeboxPage(pokemon_ids=chunk, title=title, theme_type=type_name))
        return pages

    def _roll_row(self) -> list[tuple[int, str]]:
        # Increase player hit-rate: sometimes intentionally generate a full match,
        # and often generate near-miss pair rows.
        if random.random() < ROW_JACKPOT_CHANCE:
            pick = self._roll_one()
            return [pick, pick, pick]

        if random.random() < ROW_PAIR_CHANCE:
            pair = self._roll_one()
            off = self._roll_one()
            row = [pair, pair, off]
            random.shuffle(row)
            return row

        return [self._roll_one() for _ in range(ROLL_COLUMNS)]

    def _roll_one(self) -> tuple[int, str]:
        tier = random.choices(
            population=[name for name, _weight in RARITY_TIERS],
            weights=[weight for _name, weight in RARITY_TIERS],
            k=1,
        )[0]
        if tier == "legendary":
            return random.choice(tuple(LEGENDARY_POOL)), tier
        if tier == "rare":
            return random.randint(350, MAX_RANDOM_POKEMON_ID), tier
        if tier == "uncommon":
            return random.randint(151, 700), tier
        return random.randint(1, 400), tier

    def _resolve_catches(self, rolled: list[list[tuple[int, str]]]) -> tuple[list[int], set[int]]:
        catches: list[int] = []
        won_rows: set[int] = set()
        for row_index, row in enumerate(rolled):
            ids = [pokemon_id for pokemon_id, _tier in row]
            if len(set(ids)) == 1:
                catches.append(ids[0])
                won_rows.add(row_index)
        return catches, won_rows

    def _build_roll_image(self, rolled: list[list[tuple[int, str]]], won_rows: set[int]) -> bytes:
        self._prefetch_sprites([pokemon_id for row in rolled for pokemon_id, _tier in row])
        width = BOX_PADDING * 2 + (ROLL_COLUMNS * ROLL_CELL_SIZE) + ROLL_MARKER_WIDTH
        height = BOX_PADDING * 2 + HEADER_HEIGHT + (ROLL_ROWS * ROLL_CELL_SIZE)

        canvas = Image.new("RGBA", (width, height), (238, 249, 255, 255))
        draw = ImageDraw.Draw(canvas)
        for y in range(height):
            ratio = y / max(1, (height - 1))
            r = int(238 + (255 - 238) * ratio)
            g = int(249 - (12 * ratio))
            b = int(255 - (30 * ratio))
            draw.line((0, y, width, y), fill=(r, g, b, 255))

        draw.rounded_rectangle(
            (BOX_PADDING // 2, BOX_PADDING // 2, width - (BOX_PADDING // 2), height - (BOX_PADDING // 2)),
            radius=14,
            fill=(255, 255, 255, 220),
            outline=(120, 170, 210, 255),
            width=2,
        )
        draw.rounded_rectangle(
            (BOX_PADDING, BOX_PADDING, width - BOX_PADDING, BOX_PADDING + HEADER_HEIGHT),
            radius=10,
            fill=(190, 222, 255, 255),
            outline=(120, 170, 210, 255),
            width=1,
        )
        draw.text((BOX_PADDING + 10, BOX_PADDING + 8), "CATCH ROLL", fill=(35, 70, 110, 255))

        for row_idx, row in enumerate(rolled):
            for col_idx, (pokemon_id, _tier) in enumerate(row):
                cell_x = BOX_PADDING + col_idx * ROLL_CELL_SIZE
                cell_y = BOX_PADDING + HEADER_HEIGHT + row_idx * ROLL_CELL_SIZE
                draw.rounded_rectangle(
                    (cell_x + 4, cell_y + 4, cell_x + ROLL_CELL_SIZE - 4, cell_y + ROLL_CELL_SIZE - 4),
                    radius=10,
                    fill=(245, 250, 255, 255),
                    outline=(184, 212, 237, 255),
                    width=1,
                )
                sprite = self._load_resized_sprite(pokemon_id, SPRITE_SIZE)
                paste_x = cell_x + (ROLL_CELL_SIZE - SPRITE_SIZE) // 2
                paste_y = cell_y + (ROLL_CELL_SIZE - SPRITE_SIZE) // 2
                canvas.paste(sprite, (paste_x, paste_y), sprite)

            marker = "OK" if row_idx in won_rows else "X"
            marker_color = (16, 135, 66, 255) if row_idx in won_rows else (178, 34, 34, 255)
            marker_x = BOX_PADDING + (ROLL_COLUMNS * ROLL_CELL_SIZE) + 8
            marker_y = BOX_PADDING + HEADER_HEIGHT + row_idx * ROLL_CELL_SIZE + (ROLL_CELL_SIZE // 2) - 8
            draw.text((marker_x, marker_y), marker, fill=marker_color)

        output = io.BytesIO()
        canvas.save(output, format="PNG", optimize=True, compress_level=9)
        return output.getvalue()

    def _build_box_image_from_ids(self, pokemon_ids: list[int], box_title: str = "POKEBOX", theme_type: str | None = None) -> bytes:
        self._prefetch_sprites(pokemon_ids)
        slots = POKEBOX_PAGE_SIZE
        rows = (slots + BOX_COLUMNS - 1) // BOX_COLUMNS
        width = BOX_PADDING * 2 + BOX_COLUMNS * CELL_SIZE
        height = BOX_PADDING * 2 + HEADER_HEIGHT + rows * CELL_SIZE

        c = self._theme_colors(theme_type)
        canvas = Image.new("RGBA", (width, height), c["bg_top"])
        draw = ImageDraw.Draw(canvas)
        for y in range(height):
            ratio = y / max(1, (height - 1))
            r = int(c["bg_top"][0] + (c["bg_bottom"][0] - c["bg_top"][0]) * ratio)
            g = int(c["bg_top"][1] + (c["bg_bottom"][1] - c["bg_top"][1]) * ratio)
            b = int(c["bg_top"][2] + (c["bg_bottom"][2] - c["bg_top"][2]) * ratio)
            draw.line((0, y, width, y), fill=(r, g, b, 255))

        draw.rounded_rectangle(
            (BOX_PADDING // 2, BOX_PADDING // 2, width - (BOX_PADDING // 2), height - (BOX_PADDING // 2)),
            radius=14,
            fill=c["panel_fill"],
            outline=c["outline"],
            width=2,
        )
        draw.rounded_rectangle(
            (BOX_PADDING, BOX_PADDING, width - BOX_PADDING, BOX_PADDING + HEADER_HEIGHT),
            radius=10,
            fill=c["header_fill"],
            outline=c["outline"],
            width=1,
        )
        draw.text((BOX_PADDING + 10, BOX_PADDING + 8), box_title.upper(), fill=c["title_text"])

        for idx in range(slots):
            row = idx // BOX_COLUMNS
            col = idx % BOX_COLUMNS
            cell_x = BOX_PADDING + col * CELL_SIZE
            cell_y = BOX_PADDING + HEADER_HEIGHT + row * CELL_SIZE
            draw.rounded_rectangle(
                (cell_x + 3, cell_y + 3, cell_x + CELL_SIZE - 3, cell_y + CELL_SIZE - 3),
                radius=10,
                fill=(245, 250, 255, 235),
                outline=(184, 212, 237, 255),
                width=1,
            )
            if idx < len(pokemon_ids):
                pokemon_id = pokemon_ids[idx]
                sprite = self._load_resized_sprite(pokemon_id, SPRITE_SIZE)
                paste_x = cell_x + (CELL_SIZE - SPRITE_SIZE) // 2
                paste_y = cell_y + (CELL_SIZE - SPRITE_SIZE) // 2
                canvas.paste(sprite, (paste_x, paste_y), sprite)
            else:
                # Fill remaining slots so every page has consistent dimensions and visual structure.
                draw.rounded_rectangle(
                    (cell_x + 12, cell_y + 12, cell_x + CELL_SIZE - 12, cell_y + CELL_SIZE - 12),
                    radius=8,
                    outline=(198, 216, 234, 255),
                    width=1,
                )

        output = io.BytesIO()
        canvas.save(output, format="PNG", optimize=True, compress_level=9)
        return output.getvalue()

    def _load_resized_sprite(self, pokemon_id: int, size: int) -> Image.Image:
        thumb_file = self.thumb_cache_dir / f"{pokemon_id}_{size}.png"
        if thumb_file.exists():
            try:
                with Image.open(thumb_file) as img:
                    return img.convert("RGBA")
            except Exception:
                logger.warning("Failed reading cached sprite thumbnail for id=%s size=%s", pokemon_id, size)

        sprite = self._load_sprite(pokemon_id)
        resized = sprite.resize((size, size), Image.Resampling.LANCZOS)
        try:
            resized.save(thumb_file, format="PNG", optimize=True, compress_level=9)
        except Exception:
            logger.debug("Failed writing sprite thumbnail cache for id=%s size=%s", pokemon_id, size)
        return resized

    def _prefetch_sprites(self, pokemon_ids: list[int]) -> None:
        unique_ids = sorted({int(pokemon_id) for pokemon_id in pokemon_ids if int(pokemon_id) > 0})
        missing = [pokemon_id for pokemon_id in unique_ids if not (self.sprite_cache_dir / f"{pokemon_id}.png").exists()]
        if not missing:
            return

        first_error: Exception | None = None
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(8, max(1, len(missing)))) as pool:
            futures = [pool.submit(self._load_sprite, pokemon_id) for pokemon_id in missing]
            for fut in concurrent.futures.as_completed(futures):
                try:
                    img = fut.result()
                    img.close()
                except SpriteRateLimitError as exc:
                    first_error = exc
                    break
                except Exception as exc:
                    first_error = first_error or exc
        if isinstance(first_error, SpriteRateLimitError):
            raise first_error

    @staticmethod
    def _theme_colors(theme_type: str | None) -> dict[str, tuple[int, int, int, int]]:
        themes: dict[str, dict[str, tuple[int, int, int, int]]] = {
            "Fire": {
                "bg_top": (255, 235, 220, 255),
                "bg_bottom": (255, 187, 140, 255),
                "panel_fill": (255, 252, 248, 220),
                "header_fill": (255, 196, 157, 255),
                "outline": (186, 92, 50, 255),
                "title_text": (110, 40, 18, 255),
            },
            "Water": {
                "bg_top": (225, 242, 255, 255),
                "bg_bottom": (171, 215, 255, 255),
                "panel_fill": (248, 253, 255, 220),
                "header_fill": (177, 218, 255, 255),
                "outline": (56, 121, 186, 255),
                "title_text": (23, 63, 115, 255),
            },
            "Grass": {
                "bg_top": (232, 250, 232, 255),
                "bg_bottom": (187, 230, 180, 255),
                "panel_fill": (248, 255, 247, 220),
                "header_fill": (191, 231, 185, 255),
                "outline": (67, 137, 61, 255),
                "title_text": (30, 85, 28, 255),
            },
            "Electric": {
                "bg_top": (255, 250, 218, 255),
                "bg_bottom": (255, 233, 143, 255),
                "panel_fill": (255, 255, 245, 220),
                "header_fill": (255, 237, 154, 255),
                "outline": (170, 140, 35, 255),
                "title_text": (93, 78, 19, 255),
            },
        }
        default_theme = {
            "bg_top": (238, 249, 255, 255),
            "bg_bottom": (255, 237, 225, 255),
            "panel_fill": (255, 255, 255, 220),
            "header_fill": (190, 222, 255, 255),
            "outline": (120, 170, 210, 255),
            "title_text": (35, 70, 110, 255),
        }
        if not theme_type:
            return default_theme
        return themes.get(theme_type, default_theme)

    def _load_sprite(self, pokemon_id: int) -> Image.Image:
        cache_file = self.sprite_cache_dir / f"{pokemon_id}.png"
        if cache_file.exists():
            try:
                with Image.open(cache_file) as img:
                    return img.convert("RGBA")
            except Exception:
                logger.warning("Failed reading cached sprite for id=%s; re-downloading", pokemon_id)

        primary_url = SPRITE_URL.format(pokemon_id=pokemon_id)
        fallback_url = SPRITE_URL_FALLBACK.format(pokemon_id=pokemon_id)
        try:
            resp = requests.get(primary_url, timeout=8)
            if self._is_rate_limited_response(resp):
                raise SpriteRateLimitError("Sprite host rate limit encountered")
            if resp.status_code >= 400:
                fallback_resp = requests.get(fallback_url, timeout=8)
                if self._is_rate_limited_response(fallback_resp):
                    raise SpriteRateLimitError("Sprite host rate limit encountered")
                fallback_resp.raise_for_status()
                resp = fallback_resp
            else:
                resp.raise_for_status()
            cache_file.write_bytes(resp.content)
            with Image.open(io.BytesIO(resp.content)) as img:
                return img.convert("RGBA")
        except SpriteRateLimitError:
            raise
        except Exception:
            logger.warning("Failed downloading sprite for id=%s", pokemon_id, exc_info=True)
            return self._fallback_sprite()

    def _resolve_names(self, pokemon_ids: set[int]) -> dict[int, str]:
        self._ensure_name_index()
        out: dict[int, str] = {}
        for pokemon_id in pokemon_ids:
            out[int(pokemon_id)] = self._name_by_id.get(int(pokemon_id), f"Pokemon #{int(pokemon_id)}")
        return out

    def _ensure_name_index(self) -> None:
        if self._name_index_loaded:
            return
        self._name_index_loaded = True
        try:
            resp = requests.get(POKEAPI_POKEMON_INDEX_URL, timeout=10)
            resp.raise_for_status()
            data = resp.json() if resp.text else {}
            results = data.get("results", [])
            if not isinstance(results, list):
                return
            for idx, rec in enumerate(results, start=1):
                if idx > MAX_RANDOM_POKEMON_ID:
                    break
                if not isinstance(rec, dict):
                    continue
                name = str(rec.get("name") or "").strip()
                if name:
                    self._name_by_id[idx] = self._display_name(name)
        except Exception:
            logger.warning("Failed loading Pokemon names from PokeAPI; falling back to ids.", exc_info=True)

    def _resolve_primary_types(self, pokemon_ids: set[int]) -> dict[int, str]:
        self._ensure_type_index()
        out: dict[int, str] = {}
        for pokemon_id in pokemon_ids:
            out[int(pokemon_id)] = self._primary_type_by_id.get(int(pokemon_id), "Unknown")
        return out

    def _ensure_type_index(self) -> None:
        if self._type_index_loaded:
            return
        self._type_index_loaded = True

        try:
            pokemon_csv = self._fetch_csv_rows(POKEAPI_POKEMON_CSV_URL)
            pokemon_types_csv = self._fetch_csv_rows(POKEAPI_POKEMON_TYPES_CSV_URL)
            types_csv = self._fetch_csv_rows(POKEAPI_TYPES_CSV_URL)
        except Exception:
            logger.warning("Failed loading type metadata from PokeAPI CSVs.", exc_info=True)
            return

        type_name_by_id: dict[int, str] = {}
        for row in types_csv:
            try:
                type_id = int(row.get("id", "0") or 0)
            except Exception:
                continue
            identifier = str(row.get("identifier") or "").strip()
            if type_id > 0 and identifier:
                type_name_by_id[type_id] = self._display_name(identifier)

        default_pokemon_id_by_species: dict[int, int] = {}
        for row in pokemon_csv:
            try:
                is_default = int(row.get("is_default", "0") or 0)
                pokemon_id = int(row.get("id", "0") or 0)
                species_id = int(row.get("species_id", "0") or 0)
            except Exception:
                continue
            if is_default == 1 and pokemon_id > 0 and species_id > 0 and species_id <= MAX_RANDOM_POKEMON_ID:
                default_pokemon_id_by_species[species_id] = pokemon_id

        primary_type_by_pokemon_id: dict[int, str] = {}
        for row in pokemon_types_csv:
            try:
                pokemon_id = int(row.get("pokemon_id", "0") or 0)
                type_id = int(row.get("type_id", "0") or 0)
                slot = int(row.get("slot", "0") or 0)
            except Exception:
                continue
            if slot != 1:
                continue
            type_name = type_name_by_id.get(type_id)
            if pokemon_id > 0 and type_name:
                primary_type_by_pokemon_id[pokemon_id] = type_name

        for species_id, pokemon_id in default_pokemon_id_by_species.items():
            self._primary_type_by_id[species_id] = primary_type_by_pokemon_id.get(pokemon_id, "Unknown")

    @staticmethod
    def _fetch_csv_rows(url: str) -> list[dict[str, str]]:
        resp = requests.get(url, timeout=12)
        resp.raise_for_status()
        text = resp.text if resp.text else ""
        if not text.strip():
            return []
        reader = csv.DictReader(io.StringIO(text))
        return [dict(row) for row in reader if isinstance(row, dict)]

    @staticmethod
    def _region_for_id(pokemon_id: int) -> str:
        pid = int(pokemon_id)
        if 1 <= pid <= 151:
            return "Kanto"
        if 152 <= pid <= 251:
            return "Johto"
        if 252 <= pid <= 386:
            return "Hoenn"
        if 387 <= pid <= 493:
            return "Sinnoh"
        if 494 <= pid <= 649:
            return "Unova"
        if 650 <= pid <= 721:
            return "Kalos"
        if 722 <= pid <= 809:
            return "Alola"
        if 810 <= pid <= 905:
            return "Galar"
        if 906 <= pid <= 1025:
            return "Paldea"
        return "Unknown"

    @classmethod
    def _region_order(cls, pokemon_id: int) -> tuple[int, int]:
        region = cls._region_for_id(pokemon_id)
        order = {
            "Kanto": 1,
            "Johto": 2,
            "Hoenn": 3,
            "Sinnoh": 4,
            "Unova": 5,
            "Kalos": 6,
            "Alola": 7,
            "Galar": 8,
            "Paldea": 9,
        }
        return order.get(region, 99), int(pokemon_id)

    @staticmethod
    def _display_name(raw_name: str) -> str:
        return raw_name.replace("-", " ").replace("_", " ").title()

    @staticmethod
    def _is_rate_limited_response(resp: requests.Response) -> bool:
        if resp.status_code == 429:
            return True
        if resp.status_code == 403 and resp.headers.get("X-RateLimit-Remaining") == "0":
            return True
        return False

    @staticmethod
    def _fallback_sprite() -> Image.Image:
        img = Image.new("RGBA", (SPRITE_SIZE, SPRITE_SIZE), (220, 230, 245, 255))
        draw = ImageDraw.Draw(img)
        draw.rounded_rectangle((2, 2, SPRITE_SIZE - 2, SPRITE_SIZE - 2), radius=8, outline=(140, 150, 170, 255), width=2)
        draw.text((SPRITE_SIZE // 2 - 4, SPRITE_SIZE // 2 - 7), "?", fill=(90, 100, 120, 255))
        return img
