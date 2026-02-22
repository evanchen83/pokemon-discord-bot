from __future__ import annotations

import asyncio
import io
import logging
from dataclasses import dataclass
from typing import Protocol

import discord

from .image_rendering import POKEBOX_PAGE_SIZE

logger = logging.getLogger(__name__)


class SpriteRateLimitError(RuntimeError):
    """Raised when the upstream sprite host indicates rate limiting."""


@dataclass(frozen=True)
class PokeboxPage:
    pokemon_ids: list[int]
    title: str
    theme_type: str | None = None


class _PcBoxRenderProtocol(Protocol):
    bot: object

    def _build_box_image_from_ids(self, pokemon_ids: list[int], box_title: str = "POKEBOX", theme_type: str | None = None) -> bytes: ...


class PokeboxPagerView(discord.ui.View):
    def __init__(
        self,
        *,
        cog: _PcBoxRenderProtocol,
        owner_user_id: int,
        pages: list[PokeboxPage],
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

    def _current_page(self) -> PokeboxPage:
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
            color=getattr(self.cog.bot, "embed_color"),
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


def region_for_id(pokemon_id: int) -> str:
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


def region_order(pokemon_id: int) -> tuple[int, int]:
    region = region_for_id(pokemon_id)
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


def build_pokebox_pages(*, sorted_ids: list[int], sort_key: str, primary_types: dict[int, str]) -> list[PokeboxPage]:
    if sort_key not in {"type", "region"}:
        return [
            PokeboxPage(
                pokemon_ids=sorted_ids[idx : idx + POKEBOX_PAGE_SIZE],
                title="Your PokeBox",
            )
            for idx in range(0, len(sorted_ids), POKEBOX_PAGE_SIZE)
        ]

    if sort_key == "region":
        by_region: dict[str, list[int]] = {}
        for pokemon_id in sorted_ids:
            region = region_for_id(pokemon_id)
            by_region.setdefault(region, []).append(pokemon_id)

        region_ordered = ["Kanto", "Johto", "Hoenn", "Sinnoh", "Unova", "Kalos", "Alola", "Galar", "Paldea", "Unknown"]
        pages: list[PokeboxPage] = []
        for region_name in region_ordered:
            ids = by_region.get(region_name, [])
            if not ids:
                continue
            for chunk_start in range(0, len(ids), POKEBOX_PAGE_SIZE):
                chunk = ids[chunk_start : chunk_start + POKEBOX_PAGE_SIZE]
                part = (chunk_start // POKEBOX_PAGE_SIZE) + 1
                title = f"{region_name} PokeBox"
                if len(ids) > POKEBOX_PAGE_SIZE:
                    title = f"{region_name} PokeBox (Part {part})"
                pages.append(PokeboxPage(pokemon_ids=chunk, title=title))
        return pages

    by_type: dict[str, list[int]] = {}
    for pokemon_id in sorted_ids:
        t = primary_types.get(pokemon_id, "Unknown")
        by_type.setdefault(t, []).append(pokemon_id)

    type_ordered = sorted(by_type.keys(), key=lambda type_name: type_name.lower())
    pages: list[PokeboxPage] = []
    for type_name in type_ordered:
        ids = by_type[type_name]
        for chunk_start in range(0, len(ids), POKEBOX_PAGE_SIZE):
            chunk = ids[chunk_start : chunk_start + POKEBOX_PAGE_SIZE]
            part = (chunk_start // POKEBOX_PAGE_SIZE) + 1
            title = f"{type_name} PokeBox"
            if len(ids) > POKEBOX_PAGE_SIZE:
                title = f"{type_name} PokeBox (Part {part})"
            pages.append(PokeboxPage(pokemon_ids=chunk, title=title, theme_type=type_name))
    return pages
