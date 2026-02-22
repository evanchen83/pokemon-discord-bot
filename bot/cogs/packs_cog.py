from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any

import discord
from discord import app_commands
from discord.ext import commands

from features.embed_standards import EMBED_PAGE_CHAR_LIMIT
from features.pack_opening import format_pull_lines, rarity_bucket

if TYPE_CHECKING:
    from discord_wxo_bot import PokemonBot

logger = logging.getLogger(__name__)


class PacksCog(commands.Cog):
    def __init__(self, bot: "PokemonBot"):
        self.bot = bot
        # Fallback limiter used only when Postgres pack history is unavailable.
        self._open_pack_daily_usage_fallback: dict[int, tuple[int, int]] = {}

    async def _consume_open_pack_daily_slot_fallback(
        self, *, user_id: int, day_start_utc: datetime, daily_limit: int
    ) -> tuple[bool, int]:
        if daily_limit < 1:
            return False, 0
        day_key = int(day_start_utc.timestamp() // 86400)
        fallback = self._open_pack_daily_usage_fallback.get(user_id)
        if not fallback or fallback[0] != day_key:
            self._open_pack_daily_usage_fallback[user_id] = (day_key, 1)
            return True, 1
        current_uses = fallback[1]
        if current_uses >= daily_limit:
            return False, current_uses
        next_uses = current_uses + 1
        self._open_pack_daily_usage_fallback[user_id] = (day_key, next_uses)
        return True, next_uses

    @app_commands.command(name="open_pack", description="Open a cosmetic Pokemon TCG booster pack")
    @app_commands.describe(set_name="Set to open (autocomplete enabled)")
    async def open_pack(self, interaction: discord.Interaction, set_name: str) -> None:
        started_at = time.perf_counter()
        outcome = "success"
        successful_set_name: str | None = None
        try:
            await interaction.response.defer(thinking=True)
            if not self.bot.pack_service.is_available:
                outcome = "unavailable"
                await interaction.followup.send(
                    "Pack data is not available in this runtime. Open-pack is currently disabled.",
                    ephemeral=True,
                )
                return
            set_meta = self.bot.pack_service.get_set(set_name)
            if set_meta is None:
                outcome = "invalid_input"
                await interaction.followup.send(
                    f"Unknown set `{set_name}`. Start typing in `/open_pack` and pick one from autocomplete.",
                    ephemeral=True,
                )
                return

            now_utc = datetime.now(timezone.utc)
            day_start_utc = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)
            next_reset_utc = day_start_utc + timedelta(days=1)
            next_reset_unix = int(next_reset_utc.timestamp())
            if self.bot.pack_history.is_available:
                try:
                    allowed, opened_today = await asyncio.to_thread(
                        self.bot.pack_history.consume_open_pack_command_slot,
                        user_id=interaction.user.id,
                        day_start_utc=day_start_utc,
                        daily_limit=self.bot.open_pack_daily_limit,
                    )
                except Exception:
                    logger.exception("Failed consuming pack-open usage for user=%s", interaction.user.id)
                    allowed, opened_today = await self._consume_open_pack_daily_slot_fallback(
                        user_id=interaction.user.id,
                        day_start_utc=day_start_utc,
                        daily_limit=self.bot.open_pack_daily_limit,
                    )
            else:
                allowed, opened_today = await self._consume_open_pack_daily_slot_fallback(
                    user_id=interaction.user.id,
                    day_start_utc=day_start_utc,
                    daily_limit=self.bot.open_pack_daily_limit,
                )
            if not allowed:
                outcome = "rate_limited"
                await interaction.followup.send(
                    (
                        f"You have reached your daily pack limit ({self.bot.open_pack_daily_limit}/{self.bot.open_pack_daily_limit}). "
                        f"Next refresh: <t:{next_reset_unix}:F> (<t:{next_reset_unix}:R>)."
                    ),
                    ephemeral=True,
                )
                return

            pulled = self.bot.pack_service.open_pack(set_meta.set_id)
            if not pulled:
                outcome = "empty"
                await interaction.followup.send(f"No cards available for set `{set_meta.name}`.", ephemeral=True)
                return
            history_saved = False
            if self.bot.pack_history.is_available and interaction.channel_id is not None:
                try:
                    await asyncio.to_thread(
                        self.bot.pack_history.save_pack_opening,
                        user_id=interaction.user.id,
                        channel_id=interaction.channel_id,
                        set_meta=set_meta,
                        cards=pulled,
                    )
                    history_saved = True
                except Exception:
                    logger.exception("Failed saving pack history for user=%s", interaction.user.id)

            packs_left_today = max(0, self.bot.open_pack_daily_limit - opened_today)

            normals = [c for c in pulled if rarity_bucket(c.rarity) == "normal"]
            rares = [c for c in pulled if rarity_bucket(c.rarity) == "rare"]
            super_rares = [c for c in pulled if rarity_bucket(c.rarity) == "super_rare"]

            embed = discord.Embed(
                title=f"Opened Pack: {set_meta.name}",
                description=(
                    f"Series: **{set_meta.series}**\n"
                    f"Cards pulled: **{len(pulled)}**\n"
                    f"Packs left today: **{packs_left_today}/{self.bot.open_pack_daily_limit}**"
                    + (
                        f"\nNext refresh: <t:{next_reset_unix}:F> (<t:{next_reset_unix}:R>)"
                        if packs_left_today == 0
                        else ""
                    )
                ),
                color=self.bot.embed_color,
            )
            if set_meta.image_logo:
                embed.set_thumbnail(url=set_meta.image_logo)
            embed.add_field(name="Normals", value=format_pull_lines(normals), inline=False)
            embed.add_field(name="Rares", value=format_pull_lines(rares), inline=False)
            embed.add_field(name="Super Rares", value=format_pull_lines(super_rares), inline=False)

            save_suffix = "Saved to collection" if history_saved else "Not saved to collection"
            embed.set_footer(text=f"Cosmetic pull | {save_suffix} | Daily limit: {opened_today}/{self.bot.open_pack_daily_limit}")

            card_embeds: list[discord.Embed] = [embed]
            for idx, card in enumerate(pulled, start=1):
                card_embed = discord.Embed(
                    title=f"{set_meta.name} • Card {idx}/{len(pulled)}",
                    description=f"**{card.name}**\nRarity: **{card.rarity}**\nNumber: **{card.number or 'N/A'}**",
                    color=self.bot.embed_color,
                )
                if set_meta.image_logo:
                    card_embed.set_thumbnail(url=set_meta.image_logo)
                if card.image_url:
                    card_embed.set_image(url=card.image_url)
                card_embed.set_footer(
                    text=f"Cosmetic pull | {save_suffix} | Daily limit: {opened_today}/{self.bot.open_pack_daily_limit}"
                )
                card_embeds.append(card_embed)

            pager = self.bot.make_embed_pager(embeds=card_embeds, owner_user_id=interaction.user.id)
            sent = await interaction.followup.send(embed=card_embeds[0], view=pager, wait=True)
            pager.message = sent
            successful_set_name = set_meta.name
        except Exception:
            outcome = "error"
            raise
        finally:
            self.bot.record_command_metric(command="open_pack", outcome=outcome, started_at=started_at)
            if outcome == "success" and successful_set_name:
                self.bot.metrics.record_open_pack_set(successful_set_name)

    @open_pack.autocomplete("set_name")
    async def open_pack_set_autocomplete(
        self,
        interaction: discord.Interaction,
        current: str,
    ) -> list[app_commands.Choice[str]]:
        if not self.bot.pack_service.is_available:
            return []
        out: list[app_commands.Choice[str]] = []
        for rec in self.bot.pack_service.autocomplete_sets(current):
            label = rec.name
            if len(label) > 100:
                label = label[:97] + "..."
            out.append(app_commands.Choice(name=label, value=rec.set_id))
        return out

    @app_commands.command(name="my_cards", description="View your pulled cards grouped by set")
    async def my_cards(self, interaction: discord.Interaction) -> None:
        started_at = time.perf_counter()
        outcome = "success"
        try:
            await interaction.response.defer(thinking=True, ephemeral=True)
            if not self.bot.pack_history.is_available:
                outcome = "unavailable"
                await interaction.followup.send(self.bot.pack_history_unavailable_text, ephemeral=True)
                return

            try:
                rows = await asyncio.to_thread(
                    self.bot.pack_history.get_collection_grouped_by_set,
                    user_id=interaction.user.id,
                    max_sets=self.bot.my_cards_default_set_limit,
                )
            except Exception:
                outcome = "error"
                logger.exception("Failed reading pack history for user=%s", interaction.user.id)
                await interaction.followup.send(self.bot.pack_history_unavailable_text, ephemeral=True)
                return

            if not rows:
                outcome = "empty"
                await interaction.followup.send("No cards in your collection yet. Open a pack first with `/open_pack`.", ephemeral=True)
                return

            set_order: list[str] = []
            rows_by_set: dict[str, list[Any]] = {}
            for row in rows:
                if row.set_name not in rows_by_set:
                    set_order.append(row.set_name)
                    rows_by_set[row.set_name] = []
                rows_by_set[row.set_name].append(row)

            set_blocks: list[str] = []
            for set_name in set_order:
                set_rows = rows_by_set[set_name]
                set_rows.sort(
                    key=lambda r: (
                        -self.bot.rarity_rank(r.rarity),
                        -(r.copies or 0),
                        (r.card_name or "").lower(),
                        (r.card_number or ""),
                    )
                )
                top_rank = self.bot.rarity_rank(set_rows[0].rarity) if set_rows else 0
                lines_for_set: list[str] = []
                for row in set_rows:
                    num = f" #{row.card_number}" if row.card_number else ""
                    dup = f" ({row.copies}x)" if row.copies > 1 else ""
                    line = f"• {row.card_name}{num} [{row.rarity}]{dup}"
                    if self.bot.rarity_rank(row.rarity) == top_rank:
                        line = f"**{line}**"
                    lines_for_set.append(line)
                set_blocks.append(f"**{set_name}:**\n" + "\n".join(lines_for_set))

            pages = self.bot.paginate_set_blocks(set_blocks, limit=EMBED_PAGE_CHAR_LIMIT)
            embeds: list[discord.Embed] = []
            total = len(pages)
            for idx, page in enumerate(pages, start=1):
                title = "Your Card Collection" if total == 1 else f"Your Card Collection ({idx}/{total})"
                embed = discord.Embed(title=title, description=page, color=self.bot.embed_color)
                embeds.append(embed)

            if len(embeds) == 1:
                await interaction.followup.send(embed=embeds[0], ephemeral=True)
                return

            pager = self.bot.make_embed_pager(embeds=embeds, owner_user_id=interaction.user.id)
            msg = await interaction.followup.send(embed=embeds[0], view=pager, ephemeral=True, wait=True)
            pager.message = msg
        finally:
            self.bot.record_command_metric(command="my_cards", outcome=outcome, started_at=started_at)
