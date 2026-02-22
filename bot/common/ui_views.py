from __future__ import annotations

import logging
from typing import Optional

import discord

logger = logging.getLogger("discord_wxo_bot")


class EmbedPagerView(discord.ui.View):
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
