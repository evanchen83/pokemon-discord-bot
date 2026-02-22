from __future__ import annotations

import discord


def build_agent_error_embed(exc: Exception) -> discord.Embed:
    err = discord.Embed(
        title="Pokemon TCG Agent",
        description=f"Agent request failed: `{exc}`",
        color=discord.Color.red(),
    )
    err.set_footer(text="Powered by IBM watsonx Orchestrate")
    return err
