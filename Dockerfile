FROM python:3.11-slim
COPY --from=ghcr.io/astral-sh/uv:0.10.4 /uv /uvx /bin/

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY pyproject.toml uv.lock ./
RUN uv sync --locked --no-dev

COPY bot ./bot
COPY data/pokemontcg/csv ./data/pokemontcg/csv

CMD ["/app/.venv/bin/python", "bot/discord_wxo_bot.py"]
