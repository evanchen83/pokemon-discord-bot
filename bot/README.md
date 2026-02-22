# Discord -> WXO Bot

## What it does

- Adds slash command: `/pokeagent question:<text>`
- Adds slash command: `/open_pack set_name:<set>` (cosmetic only, with autocomplete)
- Adds slash command: `/my_cards` (shows your pulled cards grouped by set)
- Adds catch commands: `$catch` and `/catch` (slot-roll style catch command; saved to Postgres)
- Adds collection commands: `$pokebox` and `/pokebox` (renders your previously caught Pokemon sprites with sort + button pagination)
- Adds restricted test grant command: `$grantpokemon` and `/grantpokemon` (username `chewychiyu` only)
- Sends user message to WXO agent (`WXO_AGENT_NAME`, default `pokemon_tcg_agent`)
- Reuses per-user/per-channel WXO `thread_id`
- Expires inactive threads after `THREAD_TTL_MINUTES` (default `10`)

## Structure

- Cog-based command/listener architecture
- `bot/cogs/pokeagent_cog.py` for `/pokeagent` and mention/reply listener flow
- `bot/cogs/packs_cog.py` for `/open_pack`, autocomplete, and `/my_cards`
- `bot/cogs/pcbox_cog.py` for `$catch`/`/catch` catches and `$pokebox`/`/pokebox` sort/pagination rendering
- `bot/discord_wxo_bot.py` keeps bot wiring, shared services, and shared UI helpers

## Required env vars

- `DISCORD_BOT_TOKEN` (already in your `.env`)

Optional:

- `DISCORD_GUILD_ID` (optional; used for startup guild sync when `DISCORD_SYNC_COMMANDS=true`)
- `DISCORD_SYNC_COMMANDS` (default `false`; opt-in startup sync only)
- `WXO_AGENT_NAME` (default: `pokemon_tcg_agent`)
- `WXO_AGENT_ID` (optional override; if set, skips agent-name lookup)
- `THREAD_TTL_MINUTES` (default: `10`)
- `WXO_BASE_URL`
- `WXO_API_KEY`
- `PACK_PG_DSN` (Postgres DSN for pack history persistence)
- `THREAD_PG_DSN` (optional; defaults to `PACK_PG_DSN` for thread context persistence)
- `DISCORD_SHARD_COUNT` (optional total shard count; when omitted discord.py auto-detects)
- `DISCORD_SHARD_IDS` (optional comma-separated shard IDs assigned to this process, e.g. `0,1`)
- `DISCORD_SYNC_LEADER` (default `true`; when sharding across containers set to `true` on one container only)
- `METRICS_ENABLED` (default `true`; enables Prometheus metrics endpoint)
- `METRICS_PORT` (default `9108`; metrics endpoint port inside container)
- `CATCH_DAILY_LIMIT` (default `25`; max `$catch`/`/catch` uses per user per UTC day)

## Run

```bash
uv sync --locked
uv run python bot/discord_wxo_bot.py
```

## Notes

- In Docker, set `WXO_BASE_URL` + `WXO_API_KEY` in `.env`.
- Outside Docker, if explicit WXO vars are missing, the bot falls back to active ADK env credentials.
- App-command sync is manual by default. Use owner command (mention-prefix):
  - `@Bot sync` or `@Bot sync global`
  - `@Bot sync guild`
  - `@Bot sync copy` (dev helper: mirrors globals into the current guild)
  - `@Bot sync clear` (clears guild overrides then syncs)

## Docker

```bash
docker compose up --build
```

`docker-compose.yml` in this repo is configured for two local shard containers:
- `discord-bot-shard-0` with `DISCORD_SHARD_IDS=0`
- `discord-bot-shard-1` with `DISCORD_SHARD_IDS=1`
- Metrics are exposed for local scrape:
  - shard 0: `http://localhost:9108/metrics`
  - shard 1: `http://localhost:9109/metrics`

### Database migrations (Liquibase)

- Compose now runs a one-shot `db-init` service before bot containers start.
- Changelog entrypoint: `db/changelog/db.changelog-master.xml`
- Baseline schema changelog: `db/changelog/0001_baseline.sql`
- Add future schema changes as new files (for example `0002_add_*.sql`) and include them in `db.changelog-master.xml` in order.

### Metrics

Prometheus metrics now include:
- `discord_command_total{command,outcome}`
- `discord_command_hour_total{command,hour_utc}`
- `discord_command_duration_seconds{command,outcome}`
- `discord_open_pack_set_total{set_name}`

### Prometheus + Grafana

- Prometheus: `http://localhost:9090`
- Grafana: `http://localhost:3000`
- Grafana default credentials:
  - username: `admin`
  - password: `admin`

This repo now provisions:
- Prometheus scrape config: `monitoring/prometheus/prometheus.yml`
- Grafana datasource provisioning: `monitoring/grafana/provisioning/datasources/prometheus.yml`
- Grafana dashboard provisioning: `monitoring/grafana/provisioning/dashboards/dashboards.yml`
- Dashboard JSON: `monitoring/grafana/dashboards/pokemon-discord-usage.json`
