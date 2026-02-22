# Discord -> WXO Bot

## What it does

- Adds slash command: `/pokeagent question:<text>`
- Adds slash command: `/open_pack set_name:<set>` (cosmetic only, with autocomplete)
- Adds slash command: `/my_cards` (shows your pulled cards grouped by set)
- Adds catch commands: `$catch` and `/catch` (slot-roll style catch command; saved to Postgres)
- Adds collection commands: `$pokebox` and `/pokebox` (renders your previously caught Pokemon sprites with sort + button pagination)
- Adds restricted test grant command: `$grantpokemon` and `/grantpokemon` (username `chewychiyu` only)
- Sends user message to WXO agent (`WO_AGENT_NAME`, default `pokemon_tcg_agent`)
- Reuses per-user/per-channel WXO `thread_id`
- Expires inactive threads after `THREAD_TTL_MINUTES` (default `10`)

## Structure

- Cog-based command/listener architecture with feature packages under `bot/cogs/`
- `bot/cogs/pokeagent/` for `/pokeagent` plus mention/reply flow
- `bot/cogs/packs/` for `/open_pack`, autocomplete, and `/my_cards`
- `bot/cogs/pcbox/` for `$catch`/`/catch` and `$pokebox`/`/pokebox` rendering
- `bot/cogs/admin/` for sync + grant helpers
- `bot/common/` for shared config, Discord rendering, pager UI, metrics, and types
- `bot/infrastructure/` for WXO runtime client + thread state persistence
- `bot/discord_wxo_bot.py` as thin composition/root entrypoint

## Required env vars

- `DISCORD_BOT_TOKEN` (already in your `.env`)

Optional:

- `DISCORD_GUILD_ID` (optional; used for startup guild sync when `DISCORD_SYNC_COMMANDS=true`)
- `DISCORD_SYNC_COMMANDS` (default `false`; opt-in startup sync only)
- `WO_AGENT_NAME` (default: `pokemon_tcg_agent`)
- `WO_AGENT_ID` (optional override; if set, skips agent-name lookup)
- `THREAD_TTL_MINUTES` (default: `10`)
- `WO_INSTANCE` (cloud runtime base URL; used by default)
- `WO_API_KEY` (cloud runtime API key; used by default)
- `WO_RUNTIME_INSTANCE` (optional bot-only target override; defaults to `WO_INSTANCE`)
- `WO_RUNTIME_API_KEY` (optional bot-only key override; defaults to `WO_API_KEY`)
- `WO_LOCAL_USERNAME` (only used when runtime target is local; default `wxo.archer@ibm.com`)
- `WO_LOCAL_PASSWORD` (only used when runtime target is local; default `watsonx`)
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

### Cloud-hosted WXO (Production/Default)

- Set `WO_INSTANCE` + `WO_API_KEY` in `.env`.
- Bot auth uses API key header (`WO_API_KEY`).
- `WO_LOCAL_USERNAME` / `WO_LOCAL_PASSWORD` are ignored in cloud mode.
- Advanced override (optional): set `WO_RUNTIME_INSTANCE` / `WO_RUNTIME_API_KEY` only if bot runtime target should differ from default cloud target.

### Self-hosted runtime (Local ADK)

- Keep cloud vars (`WO_INSTANCE`, `WO_API_KEY`) for script/import workflows and cloud-backed AI inference access.
- Point bot runtime to local with `WO_RUNTIME_INSTANCE=http://<your-local-runtime>`.
- Set `WO_LOCAL_USERNAME` + `WO_LOCAL_PASSWORD` for local token login.
- Bot gets JWT from `/auth/token` in this mode.
- API key is not used for bot request auth in this local mode.

### Script-only env

- `WO_ENV` is used by import scripts, not by bot runtime auth.
- For cloud ADK environments: add/create the environment in ADK first, then activate it. Activating without adding works only for the default local environment.
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
