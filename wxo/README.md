# WXO Deployment Manifests

Central location for all watsonx Orchestrate deployment YAML files.

## Layout

- `wxo/registry.yaml`: index of manifests and deployment order
- `wxo/environments/`: environment configs and references
- `wxo/knowledge-bases/`: KB deployment manifests
- `wxo/agents/`: agent manifests
- `wxo/channels/`: channel/binding manifests (Discord, web chat, etc.)
- `wxo/templates/`: starter templates for new manifests

## Convention

- Keep one logical resource per YAML file.
- Use predictable names: `<domain>-<resource>.yaml`.
- Add every new manifest to `registry.yaml` so deploy automation has a single source of truth.

## Example naming

- `pokemon-tcg-kb.yaml`
- `pokemon-tcg-agent.yaml`
- `pokemon-discord-channel.yaml`

## Import Workflow

Use the project helper wrapper to import/update resources using `.env` credentials.
It activates `WO_ENV` first (defaults to `local`), then imports resources into that active environment.
For cloud ADK environments, add/create that environment in ADK before activating it. Activating without adding typically only works for the default local environment.

```bash
wxo/scripts/import_wxo_from_env.sh all
```

This imports in order:
1. Tools
2. Knowledge base
3. Agent

Useful variants:

```bash
wxo/scripts/import_wxo_from_env.sh tools
wxo/scripts/import_wxo_from_env.sh kb
wxo/scripts/import_wxo_from_env.sh agent
wxo/scripts/import_wxo_from_env.sh all --env local
```

Direct/manual script (if you want to skip `.env` wrapper):

```bash
wxo/scripts/import_wxo_resources.sh all --env local
```
