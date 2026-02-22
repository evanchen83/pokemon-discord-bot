# WXO Deployment Manifests

Central location for all watsonx Orchestrate deployment YAML files.

## Layout

- `deploy/wxo/registry.yaml`: index of manifests and deployment order
- `deploy/wxo/environments/`: environment configs and references
- `deploy/wxo/knowledge-bases/`: KB deployment manifests
- `deploy/wxo/agents/`: agent manifests
- `deploy/wxo/channels/`: channel/binding manifests (Discord, web chat, etc.)
- `deploy/wxo/templates/`: starter templates for new manifests

## Convention

- Keep one logical resource per YAML file.
- Use predictable names: `<domain>-<resource>.yaml`.
- Add every new manifest to `registry.yaml` so deploy automation has a single source of truth.

## Example naming

- `pokemon-tcg-kb.yaml`
- `pokemon-tcg-agent.yaml`
- `pokemon-discord-channel.yaml`

## Import Workflow

Use the project helper script to import/update resources into the active ADK environment.

```bash
scripts/import_wxo_resources.sh
```

This imports in order:
1. Tools
2. Knowledge base
3. Agent

Useful variants:

```bash
scripts/import_wxo_resources.sh tools
scripts/import_wxo_resources.sh kb
scripts/import_wxo_resources.sh agent
scripts/import_wxo_resources.sh all --env local
```
