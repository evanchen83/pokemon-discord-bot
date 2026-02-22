#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Import watsonx Orchestrate resources using credentials from .env.

Usage:
  scripts/import_wxo_from_env.sh [all|tools|kb|agent] [options]

Modes:
  all      Import tools, knowledge base, then agent (default)
  tools    Import tool definitions only
  kb       Import knowledge base only
  agent    Import agent only

Options:
  --env <name>      ADK environment name to activate before import
  --env-file <path> Path to env file (default: .env)
  -h, --help        Show this help

Examples:
  scripts/import_wxo_from_env.sh
  scripts/import_wxo_from_env.sh all --env dev
  scripts/import_wxo_from_env.sh tools
EOF
}

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODE="all"
ENV_FILE="$ROOT_DIR/.env"
ADK_ENV_NAME=""
PASSTHROUGH_ARGS=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    all|tools|kb|agent)
      MODE="$1"
      shift
      ;;
    --env)
      ADK_ENV_NAME="${2:-}"
      shift 2
      ;;
    --env-file)
      ENV_FILE="${2:-}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      PASSTHROUGH_ARGS+=("$1")
      shift
      ;;
  esac
done

if [[ ! -f "$ENV_FILE" ]]; then
  echo "Env file not found: $ENV_FILE" >&2
  exit 1
fi

set -a
# shellcheck disable=SC1090
source "$ENV_FILE"
set +a

INSTANCE_URL="${WO_INSTANCE:-}"
API_KEY="${WO_API_KEY:-}"

if [[ -z "${INSTANCE_URL}" ]]; then
  echo "Missing WO_INSTANCE in env file: $ENV_FILE" >&2
  exit 1
fi
if [[ -z "${API_KEY}" ]]; then
  echo "Missing WO_API_KEY in env file: $ENV_FILE" >&2
  exit 1
fi

if [[ -z "$ADK_ENV_NAME" ]]; then
  ADK_ENV_NAME="${WO_ENV:-local}"
fi

IMPORT_SCRIPT="$ROOT_DIR/scripts/import_wxo_resources.sh"
if [[ ! -f "$IMPORT_SCRIPT" ]]; then
  echo "Import script not found: $IMPORT_SCRIPT" >&2
  exit 1
fi

CMD=("$IMPORT_SCRIPT" "$MODE")
if [[ "${#PASSTHROUGH_ARGS[@]}" -gt 0 ]]; then
  CMD+=("${PASSTHROUGH_ARGS[@]}")
fi

echo "Using env file: $ENV_FILE"
echo "Target instance: $INSTANCE_URL"
echo "ADK environment: $ADK_ENV_NAME"

if command -v orchestrate >/dev/null 2>&1; then
  CLI=(orchestrate)
else
  CLI=(python3 -m ibm_watsonx_orchestrate.cli.main)
fi

echo "Activating ADK environment..."
"${CLI[@]}" env activate "$ADK_ENV_NAME"
echo "Environment activated."

"${CMD[@]}"
