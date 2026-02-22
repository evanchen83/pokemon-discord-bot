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
  --no-activate     Skip ADK env activation even if --env / WO_ENV is set
  -h, --help        Show this help

Examples:
  scripts/import_wxo_from_env.sh
  scripts/import_wxo_from_env.sh all --env dev
  scripts/import_wxo_from_env.sh tools --no-activate
EOF
}

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODE="all"
ENV_FILE="$ROOT_DIR/.env"
ADK_ENV_NAME=""
NO_ACTIVATE="false"
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
    --no-activate)
      NO_ACTIVATE="true"
      shift
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
if [[ "$NO_ACTIVATE" != "true" && -n "$ADK_ENV_NAME" ]]; then
  CMD+=("--env" "$ADK_ENV_NAME")
fi
if [[ "${#PASSTHROUGH_ARGS[@]}" -gt 0 ]]; then
  CMD+=("${PASSTHROUGH_ARGS[@]}")
fi

echo "Using env file: $ENV_FILE"
echo "Target instance: $INSTANCE_URL"
if [[ "$NO_ACTIVATE" == "true" ]]; then
  echo "ADK environment activation: skipped"
elif [[ -n "$ADK_ENV_NAME" ]]; then
  echo "ADK environment: $ADK_ENV_NAME"
else
  echo "ADK environment: not set (using default: local)"
fi

"${CMD[@]}"
