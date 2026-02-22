#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Import/update watsonx Orchestrate resources for this project.

Usage:
  scripts/import_wxo_resources.sh [all|tools|kb|agent] [options]

Modes:
  all      Import tools, knowledge base, then agent (default)
  tools    Import tool definitions only
  kb       Import knowledge base only
  agent    Import agent only

Options:
  --env <name>          Activate an existing ADK environment before import
  --tool-file <path>    Override tool file path
  --kb-file <path>      Override knowledge base manifest path
  --agent-file <path>   Override agent manifest path
  -h, --help            Show this help

Examples:
  scripts/import_wxo_resources.sh
  scripts/import_wxo_resources.sh tools
  scripts/import_wxo_resources.sh agent --env local
EOF
}

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODE="all"
ENV_NAME=""
TOOL_FILE="$ROOT_DIR/deploy/wxo/tools/pokemon_tcg_stats_tools.py"
KB_FILE="$ROOT_DIR/deploy/wxo/knowledge-bases/pokemon-tcg-kb.yaml"
AGENT_FILE="$ROOT_DIR/deploy/wxo/agents/pokemon-tcg-agent.yaml"
REQ_FILE=""
PACKAGE_ROOT="$ROOT_DIR/deploy/wxo/tools"
TEMP_REQ_FILE=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    all|tools|kb|agent)
      MODE="$1"
      shift
      ;;
    --env)
      ENV_NAME="${2:-}"
      shift 2
      ;;
    --tool-file)
      TOOL_FILE="${2:-}"
      shift 2
      ;;
    --kb-file)
      KB_FILE="${2:-}"
      shift 2
      ;;
    --agent-file)
      AGENT_FILE="${2:-}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage
      exit 2
      ;;
  esac
done

if command -v orchestrate >/dev/null 2>&1; then
  CLI=(orchestrate)
else
  CLI=(python3 -m ibm_watsonx_orchestrate.cli.main)
fi

run_cli() {
  "${CLI[@]}" "$@"
}

cleanup() {
  if [[ -n "$TEMP_REQ_FILE" && -f "$TEMP_REQ_FILE" ]]; then
    rm -f "$TEMP_REQ_FILE"
  fi
}
trap cleanup EXIT

check_file() {
  local f="$1"
  if [[ ! -f "$f" ]]; then
    echo "File not found: $f" >&2
    exit 1
  fi
}

import_tools() {
  check_file "$TOOL_FILE"
  if command -v uv >/dev/null 2>&1 && [[ -f "$ROOT_DIR/pyproject.toml" && -f "$ROOT_DIR/uv.lock" ]]; then
    TEMP_REQ_FILE="$(mktemp)"
    (
      cd "$ROOT_DIR"
      uv export --format requirements-txt --no-dev --no-hashes > "$TEMP_REQ_FILE"
    )
    REQ_FILE="$TEMP_REQ_FILE"
  elif [[ -f "$ROOT_DIR/requirements.txt" ]]; then
    REQ_FILE="$ROOT_DIR/requirements.txt"
  else
    echo "No dependency manifest found for tool import." >&2
    echo "Install uv and ensure pyproject.toml + uv.lock are present, or provide requirements.txt." >&2
    exit 1
  fi
  check_file "$REQ_FILE"
  echo "Importing tools from: $TOOL_FILE"
  run_cli tools import -k python -f "$TOOL_FILE" -r "$REQ_FILE" -p "$PACKAGE_ROOT"
}

import_kb() {
  check_file "$KB_FILE"
  echo "Importing knowledge base from: $KB_FILE"
  run_cli knowledge-bases import -f "$KB_FILE"
}

import_agent() {
  check_file "$AGENT_FILE"
  echo "Importing agent from: $AGENT_FILE"
  run_cli agents import -f "$AGENT_FILE"
}

if [[ -n "$ENV_NAME" ]]; then
  echo "Activating ADK environment: $ENV_NAME"
  run_cli env activate "$ENV_NAME"
fi

echo "Using CLI: ${CLI[*]}"

case "$MODE" in
  all)
    import_tools
    import_kb
    import_agent
    ;;
  tools)
    import_tools
    ;;
  kb)
    import_kb
    ;;
  agent)
    import_agent
    ;;
esac

echo "Import complete."
