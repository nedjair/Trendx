#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "${SCRIPT_DIR}")"

ENV_FILE="${PROJECT_DIR}/.env"

if [ ! -f "${ENV_FILE}" ]; then
  echo "ERROR: .env file not found at ${ENV_FILE}" >&2
  exit 1
fi

set -a
# SC1090 nécessaire : chargement dynamique du .env local (chemin construit, jamais distant).
# shellcheck disable=SC1090
source "${ENV_FILE}"
set +a

export THINGSBOARD_URL="${TB_BASE_URL:-http://10.0.0.1:8081}"
export THINGSBOARD_USERNAME="${TB_USERNAME:-}"
export THINGSBOARD_PASSWORD="${TB_PASSWORD:-}"

exec docker run -i --rm \
  -e THINGSBOARD_URL="${THINGSBOARD_URL}" \
  -e THINGSBOARD_USERNAME="${THINGSBOARD_USERNAME}" \
  -e THINGSBOARD_PASSWORD="${THINGSBOARD_PASSWORD}" \
  thingsboard/mcp
