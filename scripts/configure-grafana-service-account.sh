#!/usr/bin/env bash
set -Eeuo pipefail
umask 077
require() { command -v "$1" >/dev/null || { echo "Missing command: $1" >&2; exit 1; }; }
for c in curl jq; do require "$c"; done
for v in GRAFANA_URL GRAFANA_ADMIN_USER GRAFANA_ADMIN_PASSWORD GRAFANA_DATASOURCE_NAME GRAFANA_SERVICE_ACCOUNT_NAME; do [[ -n ${!v:-} ]] || { echo "Missing variable: $v" >&2; exit 1; }; done
# SC1007 volontaire : préfixe CDPATH= vide neutralisant CDPATH pour cd (portabilité POSIX).
# shellcheck disable=SC1007
ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
token_file=${GRAFANA_TOKEN_FILE:-.secrets/grafana-trendx.token}; [[ $token_file = /* ]] || token_file="$ROOT_DIR/$token_file"
mkdir -p "$(dirname "$token_file")"; chmod 700 "$(dirname "$token_file")"
auth_file=$(mktemp); response=$(mktemp); trap 'rm -f "$auth_file" "$response"' EXIT; chmod 600 "$auth_file"
printf 'user = %s\npassword = %s\n' "$GRAFANA_ADMIN_USER" "$GRAFANA_ADMIN_PASSWORD" >"$auth_file"
gapi() { curl --config "$auth_file" -fsS -H 'Content-Type: application/json' "$@"; }
folder=$(gapi "$GRAFANA_URL/api/folders/uid/${GRAFANA_FOLDER_UID:-trendx}" 2>/dev/null || true)
[[ -n $folder ]] || { echo "Grafana folder absent; refusing before any mutation" >&2; exit 1; }
folder_uid=$(jq -r '.uid // empty' <<<"$folder")
[[ $folder_uid == "${GRAFANA_FOLDER_UID:-trendx}" ]] || { echo "Grafana folder response is invalid; refusing before mutation" >&2; exit 1; }
ds=$(gapi "$GRAFANA_URL/api/datasources/name/$(printf '%s' "$GRAFANA_DATASOURCE_NAME" | jq -sRr @uri)")
ds_id=$(jq -r '.id' <<<"$ds"); [[ $ds_id =~ ^[0-9]+$ ]] || { echo "Grafana datasource not found; no permissions changed" >&2; exit 1; }
sa=$(gapi "$GRAFANA_URL/api/serviceaccounts/search?query=$(printf '%s' "$GRAFANA_SERVICE_ACCOUNT_NAME" | jq -sRr @uri)")
sa_id=$(jq -r --arg n "$GRAFANA_SERVICE_ACCOUNT_NAME" '.serviceAccounts[]? | select(.name == $n) | .id' <<<"$sa" | head -n1)
if [[ -z $sa_id ]]; then sa_id=$(gapi -d "$(jq -cn --arg n "$GRAFANA_SERVICE_ACCOUNT_NAME" '{name:$n,role:"Editor"}')" "$GRAFANA_URL/api/serviceaccounts" | jq -r '.id'); fi
[[ $sa_id =~ ^[0-9]+$ ]] || { echo "Grafana service account id was not returned" >&2; exit 1; }
if [[ ! -s "$token_file" ]]; then
  token=$(gapi -d "$(jq -cn --arg n "trendx-bootstrap-$(date +%s)" '{name:$n,role:"Editor"}')" "$GRAFANA_URL/api/serviceaccounts/$sa_id/tokens" | jq -r '.key')
  [[ -n $token && $token != null ]] || { echo "Grafana token was not returned" >&2; exit 1; }
  printf '%s\n' "$token" >"$token_file"; chmod 600 "$token_file"
fi
gapi -X POST -d "$(jq -cn --argjson id "$sa_id" '[{userId:$id,permission:1}]')" "$GRAFANA_URL/api/datasources/id/$ds_id/permissions" >/dev/null
folder_permission_method=${GRAFANA_FOLDER_PERMISSION_METHOD:-PUT}
[[ $folder_permission_method == PUT || $folder_permission_method == POST ]] || { echo "Unsupported Grafana folder permission method" >&2; exit 1; }
gapi -X "$folder_permission_method" -d "$(jq -cn --argjson id "$sa_id" '[{userId:$id,permission:2}]')" "$GRAFANA_URL/api/folders/${GRAFANA_FOLDER_UID:-trendx}/permissions" >/dev/null
echo "Grafana service account configured; token stored in a mode-600 file."
