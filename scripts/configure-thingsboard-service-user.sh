#!/usr/bin/env bash
set -Eeuo pipefail
umask 077
for c in curl jq openssl; do command -v "$c" >/dev/null || { echo "Missing command: $c" >&2; exit 1; }; done
for v in TB_BASE_URL TB_USERNAME TB_PASSWORD TB_DEVICE_ID TB_CONFIRMED_DEVICE_ID TB_CONFIRMED_CUSTOMER_ID TB_CONFIRMED_CUSTOMER_ISOLATED TB_SERVICE_USER_EMAIL TB_SERVICE_USER_CREATE_URL; do [[ -n ${!v:-} ]] || { echo "Missing variable: $v" >&2; exit 1; }; done
[[ $TB_DEVICE_ID == "$TB_CONFIRMED_DEVICE_ID" ]] || { echo "Refusing ThingsBoard action: device confirmation mismatch" >&2; exit 1; }
[[ ${TB_WRITEBACK_ENABLED:-false} == false && ${TB_ALARMS_ENABLED:-false} == false ]] || { echo "Refusing: writeback and alarms must remain disabled" >&2; exit 1; }
[[ ${TB_CONFIRMED_CUSTOMER_ISOLATED} == true ]] || { echo "Refusing: customer isolation must be explicitly confirmed" >&2; exit 1; }
[[ -n ${TB_CONFIRMED_CUSTOMER_DEVICE_COUNT:-} && ${TB_CONFIRMED_CUSTOMER_DEVICE_COUNT} == 1 ]] || { echo "Refusing: customer device count must be explicitly confirmed as 1" >&2; exit 1; }
ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
secret_file=${TRENDX_CREDENTIALS_FILE:-.secrets/service-accounts.env}; [[ $secret_file = /* ]] || secret_file="$ROOT_DIR/$secret_file"; mkdir -p "$(dirname "$secret_file")"; touch "$secret_file"; chmod 600 "$secret_file"
tmp=$(mktemp); trap 'rm -f "$tmp"' EXIT; token=$(curl -fsS -H 'Content-Type: application/json' -d "$(jq -cn --arg u "$TB_USERNAME" --arg p "$TB_PASSWORD" '{username:$u,password:$p}')" "$TB_BASE_URL/api/auth/login" | jq -r .token)
[[ -n $token && $token != null ]] || { echo "ThingsBoard authentication returned no token" >&2; exit 1; }
device=$(curl -fsS -H "X-Authorization: Bearer $token" "$TB_BASE_URL/api/device/info/$TB_DEVICE_ID")
customer_id=$(jq -r '.customerId?.id // .customerId // empty' <<<"$device")
[[ $customer_id == "$TB_CONFIRMED_CUSTOMER_ID" ]] || { echo "Refusing: device customer does not match confirmation" >&2; exit 1; }
[[ -n ${TB_SERVICE_USER_PASSWORD:-} ]] || TB_SERVICE_USER_PASSWORD=$(openssl rand -base64 36 | tr -dc 'A-Za-z0-9_@%+=' | cut -c1-32)
body=$(jq -cn --arg e "$TB_SERVICE_USER_EMAIL" --arg p "$TB_SERVICE_USER_PASSWORD" '{email:$e,password:$p,authority:"CUSTOMER_USER"}')
curl -fsS -H "X-Authorization: Bearer $token" -H 'Content-Type: application/json' -d "$body" "$TB_SERVICE_USER_CREATE_URL" >/dev/null
if ! grep -q '^TB_SERVICE_USER_PASSWORD=' "$secret_file"; then printf 'TB_SERVICE_USER_PASSWORD=%q\n' "$TB_SERVICE_USER_PASSWORD" >>"$secret_file"; fi
echo "ThingsBoard customer user prepared for an explicitly isolated customer; no telemetry or alarm call was made."
