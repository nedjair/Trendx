#!/usr/bin/env bash
# =============================================================================
# Trendx — Canonical SQL migration runner (000 -> 012)
# -----------------------------------------------------------------------------
# Single source of execution for the tracked migration chain.
#
#   * Discovers migrations/migrations/NNN_*.sql in strict numeric order.
#   * Default mode is --check (read-only, no writes).
#   * --apply is the ONLY mode that writes, and only when TRENDX_CONFIRM_APPLY
#     is an explicit, valid value (YES|yes|true|1|VALID). Its value is never
#     printed.
#   * Target database is a single PostgreSQL database (trendx). No \connect to
#     separate databases, no separate role/password provisioning.
#   * Each migration runs atomically:
#       - migrations that manage their own transaction (top-level BEGIN;..COMMIT;)
#         run with ON_ERROR_STOP=1 and the file's own transaction;
#       - all others run wrapped in `psql -1` (single transaction).
#     The ledger write is part of the same transaction (or, for self-managed
#     migrations, recorded immediately after success) so a failure rolls back
#     and never leaves a half-applied migration marked OK.
#   * Ledger: public.schema_version (migration_name PK, applied_at, checksum,
#     execution_seconds, status). Matches docs/deployment.md §13.
#
# Security:
#   * Never echoes PGPASSWORD, a full DSN, tokens, or any credential.
#   * Never reads/sources .env to fabricate a confirmation.
#   * Never performs destructive operations (DROP DATABASE / reset / down-clean).
#
# Connection: libpq environment only (PGHOST, PGPORT, PGUSER, PGDATABASE).
# No credential is ever hardcoded in this file.
# =============================================================================
set -Eeuo pipefail

SCRIPT_DIR=$(unset CDPATH; cd -- "$(dirname -- "$0")" && pwd)
ROOT_DIR=$(unset CDPATH; cd -- "$SCRIPT_DIR/.." && pwd)
MIGRATIONS_DIR="$ROOT_DIR/migrations"

# --- Logging (stderr for errors, stdout for informational output) -----------
log()  { printf '[trendx-migrate] %s\n' "$*" >&2; }
die()  { printf '[trendx-migrate] ERROR: %s\n' "$*" >&2; exit 1; }

MODE=""
case "${1:-}" in
  --check) MODE="check" ;;
  --apply) MODE="apply" ;;
  ""|-h|--help)
    cat >&2 <<'USAGE'
Usage: apply-migrations.sh [--check|--apply]

  --check   (default) Validate connection and list migrations that would run.
            Performs NO write, NO schema change, NO file change.
  --apply   Apply the migration chain. Requires TRENDX_CONFIRM_APPLY to be set
            to a valid value (YES|yes|true|1|VALID). Refuses any other value or
            absence of the variable.

Connection is taken from the libpq environment (PGHOST, PGPORT, PGUSER,
PGDATABASE). No credential is read from this script.
USAGE
    exit 0
    ;;
  *)
    die "unknown argument: ${1:-} (use --check or --apply)"
    ;;
esac

# --- Connection parameters (libpq standard) --------------------------------
PGHOST="${PGHOST:-}"
PGPORT="${PGPORT:-5432}"
PGUSER="${PGUSER:-}"
PGDATABASE="${PGDATABASE:-trendx}"

[[ -n "$PGHOST" ]]    || die "PGHOST is not set (libpq connection parameter required)"
[[ -n "$PGUSER" ]]    || die "PGUSER is not set (libpq connection parameter required)"
[[ -n "$PGDATABASE" ]] || die "PGDATABASE is not set (libpq connection parameter required)"

# psql is used for every operation. It reads PGPASSWORD from the environment if
# present; we never print it. We pass connection parameters explicitly so that
# no DSN is ever built or echoed by this script.
PSQL=(psql -v ON_ERROR_STOP=1 -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" --no-psqlrc --no-align --tuples-only)

# --- Discover migrations ----------------------------------------------------
[[ -d "$MIGRATIONS_DIR" ]] || die "migrations directory not found: $MIGRATIONS_DIR"

mapfile -t MIGRATIONS < <(find "$MIGRATIONS_DIR" -maxdepth 1 -name '[0-9][0-9][0-9]_*.sql' | LC_ALL=C sort)
((${#MIGRATIONS[@]})) || die "no migrations found in $MIGRATIONS_DIR"

# Verify the expected 000..012 chain is complete (no missing number).
expected=()
for n in $(seq 0 12); do
  printf -v nn "%03d" "$n"
  expected+=("$nn")
done
declare -A have=()
for f in "${MIGRATIONS[@]}"; do
  base=$(basename "$f")
  num=${base:0:3}
  have["$num"]=1
done
missing=()
for n in "${expected[@]}"; do
  [[ -n "${have[$n]:-}" ]] || missing+=("$n")
done
((${#missing[@]} == 0)) || die "missing migration number(s) in 000..012: ${missing[*]}"

# --- Helpers ----------------------------------------------------------------
checksum_of() {
  # Stable sha256 of the migration file content (no path, no metadata).
  sha256sum "$1" | awk '{print $1}'
}

pg_reachable() {
  # Read-only probe. No schema/data change.
  "${PSQL[@]}" -c "SELECT 1" >/dev/null 2>&1
}

ledger_ok() {
  # Echo "1" if the migration is recorded as OK, else "0". Tolerates a missing
  # ledger table (returns 0). Never creates the table.
  local name="$1"
  local res
  res=$("${PSQL[@]}" -c "SELECT 1 FROM public.schema_version WHERE migration_name = '$name' AND status = 'OK' LIMIT 1" 2>/dev/null) || true
  [[ "$res" == "1" ]] && echo 1 || echo 0
}

ensure_ledger() {
  # Create the ledger (idempotent). Only ever called in --apply mode.
  "${PSQL[@]}" -c "CREATE TABLE IF NOT EXISTS public.schema_version (
      migration_name    TEXT PRIMARY KEY,
      applied_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      checksum          TEXT,
      execution_seconds INTEGER,
      status            TEXT NOT NULL DEFAULT 'OK'
  )" >/dev/null
}

record_ok() {
  # Record / refresh the ledger row. Called inside --apply after a successful
  # migration. Safe to call repeatedly (ON CONFLICT DO UPDATE).
  local name="$1" checksum="$2"
  "${PSQL[@]}" -c "INSERT INTO public.schema_version (migration_name, checksum, status)
                   VALUES ('$name', '$checksum', 'OK')
                   ON CONFLICT (migration_name) DO UPDATE
                     SET applied_at = NOW(), checksum = EXCLUDED.checksum, status = 'OK'" >/dev/null
}

record_duration() {
  local name="$1" seconds="$2"
  "${PSQL[@]}" -c "UPDATE public.schema_version SET execution_seconds = $seconds WHERE migration_name = '$name'" >/dev/null 2>&1 || true
}

migration_manages_own_tx() {
  # Trendx migrations 004 (and any migration with an explicit top-level
  # BEGIN;..COMMIT;) manage their own transaction. Wrapping those in `psql -1`
  # would raise "BEGIN is not allowed in a transaction block", so we run them
  # as-is with ON_ERROR_STOP=1 (the file's own transaction guarantees atomicity)
  # and record the ledger separately afterwards.
  grep -Eq '^[[:space:]]*(BEGIN|START[[:space:]]+TRANSACTION)[[:space:]]*;' "$1"
}

apply_migration() {
  # NOTE: `start` is intentionally NOT declared local here — it is set by the
  # caller (the apply loop) immediately before this function is invoked, and is
  # used to compute the migration's duration. Redeclaring it local would shadow
  # the caller's value and, under `set -u`, leave it unbound for the duration
  # calculation.
  local file="$1" name="$2" checksum="$3" end dur
  if migration_manages_own_tx "$file"; then
    # Self-managed transaction: do not wrap in psql -1.
    if ! "${PSQL[@]}" -f "$file" >/dev/null 2>"${TMPERR}"; then
      cat "${TMPERR}" >&2 || true
      die "migration FAILED (rolled back by its own transaction): $name"
    fi
    record_ok "$name" "$checksum"
  else
    # Wrapped in a single transaction together with the ledger write, so a
    # failure rolls back both the migration and its ledger row.
    local combined
    combined=$(mktemp)
    {
      printf "\\set ON_ERROR_STOP on\n"
      printf "\\i %s\n" "$file"
      printf "INSERT INTO public.schema_version (migration_name, checksum, status)\n"
      printf "VALUES ('%s', '%s', 'OK')\n" "$name" "$checksum"
      printf "ON CONFLICT (migration_name) DO UPDATE\n"
      printf "  SET applied_at = NOW(), checksum = EXCLUDED.checksum, status = 'OK';\n"
    } >"$combined"
    if ! "${PSQL[@]}" -1 -f "$combined" >/dev/null 2>"${TMPERR}"; then
      cat "${TMPERR}" >&2 || true
      rm -f "$combined"
      die "migration FAILED (transaction rolled back): $name"
    fi
    rm -f "$combined"
  fi
  end=$(date +%s)
  dur=$((end - start))
  record_duration "$name" "$dur"
}

# --- Mode: --check ----------------------------------------------------------
if [[ "$MODE" == "check" ]]; then
  log "mode=check (read-only — no write will be performed)"
  log "target database: $PGDATABASE  (host=$PGHOST port=$PGPORT user=$PGUSER)"
  pg_reachable || die "PostgreSQL is not reachable at $PGHOST:$PGPORT (db=$PGDATABASE)"
  log "PostgreSQL reachable."
  echo "Migrations that would be applied (chain 000 -> 012):"
  for f in "${MIGRATIONS[@]}"; do
    name=$(basename "$f")
    if [[ "$(ledger_ok "$name")" == "1" ]]; then
      echo "  [skip]   $name   (already recorded OK)"
    else
      echo "  [apply]  $name"
    fi
  done
  log "check complete — no changes made."
  exit 0
fi

# --- Mode: --apply ----------------------------------------------------------
# Confirmation guard: explicit and valid only.
CONFIRM="${TRENDX_CONFIRM_APPLY:-}"
valid=0
for v in YES yes true 1 VALID; do
  [[ "$CONFIRM" == "$v" ]] && valid=1
done
if [[ "$valid" -ne 1 ]]; then
  printf '[trendx-migrate] BLOCKED — TRENDX_CONFIRM_APPLY ABSENT/INVALIDE\n' >&2
  printf '[trendx-migrate] set TRENDX_CONFIRM_APPLY to one of: YES yes true 1 VALID\n' >&2
  exit 1
fi

log "mode=apply (writes enabled — confirmed)"
log "target database: $PGDATABASE  (host=$PGHOST port=$PGPORT user=$PGUSER)"
pg_reachable || die "PostgreSQL is not reachable at $PGHOST:$PGPORT (db=$PGDATABASE)"

ensure_ledger

TMPERR=$(mktemp)
trap 'rm -f "$TMPERR"' EXIT

applied=0
skipped=0
for f in "${MIGRATIONS[@]}"; do
  name=$(basename "$f")
  checksum=$(checksum_of "$f")
  if [[ "$(ledger_ok "$name")" == "1" ]]; then
    log "[skip] $name (already OK)"
    skipped=$((skipped + 1))
    continue
  fi
  log "[apply] $name"
  start=$(date +%s)
  apply_migration "$f" "$name" "$checksum"
  log "[ok]    $name"
  applied=$((applied + 1))
done

log "apply complete — applied=$applied skipped=$skipped total=${#MIGRATIONS[@]}"
exit 0
