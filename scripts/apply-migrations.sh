#!/usr/bin/env bash
# =============================================================================
# Trendx — Canonical SQL migration runner (full discovered chain)
# -----------------------------------------------------------------------------
# Single source of execution for the tracked migration chain.
#
#   * Discovers migrations/migrations/NNN_*.sql in strict numeric order.
#   * Default mode is --check (read-only, no writes).
#   * --apply is the ONLY mode that writes, and only when TRENDX_CONFIRM_APPLY
#     is an explicit, valid value (YES|yes|true|1|VALID). Its value is never
#     printed.
#   * --only 014 restricts check/apply to migration 014 alone (014-only
#     contract): any other value, several selections, or a missing 014 file
#     is REFUSED. Only the 014 ledger row is ever recorded — 000..013 rows
#     are never fabricated. Bare --apply (no --only) on a ledgerless
#     NON-EMPTY database is REFUSED (unknown history); on an EMPTY database
#     the historical fresh-bootstrap full-chain behaviour is preserved.
#   * Target database is a single PostgreSQL database (trendx). No \connect to
#     separate databases, no separate role/password provisioning.
#   * Each migration runs atomically:
#       - migrations that manage their own transaction (top-level BEGIN;..COMMIT;)
#         run with ON_ERROR_STOP=1 and the file's own transaction;
#       - all others run wrapped in `psql -1` (single transaction).
#     The ledger write is part of the same transaction (or, for self-managed
#     migrations, recorded immediately after success) so a failure rolls back
#     and never leaves a half-applied migration marked OK.
#   * Ledger: trendx_catalog.schema_version (migration_name PK, applied_at,
#     checksum, execution_seconds, status). The ledger lives in the hardened
#     application schema on purpose: production databases drop the public
#     schema, so a public ledger can neither be read nor created there.
#     Installs with a legacy public.schema_version ledger are out of scope
#     for automatic handling (ambiguous history — explicit decision required).
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
ONLY=""        # 3-digit migration number requested via --only (single selection).
ONLY_COUNT=0   # More than one --only selection is always refused.
while (($#)); do
  case "$1" in
    --check)
      [[ -z "$MODE" ]] || die "a single mode is required (--check or --apply)"
      MODE="check"; shift ;;
    --apply)
      [[ -z "$MODE" ]] || die "a single mode is required (--check or --apply)"
      MODE="apply"; shift ;;
    --only)
      ONLY_COUNT=$((ONLY_COUNT + 1))
      [[ "$ONLY_COUNT" -eq 1 ]] || die "REFUSED: --only accepts exactly one migration (got several --only selections)"
      [[ -n "${2:-}" ]] || die "REFUSED: --only requires a 3-digit migration number (got nothing)"
      ONLY="$2"; shift 2 ;;
    --only=*)
      ONLY_COUNT=$((ONLY_COUNT + 1))
      [[ "$ONLY_COUNT" -eq 1 ]] || die "REFUSED: --only accepts exactly one migration (got several --only selections)"
      ONLY="${1#--only=}"; shift ;;
    ""|-h|--help)
      cat >&2 <<'USAGE'
Usage: apply-migrations.sh [--check|--apply] [--only NNN]

  --check   (default) Validate connection and list migrations that would run.
            Performs NO write, NO schema change, NO file change.
  --apply   Apply migrations. Requires TRENDX_CONFIRM_APPLY to be set
            to a valid value (YES|yes|true|1|VALID). Refuses any other value or
            absence of the variable.
  --only NNN
            Restrict the operation to a single migration. The ONLY supported
            selection is --only 014 (scheduler B1 runs/heartbeat). Any other
            value, several selections, or a missing 014 file is REFUSED.
            With --check: prints the 014-only plan, never writes.
            With --apply: executes 014 only (same atomicity + ledger as the
            chain path) and records ONLY 014 — 000..013 rows are never
            fabricated. Applying without --only on a ledgerless NON-EMPTY
            database is REFUSED (unknown history); on an EMPTY database the
            historical full-chain bootstrap behaviour is preserved.

Connection is taken from the libpq environment (PGHOST, PGPORT, PGUSER,
PGDATABASE). No credential is read from this script.
USAGE
      exit 0
      ;;
    *)
      die "unknown argument: $1 (use --check or --apply, optionally with --only 014)"
      ;;
  esac
done
[[ -n "$MODE" ]] || die "a mode is required: --check or --apply (optionally with --only 014)"

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

# --- --only selection (014-only contract) ------------------------------------
# Only --only 014 is supported. Anything else (013, 015, 000, several
# selections, non-numeric) is REFUSED — never silently reinterpreted.
ONLY_FILE=""
if [[ -n "$ONLY" ]]; then
  [[ "$ONLY" =~ ^[0-9]{3}$ ]] || die "REFUSED: --only expects a 3-digit migration number (got '$ONLY')"
  [[ "$ONLY" == "014" ]] || die "REFUSED: the only supported single-migration selection is --only 014 (got --only $ONLY)"
  mapfile -t _only_matches < <(find "$MIGRATIONS_DIR" -maxdepth 1 -name '014_*.sql' | LC_ALL=C sort)
  ((${#_only_matches[@]} == 1)) || die "REFUSED: migration 014 file missing or ambiguous in $MIGRATIONS_DIR"
  ONLY_FILE="${_only_matches[0]}"
  TARGETS=("$ONLY_FILE")
else
  TARGETS=("${MIGRATIONS[@]}")
fi

# --- Helpers ----------------------------------------------------------------
checksum_of() {
  # Stable sha256 of the migration file content (no path, no metadata).
  sha256sum "$1" | awk '{print $1}'
}

pg_reachable() {
  # Read-only probe. No schema/data change.
  "${PSQL[@]}" -c "SELECT 1" >/dev/null 2>&1
}

# --- Ledger location (single source) -----------------------------------------
# All ledger SQL below must use $LEDGER_TABLE. Never inline another schema.
LEDGER_SCHEMA="trendx_catalog"
LEDGER_TABLE="${LEDGER_SCHEMA}.schema_version"

ledger_ok() {
  # Echo "1" if the migration is recorded as OK, else "0". Tolerates a missing
  # ledger table (returns 0). Never creates the table.
  local name="$1"
  local res
  res=$("${PSQL[@]}" -c "SELECT 1 FROM ${LEDGER_TABLE} WHERE migration_name = '$name' AND status = 'OK' LIMIT 1" 2>/dev/null) || true
  [[ "$res" == "1" ]] && echo 1 || echo 0
}

ledger_exists() {
  # True iff the ledger table exists (any content). Read-only probe.
  "${PSQL[@]}" -c "SELECT 1 FROM ${LEDGER_TABLE} LIMIT 1" >/dev/null 2>&1
}

db_has_user_objects() {
  # Echo a count of user objects evidencing an unknown migration history:
  # tables outside system schemas PLUS user schemas (a schema-only database
  # already carries history — e.g. trendx_catalog without a ledger). A fresh
  # bootstrap database reports 0 (stock public is excluded and empty).
  # Read-only probe; echoes 0 when unreachable (callers check reachability).
  local n
  n=$("${PSQL[@]}" -c "SELECT (SELECT count(*) FROM pg_tables WHERE schemaname NOT IN ('pg_catalog','information_schema')) + (SELECT count(*) FROM pg_namespace WHERE nspname NOT IN ('pg_catalog','information_schema','pg_toast','public'))" 2>/dev/null) || { echo 0; return 0; }
  echo "$n"
}

trace_only_selection() {
  # Explicit traceability for --only runs: what runs, what is excluded, why,
  # and the honesty status of the ledger. Never prints secrets (the selection
  # is a caller-supplied 3-digit number, validated before this point).
  local name
  name=$(basename "$ONLY_FILE")
  log "014-only selection: selected=$name excluded=000..013 reason='explicit --only 014'"
  if ! ledger_exists; then
    log "ledger ABSENT: history of 000..013 is UNKNOWN — not recorded, not fabricated"
  fi
}

ledger_schema_exists() {
  # True iff the ledger schema exists. Unlike ledger_exists (missing TABLE
  # raises a psql error), a missing SCHEMA still exits 0 with zero rows here,
  # so the output content — not the exit code — decides.
  local res
  res=$("${PSQL[@]}" -c "SELECT 1 FROM pg_namespace WHERE nspname = '${LEDGER_SCHEMA}' LIMIT 1" 2>/dev/null) || return 1
  [[ "$res" == "1" ]]
}

ensure_ledger() {
  # Create the ledger (idempotent) in the hardened application schema.
  # Only ever called in --apply mode. The schema itself is created only when
  # missing: PostgreSQL checks the database CREATE privilege even for
  # IF NOT EXISTS on an existing schema, so an unconditional CREATE SCHEMA
  # would break least-privilege apply roles on hardened databases.
  # Afterwards grants read access to the application role (IF EXISTS,
  # same conditional pattern as the migrations) so future --check runs work
  # under least privilege. Reached only after successful CREATEs, hence as an
  # owner — a GRANT failure surfaces loudly (misconfiguration) instead of hiding.
  if ! ledger_schema_exists; then
    "${PSQL[@]}" -c "CREATE SCHEMA ${LEDGER_SCHEMA}" >/dev/null
  fi
  "${PSQL[@]}" -c "CREATE TABLE IF NOT EXISTS ${LEDGER_TABLE} (
      migration_name    TEXT PRIMARY KEY,
      applied_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      checksum          TEXT,
      execution_seconds INTEGER,
      status            TEXT NOT NULL DEFAULT 'OK'
  )" >/dev/null
  "${PSQL[@]}" -c "DO \$\$
      BEGIN
        IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'trendx_app') THEN
          GRANT SELECT ON ${LEDGER_TABLE} TO trendx_app;
        END IF;
      END;
  \$\$;" >/dev/null
}

record_ok() {
  # Record / refresh the ledger row. Called inside --apply after a successful
  # migration. Safe to call repeatedly (ON CONFLICT DO UPDATE).
  local name="$1" checksum="$2"
  "${PSQL[@]}" -c "INSERT INTO ${LEDGER_TABLE} (migration_name, checksum, status)
                   VALUES ('$name', '$checksum', 'OK')
                   ON CONFLICT (migration_name) DO UPDATE
                     SET applied_at = NOW(), checksum = EXCLUDED.checksum, status = 'OK'" >/dev/null
}

record_duration() {
  local name="$1" seconds="$2"
  "${PSQL[@]}" -c "UPDATE ${LEDGER_TABLE} SET execution_seconds = $seconds WHERE migration_name = '$name'" >/dev/null 2>&1 || true
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
      printf 'INSERT INTO %s (migration_name, checksum, status)\n' "$LEDGER_TABLE"
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
  if [[ -n "$ONLY_FILE" ]]; then
    trace_only_selection
    echo "Migrations that would be applied (--only 014):"
  else
    echo "Migrations that would be applied (full chain):"
  fi
  for f in "${TARGETS[@]}"; do
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

if [[ -n "$ONLY_FILE" ]]; then
  trace_only_selection
else
  # Bare --apply without an explicit selection on a ledgerless NON-EMPTY
  # database would blindly replay the whole chain over an unknown history:
  # REFUSE. On an EMPTY database the historical fresh-bootstrap behaviour is
  # preserved; with a valid ledger the per-migration skip logic below applies.
  if ! ledger_exists; then
    if [[ "$(db_has_user_objects)" != "0" ]]; then
      printf '[trendx-migrate] REFUSED: ledger %s is absent but the database already contains objects — migration history is UNKNOWN.\n' "$LEDGER_TABLE" >&2
      printf '[trendx-migrate] Refusing blind full-chain replay. Use an explicit --only selection (only --only 014 is supported) or restore a valid ledger.\n' >&2
      exit 1
    fi
    log "ledger absent on an EMPTY database — fresh-bootstrap full-chain apply."
  fi
fi

ensure_ledger

TMPERR=$(mktemp)
trap 'rm -f "$TMPERR"' EXIT

applied=0
skipped=0
for f in "${TARGETS[@]}"; do
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
