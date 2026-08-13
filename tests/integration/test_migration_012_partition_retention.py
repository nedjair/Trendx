"""Integration test for migration 012 (native partition retention — B5 Lot 2).

Applies against a REAL PostgreSQL instance (CI ``postgres:16-alpine`` service,
i.e. NATIVE PostgreSQL, NO TimescaleDB) and proves that migration 012 provides
a safe native retention mechanism based on ``DROP PARTITION`` (never row-by-row
DELETE) for the five partitioned parents:

    ts_kv, predictions, anomaly_scores, data_quality, ml_metrics

The retention function ``trendx_analytics.drop_old_partitions()`` is exercised
with REAL monthly partitions (old / recent / current / forward +3 months) and
asserts the exact threshold semantics, the +3-month forward-window guard, the
no-DEFAULT guard, idempotence, the non-superuser SECURITY DEFINER path, and
that INSERT routing still works after retention.

Tested against real PostgreSQL (NEVER SQLite / mock) so the DDL logic is real.

TimescaleDB scope
-----------------
Migration 012 is native-only. The test additionally asserts the migration file
does not reference TimescaleDB-specific constructs
(``create_hypertable`` / ``add_retention_policy``) and does not redefine
``ensure_partitions_forward()`` (owned by 004).

The migration is applied by this test itself (FRESH on top of the
chain already applied in the CI before_script: 001..006, 009, 010, 011) so the
test stays self-contained regardless of the CI migration glob.
"""

from __future__ import annotations

import os
from datetime import datetime

import psycopg2
import pytest

# Keep behaviour identical to the CI job (postgres:16-alpine service).
os.environ.setdefault("PG_ADMIN_HOST", "postgres")
os.environ.setdefault("PG_ADMIN_PORT", "5432")
os.environ.setdefault("PG_ADMIN_DB", "trendx")
os.environ.setdefault("PG_ADMIN_USER", "trendx_app")
os.environ.setdefault("PG_ADMIN_PASSWORD", "trendx_app_pass")
os.environ.setdefault("TRENDX_DB_HOST", "postgres")
os.environ.setdefault("TRENDX_DB_PORT", "5432")
os.environ.setdefault("TRENDX_DB_USER", "trendx_app")
os.environ.setdefault("TRENDX_DB_PASSWORD", "trendx_app_pass")

ROOT = __file__ and __import__("pathlib").Path(__file__).parents[2]
MIGRATIONS = ROOT / "migrations"

SCHEMA = "trendx_analytics"
pytestmark = [pytest.mark.integration]

PARENTS = ["ts_kv", "predictions", "anomaly_scores", "data_quality", "ml_metrics"]


def _pg_conn_params() -> dict[str, str]:
    return {
        "host": os.environ.get("PG_ADMIN_HOST", os.environ.get("TRENDX_DB_HOST", "postgres")),
        "port": int(os.environ.get("PG_ADMIN_PORT", os.environ.get("TRENDX_DB_PORT", "5432"))),
        "dbname": os.environ.get("PG_ADMIN_DB", os.environ.get("TRENDX_DB_NAME", "trendx")),
        "user": os.environ.get("PG_ADMIN_USER", os.environ.get("TRENDX_DB_USER", "trendx_app")),
        "password": os.environ.get(
            "PG_ADMIN_PASSWORD", os.environ.get("TRENDX_DB_PASSWORD", "trendx_app_pass")
        ),
    }


def _connect(params: dict[str, str]):
    return psycopg2.connect(
        host=params["host"],
        port=params["port"],
        dbname=params["dbname"],
        user=params["user"],
        password=params["password"],
    )


def _execute(params: dict[str, str], sql: str, args: tuple = ()) -> None:
    conn = _connect(params)
    try:
        with conn.cursor() as cur:
            cur.execute(sql, args)
        conn.commit()
    finally:
        conn.close()


def _execute_many(params: dict[str, str], sql: str) -> None:
    """Run a multi-statement SQL script (e.g. a migration file) in one go."""
    conn = _connect(params)
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
    finally:
        conn.close()


def _scalar(params: dict[str, str], sql: str, args: tuple = ()):
    conn = _connect(params)
    try:
        with conn.cursor() as cur:
            cur.execute(sql, args)
            row = cur.fetchone()
            # The migration / retention function performs DDL. psycopg2 opens a
            # transaction for the function call and would ROLLBACK on close()
            # if we don't commit, discarding the DDL. Commit unconditionally:
            # a no-op after a plain SELECT.
            conn.commit()
            return row[0] if row else None
    finally:
        conn.close()


def _function_exists(params: dict[str, str]) -> bool:
    return bool(
        _scalar(
            params,
            "SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace "
            "WHERE n.nspname=%s AND p.proname=%s",
            (SCHEMA, "drop_old_partitions"),
        )
    )


def _table_exists(params: dict[str, str], schema: str, table: str) -> bool:
    return bool(
        _scalar(
            params,
            "SELECT 1 FROM information_schema.tables WHERE table_schema=%s AND table_name=%s",
            (schema, table),
        )
    )


def _relkind(params: dict[str, str], schema: str, table: str) -> str | None:
    return _scalar(
        params,
        "SELECT c.relkind FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
        "WHERE n.nspname=%s AND c.relname=%s",
        (schema, table),
    )


def _partitions(params: dict[str, str], schema: str, table: str) -> list[str]:
    conn = _connect(params)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT c.relname FROM pg_inherits i "
                "JOIN pg_class p ON p.oid=i.inhparent "
                "JOIN pg_class c ON c.oid=i.inhrelid "
                "JOIN pg_namespace pn ON pn.oid=p.relnamespace "
                "WHERE p.relname=%s AND pn.nspname=%s ORDER BY c.relname",
                (table, schema),
            )
            return [r[0] for r in cur.fetchall()]
    finally:
        conn.close()


def _month_partition_name(table: str, d: datetime) -> str:
    return f"{table}_{d.year:04d}_{d.month:02d}"


def _add_months(d: datetime, months: int) -> datetime:
    m = d.month - 1 + months
    y = d.year + m // 12
    m = m % 12 + 1
    return d.replace(year=y, month=m)


def _create_partition(params: dict[str, str], parent: str, month: datetime) -> None:
    start = month.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    end = _add_months(start, 1)
    part_name = f"{parent}_{start.year:04d}_{start.month:02d}"
    # Partition name is derived from (parent, month) — never user input — so
    # plain string interpolation for the identifier is safe here; the bound
    # values use parameterized %s.
    _execute(
        params,
        "CREATE TABLE IF NOT EXISTS trendx_analytics."
        + part_name
        + " PARTITION OF trendx_analytics."
        + parent
        + " FOR VALUES FROM (%s) TO (%s)",
        (start, end),
    )
    # In production, monthly partitions are created by
    # ensure_partitions_forward() which runs SECURITY DEFINER as trendx_migration
    # (see 004), so they are owned by trendx_migration. The retention function
    # (also SD as trendx_migration) can therefore DROP them. Mirror that
    # ownership here: trendx_app (a MEMBER of trendx_migration) transfers
    # ownership of the freshly created child to trendx_migration.
    _execute(
        params,
        "ALTER TABLE trendx_analytics." + part_name + " OWNER TO trendx_migration",
    )


def _apply_migration_012(params: dict[str, str]) -> None:
    """Apply migration 012 idempotently (no-op if already applied)."""
    if _function_exists(params):
        return
    migration = next(MIGRATIONS.glob("012_*.sql"))
    _execute_many(params, migration.read_text(encoding="utf-8"))


def _ensure_predecessors(params: dict[str, str]) -> None:
    """Ensure the five parents exist and are partitioned (004/010/011)."""
    for p in PARENTS:
        if _relkind(params, SCHEMA, p) != "p":
            pytest.skip(f"{SCHEMA}.{p} not partitioned in this DB environment")


def _run_retention(params: dict[str, str], keep_months=None) -> dict:
    if keep_months is None:
        result = _scalar(params, "SELECT trendx_analytics.drop_old_partitions(NULL)")
    else:
        result = _scalar(
            params,
            "SELECT trendx_analytics.drop_old_partitions(%s)",
            (keep_months,),
        )
    assert isinstance(result, dict), f"drop_old_partitions must return jsonb, got {result!r}"
    return result


# ---------------------------------------------------------------------------
# Fixture : build a controlled partition layout for the five parents
# ---------------------------------------------------------------------------
@pytest.fixture
def partition_layout():
    pg = _pg_conn_params()
    _apply_migration_012(pg)
    _ensure_predecessors(pg)

    now_month = datetime.now().replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    # Offsets relative to now_month used by the tests.
    layout = {
        "now": now_month,
        "m_minus_1": _add_months(now_month, -1),
        "m_minus_2": _add_months(now_month, -2),
        "m_minus_3": _add_months(now_month, -3),
        "m_minus_4": _add_months(now_month, -4),
        "m_plus_1": _add_months(now_month, 1),
        "m_plus_2": _add_months(now_month, 2),
        "m_plus_3": _add_months(now_month, 3),
    }

    # Clean slate for the test partitions we are about to create.
    for p in PARENTS:
        for off in (-4, -3, -2, -1, 0, 1, 2, 3):
            m = _add_months(now_month, off)
            pn = _month_partition_name(p, m)
            if pn in _partitions(pg, SCHEMA, p):
                _execute(pg, "DROP TABLE trendx_analytics." + pn)

    # Create a controlled set of monthly partitions for every parent.
    offsets = [-4, -3, -2, -1, 0, 1, 2, 3]
    for p in PARENTS:
        for off in offsets:
            _create_partition(pg, p, _add_months(now_month, off))

    yield layout

    # Teardown : drop the test partitions so the DB is left clean for other
    # integration tests (e.g. 009/010/011).
    for p in PARENTS:
        for off in offsets:
            m = _add_months(now_month, off)
            pn = _month_partition_name(p, m)
            if pn in _partitions(pg, SCHEMA, p):
                _execute(pg, "DROP TABLE trendx_analytics." + pn)


# ---------------------------------------------------------------------------
# 1) Fonction 012 présente
# ---------------------------------------------------------------------------
def test_migration_012_function_present():
    pg = _pg_conn_params()
    _apply_migration_012(pg)
    assert _function_exists(pg), "trendx_analytics.drop_old_partitions() must exist after 012"


# ---------------------------------------------------------------------------
# 2) SECURITY DEFINER
# ---------------------------------------------------------------------------
def test_migration_012_security_definer():
    pg = _pg_conn_params()
    _apply_migration_012(pg)
    provolatile = _scalar(
        pg,
        "SELECT p.prosecdef FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace "
        "WHERE n.nspname=%s AND p.proname=%s",
        (SCHEMA, "drop_old_partitions"),
    )
    assert provolatile is True, "drop_old_partitions must be SECURITY DEFINER (prosecdef=true)"


# ---------------------------------------------------------------------------
# 3) Owner trendx_migration
# ---------------------------------------------------------------------------
def test_migration_012_owner_trendx_migration():
    pg = _pg_conn_params()
    _apply_migration_012(pg)
    owner = _scalar(
        pg,
        "SELECT r.rolname FROM pg_proc p "
        "JOIN pg_namespace n ON n.oid=p.pronamespace "
        "JOIN pg_roles r ON r.oid=p.proowner "
        "WHERE n.nspname=%s AND p.proname=%s",
        (SCHEMA, "drop_old_partitions"),
    )
    assert owner == "trendx_migration", f"owner must be trendx_migration, got {owner!r}"


# ---------------------------------------------------------------------------
# 4) search_path sécurisé
# ---------------------------------------------------------------------------
def test_migration_012_secure_search_path():
    pg = _pg_conn_params()
    _apply_migration_012(pg)
    sp = _scalar(
        pg,
        "SELECT array_to_string(p.proconfig, ',') FROM pg_proc p "
        "JOIN pg_namespace n ON n.oid=p.pronamespace "
        "WHERE n.nspname=%s AND p.proname=%s",
        (SCHEMA, "drop_old_partitions"),
    )
    assert sp is not None, "search_path must be set"
    normalized = sp.replace(" ", "").lower()
    assert (
        "search_path=trendx_analytics,pg_temp" in normalized
    ), f"secure search_path required, got {sp!r}"


# ---------------------------------------------------------------------------
# 5) Cinq parents prise en charge
# ---------------------------------------------------------------------------
def test_migration_012_five_parents_supported(partition_layout):
    pg = _pg_conn_params()
    result = _run_retention(pg)  # default retention, no DROP expected here
    per_table = result.get("per_table", {})
    for p in PARENTS:
        assert p in per_table, f"parent {p} must be handled by retention, got {per_table}"


# ---------------------------------------------------------------------------
# 6) Partition ancienne supprimée
# ---------------------------------------------------------------------------
def test_migration_012_old_partition_dropped(partition_layout):
    pg = _pg_conn_params()
    # keep_months=3 -> m_minus_4 (start < now-2) must be dropped for ts_kv.
    p = "ts_kv"
    old = _month_partition_name(p, partition_layout["m_minus_4"])
    assert old in _partitions(pg, SCHEMA, p), "precondition: old partition must exist"
    _run_retention(pg, keep_months=3)
    assert old not in _partitions(pg, SCHEMA, p), f"old partition {old} must be DROPPED"


# ---------------------------------------------------------------------------
# 7) Partition récente conservée (m_minus_1)
# ---------------------------------------------------------------------------
def test_migration_012_recent_partition_kept(partition_layout):
    pg = _pg_conn_params()
    p = "ts_kv"
    recent = _month_partition_name(p, partition_layout["m_minus_1"])
    _run_retention(pg, keep_months=3)
    assert recent in _partitions(pg, SCHEMA, p), f"recent partition {recent} must be KEPT"


# ---------------------------------------------------------------------------
# 8) Mois courant conservé
# ---------------------------------------------------------------------------
def test_migration_012_current_month_kept(partition_layout):
    pg = _pg_conn_params()
    p = "ts_kv"
    cur = _month_partition_name(p, partition_layout["now"])
    _run_retention(pg, keep_months=3)
    assert cur in _partitions(pg, SCHEMA, p), f"current month {cur} must be KEPT"


# ---------------------------------------------------------------------------
# 9) Partitions +3 mois conservées
# ---------------------------------------------------------------------------
def test_migration_012_forward_window_kept(partition_layout):
    pg = _pg_conn_params()
    _run_retention(pg, keep_months=3)
    for p in PARENTS:
        for off in (1, 2, 3):
            pn = _month_partition_name(p, partition_layout[f"m_plus_{off}"])
            assert pn in _partitions(
                pg, SCHEMA, p
            ), f"forward partition {pn} (+{off}mo) must be KEPT"


# ---------------------------------------------------------------------------
# 10) Limit exacte de rétention (keep_months=3)
# ---------------------------------------------------------------------------
def test_migration_012_exact_threshold_keep3(partition_layout):
    pg = _pg_conn_params()
    _run_retention(pg, keep_months=3)
    expects = {
        "m_minus_4": False,  # DROP (current - 4 < seuil)
        "m_minus_3": False,  # DROP (current - 3 < seuil = current - 2)
        "m_minus_2": True,  # KEEP (current - 2 == seuil, not strictly <)
        "m_minus_1": True,  # KEEP
        "now": True,  # KEEP
        "m_plus_1": True,  # KEEP
        "m_plus_2": True,  # KEEP
        "m_plus_3": True,  # KEEP
    }
    for p in PARENTS:
        for key, kept in expects.items():
            pn = _month_partition_name(p, partition_layout[key])
            present = pn in _partitions(pg, SCHEMA, p)
            assert present == kept, f"{p} {key} ({pn}) expected kept={kept}, present={present}"


# ---------------------------------------------------------------------------
# 11) keep_months <= 0 = aucun DROP
# ---------------------------------------------------------------------------
def test_migration_012_zero_keep_no_drop(partition_layout):
    pg = _pg_conn_params()
    old = _month_partition_name("ts_kv", partition_layout["m_minus_4"])
    for km in (0, -5):
        result = _run_retention(pg, keep_months=km)
        assert result.get("dropped_count", 0) == 0, f"keep_months={km} must drop nothing"
        assert old in _partitions(pg, SCHEMA, "ts_kv"), f"keep_months={km} must not drop {old}"


# ---------------------------------------------------------------------------
# 12) Absence de partition ancienne = no-op
# ---------------------------------------------------------------------------
def test_migration_012_no_old_partition_noop():
    pg = _pg_conn_params()
    _apply_migration_012(pg)
    _ensure_predecessors(pg)
    now_month = datetime.now().replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    # Only a single recent set of partitions; nothing old enough to drop.
    for p in PARENTS:
        for off in (0, 1, 2, 3):
            m = _add_months(now_month, off)
            if _month_partition_name(p, m) not in _partitions(pg, SCHEMA, p):
                _create_partition(pg, p, m)
    result = _run_retention(pg, keep_months=3)
    assert result.get("dropped_count", 0) == 0, "no old partition -> no-op, nothing dropped"


# ---------------------------------------------------------------------------
# 13) Idempotence
# ---------------------------------------------------------------------------
def test_migration_012_idempotent(partition_layout):
    pg = _pg_conn_params()
    r1 = _run_retention(pg, keep_months=3)
    dropped1 = set(r1.get("dropped", []))
    r2 = _run_retention(pg, keep_months=3)
    dropped2 = set(r2.get("dropped", []))
    assert dropped2 == set(), f"second run must drop nothing, got {dropped2}"
    # The only thing dropped on first run must be the single old partition(s).
    # dropped1 entries are fully-qualified (schema.table).
    old = "trendx_analytics." + _month_partition_name("ts_kv", partition_layout["m_minus_4"])
    assert old in dropped1, "first run should have dropped the old partition"


# ---------------------------------------------------------------------------
# 14) Aucune DEFAULT supprimée/crée
# ---------------------------------------------------------------------------
def test_migration_012_no_default_partition(partition_layout):
    pg = _pg_conn_params()
    _run_retention(pg, keep_months=3)
    for p in PARENTS:
        parts = _partitions(pg, SCHEMA, p)
        assert not any(
            "default" in x.lower() for x in parts
        ), f"{p} must not have DEFAULT partition after retention: {parts}"


# ---------------------------------------------------------------------------
# 15) Rétention indépendante pour les cinq tables
# ---------------------------------------------------------------------------
def test_migration_012_independent_per_table(partition_layout):
    pg = _pg_conn_params()
    # With keep_months=3 each table drops ONLY its own m_minus_4 month.
    _run_retention(pg, keep_months=3)
    for p in PARENTS:
        old = _month_partition_name(p, partition_layout["m_minus_4"])
        assert old not in _partitions(pg, SCHEMA, p), f"{p} old partition must be dropped"
        recent = _month_partition_name(p, partition_layout["m_minus_1"])
        assert recent in _partitions(pg, SCHEMA, p), f"{p} recent partition must be kept"


# ---------------------------------------------------------------------------
# 16) INSERT toujours routé après rétention
# ---------------------------------------------------------------------------
def test_migration_012_insert_routing_after_retention(partition_layout):
    pg = _pg_conn_params()
    _run_retention(pg, keep_months=3)
    # The current-month partition still exists -> INSERT must route.
    cur_month = partition_layout["now"]
    entity_id = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
    _execute(pg, "DELETE FROM trendx_analytics.ts_kv WHERE entity_id=%s", (entity_id,))
    _execute(
        pg,
        "INSERT INTO trendx_analytics.ts_kv (ts, entity_id, metric_key, dbl_v) "
        "VALUES (%s, %s, 'm_routing', 1.23) "
        "ON CONFLICT (ts, entity_id, metric_key) DO NOTHING",
        (cur_month, entity_id),
    )
    cur_part = _month_partition_name("ts_kv", cur_month)
    cnt = _scalar(
        pg,
        "SELECT count(*) FROM trendx_analytics." + cur_part + " WHERE entity_id=%s",
        (entity_id,),
    )
    assert cnt == 1, f"row must route into {cur_part} after retention"


# ---------------------------------------------------------------------------
# 17) ownership / security path non-superuser
# ---------------------------------------------------------------------------
def test_migration_012_non_superuser_security_definer():
    pg = _pg_conn_params()
    _apply_migration_012(pg)
    _ensure_predecessors(pg)
    now_month = datetime.now().replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    # trendx_app is the bootstrap role in CI (NOT superuser after the migration
    # chain ownership transfers). It is a MEMBER of trendx_migration. Running the
    # function as trendx_app must DROP via the SECURITY DEFINER path.
    is_super = _scalar(
        pg,
        "SELECT rolsuper FROM pg_roles WHERE rolname=%s",
        (pg["user"],),
    )
    # Build an old partition as trendx_app context (it owns trendx_analytics via
    # membership of trendx_migration / grants from 009-011).
    p = "ts_kv"
    old_month = _add_months(now_month, -4)
    old = _month_partition_name(p, old_month)
    if old in _partitions(pg, SCHEMA, p):
        _execute(pg, "DROP TABLE trendx_analytics." + old)
    _create_partition(pg, p, old_month)
    assert old in _partitions(pg, SCHEMA, p), "precondition: old partition exists"

    # Run retention as the (non-superuser) trendx_app role via the SD function.
    result = _run_retention(pg, keep_months=3)
    assert result.get("dropped_count", 0) >= 1, (
        "retention must drop the old partition even when invoked as non-superuser "
        f"trendx_app (superuser={is_super}); got {result}"
    )
    assert old not in _partitions(
        pg, SCHEMA, p
    ), f"old partition {old} must be dropped by non-superuser SD path"


# ---------------------------------------------------------------------------
# 18) Concurrence / advisory lock (pas de crash double exécution)
# ---------------------------------------------------------------------------
def test_migration_012_advisory_lock_safe(partition_layout):
    pg = _pg_conn_params()
    # Two sequential runs acquire the same advisory xact lock without deadlock.
    r1 = _run_retention(pg, keep_months=3)
    r2 = _run_retention(pg, keep_months=3)
    assert r1.get("dropped_count", 0) >= 1
    assert r2.get("dropped_count", 0) == 0


# ---------------------------------------------------------------------------
# 19) Absence de dépendance TimescaleDB
# ---------------------------------------------------------------------------
def test_migration_012_no_timescaledb_dependency():
    migration = next(MIGRATIONS.glob("012_*.sql"))
    text = migration.read_text(encoding="utf-8").lower()
    assert "create_hypertable" not in text, "012 must not call create_hypertable"
    assert "add_retention_policy" not in text, "012 must not call add_retention_policy"
    assert "create extension" not in text, "012 must not CREATE EXTENSION"
    import re

    assert not re.search(
        r"create\s+or\s+replace\s+function[^\n]*ensure_partitions_forward",
        text,
        re.IGNORECASE,
    ), "012 must not redefine ensure_partitions_forward() (004 owns forward)"


# ---------------------------------------------------------------------------
# 20) Absence de régression des tests 009/010/011 (fonction forward intacte)
# ---------------------------------------------------------------------------
def test_migration_012_forward_still_operational():
    pg = _pg_conn_params()
    _apply_migration_012(pg)
    _ensure_predecessors(pg)
    result = _scalar(pg, "SELECT trendx_analytics.ensure_partitions_forward(3)")
    assert isinstance(result, dict), "ensure_partitions_forward(3) must still work after 012"
    for p in PARENTS:
        assert _month_partition_name(p, datetime.now().replace(day=1)) in _partitions(
            pg, SCHEMA, p
        ), f"{p} current-month partition missing after forward"


def test_migration_012_function_uses_drop_not_delete():
    migration = next(MIGRATIONS.glob("012_*.sql"))
    text = migration.read_text(encoding="utf-8").lower()
    # The retention must use DROP TABLE, never DELETE FROM for purging.
    assert "drop table" in text, "012 must DROP TABLE partitions"
    # No row-by-row DELETE used for the retention purge.
    assert "delete from" not in text, "012 must not DELETE row-by-row for retention"


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main([__file__, "-v"]))
