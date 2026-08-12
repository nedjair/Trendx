"""Integration test for migration 007 (topology discovery tables).

Applies migration 007 against a real PostgreSQL instance and proves that the
three ``trendx_catalog`` topology tables are created with the expected
structure and grants, that 007 is idempotent, and that
``TopologyDiscoveryService.update_catalog()`` actually writes and reads back
topology data through the ORM.

The ThingsBoard client is MOCKED (no network), but the PostgreSQL database is
REAL: the catalog engine (search_path ``trendx_catalog,public``) is pointed at
the CI PostgreSQL service via environment variables, and rows are asserted
after a real COMMIT and a NEW SESSION read.

The test must NOT modify any migration file, ORM model, or protected component.
It requires a live PostgreSQL and is selected by the ``integration`` marker; it
only runs when ``--run-integration`` is passed or a DB is reachable, consistent
with ``tests/conftest.py`` (fail-closed: any psql/assertion error fails the job;
no ``|| true``, no swallowed exceptions, no artificial skip).
"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path
from unittest.mock import MagicMock

import pytest

# Environment MUST be set before importing ``trendx.database``: SQLAlchemy
# engines (including the catalog engine used by update_catalog) are registered
# at import time from settings resolved from the environment. CI provides these
# variables; setdefault keeps behaviour identical for a local run against a
# reachable PostgreSQL.
os.environ.setdefault("PG_ADMIN_HOST", "postgres")
os.environ.setdefault("PG_ADMIN_PORT", "5432")
os.environ.setdefault("PG_ADMIN_DB", "trendx")
os.environ.setdefault("PG_ADMIN_USER", "trendx_app")
os.environ.setdefault("PG_ADMIN_PASSWORD", "trendx_app_pass")
os.environ.setdefault("TRENDX_APP_USER", "trendx_app")
os.environ.setdefault("TRENDX_APP_PASSWORD", "trendx_app_pass")
os.environ.setdefault("TRENDX_DB_NAME", "trendx")
os.environ.setdefault("TRENDX_DB_HOST", "postgres")
os.environ.setdefault("TRENDX_DB_PORT", "5432")
os.environ.setdefault("TRENDX_DB_USER", "trendx_app")
os.environ.setdefault("TRENDX_DB_PASSWORD", "trendx_app_pass")

from trendx.database import manager as db_manager  # noqa: E402  (registers engines)
from trendx.thingsboard.client import EntityId, Relation  # noqa: E402
from trendx.thingsboard.discovery import (  # noqa: E402
    CatalogEntry,
    SyncMetadata,
    TopologyDiscoveryService,
    TopologyEntity,
    TopologyRelation,
)

ROOT = Path(__file__).parents[2]
MIGRATIONS = ROOT / "migrations"

TOPOLOGY_TABLES = ("topology_entities", "topology_relations", "sync_metadata")

EXPECTED_COLUMNS = {
    "topology_entities": {
        "id",
        "entity_type",
        "entity_id",
        "name",
        "label",
        "entity_data",
        "attributes",
        "telemetry_keys",
        "first_seen",
        "last_seen",
    },
    "topology_relations": {
        "id",
        "from_type",
        "from_id",
        "to_type",
        "to_id",
        "relation_type",
        "relation_data",
    },
    "sync_metadata": {"id", "sync_key", "sync_value"},
}


def _pg_conn_params() -> dict[str, str]:
    """Build connection params from CI environment variables."""
    return {
        "host": os.environ.get("PG_ADMIN_HOST", os.environ.get("TRENDX_DB_HOST", "postgres")),
        "port": int(os.environ.get("PG_ADMIN_PORT", os.environ.get("TRENDX_DB_PORT", "5432"))),
        "dbname": os.environ.get("PG_ADMIN_DB", os.environ.get("TRENDX_DB_NAME", "trendx")),
        "user": os.environ.get("PG_ADMIN_USER", os.environ.get("TRENDX_DB_USER", "trendx_app")),
        "password": os.environ.get(
            "PG_ADMIN_PASSWORD", os.environ.get("TRENDX_DB_PASSWORD", "trendx_app_pass")
        ),
    }


def _psql_run(sql_file: Path, params: dict[str, str]) -> None:
    """Apply a SQL migration file via psql with ON_ERROR_STOP (strict)."""
    env = dict(os.environ)
    env["PGHOST"] = params["host"]
    env["PGPORT"] = str(params["port"])
    env["PGDATABASE"] = params["dbname"]
    env["PGUSER"] = params["user"]
    env["PGPASSWORD"] = params["password"]
    subprocess.run(
        ["psql", "-v", "ON_ERROR_STOP=1", "-f", str(sql_file)],
        env=env,
        check=True,
    )


def _connect(params: dict[str, str]):
    import psycopg2

    return psycopg2.connect(
        host=params["host"],
        port=params["port"],
        dbname=params["dbname"],
        user=params["user"],
        password=params["password"],
    )


def _ensure_schemas(params: dict[str, str]) -> None:
    conn = _connect(params)
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS trendx_catalog")
            cur.execute("CREATE SCHEMA IF NOT EXISTS trendx_analytics")
    finally:
        conn.close()


def _table_exists(params: dict[str, str], table: str) -> bool:
    conn = _connect(params)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = 'trendx_catalog' AND table_name = %s
                """,
                (table,),
            )
            return cur.fetchone() is not None
    finally:
        conn.close()


def _columns(params: dict[str, str], table: str) -> set[str]:
    conn = _connect(params)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'trendx_catalog' AND table_name = %s
                """,
                (table,),
            )
            return {row[0] for row in cur.fetchall()}
    finally:
        conn.close()


def _has_unique_constraint(params: dict[str, str], table: str, column: str) -> bool:
    conn = _connect(params)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT 1
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu
                  ON tc.constraint_name = kcu.constraint_name
                WHERE tc.table_schema = 'trendx_catalog'
                  AND tc.table_name = %s
                  AND tc.constraint_type = 'UNIQUE'
                  AND kcu.column_name = %s
                """,
                (table, column),
            )
            return cur.fetchone() is not None
    finally:
        conn.close()


def _has_table_privilege(params: dict[str, str], role: str, table: str, priv: str) -> bool:
    conn = _connect(params)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT has_table_privilege(%s, %s, %s)",
                (role, f"trendx_catalog.{table}", priv),
            )
            return bool(cur.fetchone()[0])
    finally:
        conn.close()


def _has_sequence_privilege(params: dict[str, str], role: str, sequence: str, priv: str) -> bool:
    conn = _connect(params)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT has_sequence_privilege(%s, %s, %s)",
                (role, f"trendx_catalog.{sequence}", priv),
            )
            return bool(cur.fetchone()[0])
    finally:
        conn.close()


@pytest.mark.integration
@pytest.mark.migration_apply
def test_migration_007_creates_topology_tables():
    params = _pg_conn_params()
    migration_007 = MIGRATIONS / "007_topology_tables.sql"
    assert migration_007.exists(), "migrations/007_topology_tables.sql missing"

    _ensure_schemas(params)
    _psql_run(migration_007, params)

    for table in TOPOLOGY_TABLES:
        assert _table_exists(params, table), f"trendx_catalog.{table} missing after 007"


@pytest.mark.integration
@pytest.mark.migration_apply
def test_migration_007_table_structure():
    params = _pg_conn_params()
    migration_007 = MIGRATIONS / "007_topology_tables.sql"
    _ensure_schemas(params)
    _psql_run(migration_007, params)

    for table, expected in EXPECTED_COLUMNS.items():
        got = _columns(params, table)
        missing = expected - got
        assert not missing, f"missing columns in trendx_catalog.{table}: {missing}"

    assert _has_unique_constraint(
        params, "sync_metadata", "sync_key"
    ), "sync_metadata.sync_key must be UNIQUE"


@pytest.mark.integration
@pytest.mark.migration_apply
def test_migration_007_grants():
    params = _pg_conn_params()
    migration_007 = MIGRATIONS / "007_topology_tables.sql"
    _ensure_schemas(params)
    _psql_run(migration_007, params)

    # trendx_app gets full DML on the three tables + USAGE on the SERIAL
    # sequences (nextval). trendx_ro gets SELECT only (no sequence grants in 007).
    for table in TOPOLOGY_TABLES:
        for priv in ("SELECT", "INSERT", "UPDATE", "DELETE"):
            assert _has_table_privilege(
                params, "trendx_app", table, priv
            ), f"trendx_app missing {priv} on trendx_catalog.{table}"
        assert _has_table_privilege(
            params, "trendx_ro", table, "SELECT"
        ), f"trendx_ro missing SELECT on trendx_catalog.{table}"

    for seq in ("topology_entities_id_seq", "topology_relations_id_seq", "sync_metadata_id_seq"):
        assert _has_sequence_privilege(
            params, "trendx_app", seq, "USAGE"
        ), f"trendx_app missing USAGE on trendx_catalog.{seq}"


@pytest.mark.integration
@pytest.mark.migration_apply
def test_migration_007_idempotent():
    params = _pg_conn_params()
    migration_007 = MIGRATIONS / "007_topology_tables.sql"
    _ensure_schemas(params)
    _psql_run(migration_007, params)
    # Re-applying must succeed (IF NOT EXISTS); ON_ERROR_STOP would fail otherwise.
    _psql_run(migration_007, params)
    for table in TOPOLOGY_TABLES:
        assert _table_exists(params, table), f"trendx_catalog.{table} missing after re-apply"


@pytest.mark.integration
@pytest.mark.migration_apply
@pytest.mark.asyncio
async def test_topology_orm_update_catalog_persists():
    params = _pg_conn_params()
    migration_007 = MIGRATIONS / "007_topology_tables.sql"
    _ensure_schemas(params)
    _psql_run(migration_007, params)

    # ThingsBoard client is MOCKED; PostgreSQL is REAL.
    service = TopologyDiscoveryService(client=MagicMock())
    service._catalog.devices["device-test-001"] = CatalogEntry(
        entity_type="DEVICE",
        entity_id="device-test-001",
        name="Device Test",
        label="dev-label",
        entity_data={"k": "v"},
        attributes={"a": 1},
        telemetry_keys=["temperature"],
        first_seen=1700000000.0,
        last_seen=0.0,
    )
    service._catalog.assets["asset-test-001"] = CatalogEntry(
        entity_type="ASSET",
        entity_id="asset-test-001",
        name="Asset Test",
        label="",
        entity_data={},
        attributes={},
        telemetry_keys=[],
        first_seen=1700000000.0,
    )
    service._catalog.relations = [
        Relation(
            from_=EntityId(entityType="DEVICE", id="device-test-001"),
            to=EntityId(entityType="ASSET", id="asset-test-001"),
            type="Contains",
            additionalInfo={},
        )
    ]
    service._catalog.last_sync_ts = 1700000100.0

    # Real write through the catalog engine (search_path=trendx_catalog,public).
    await service.update_catalog()

    # NEW SESSION read-back proves WRITE -> COMMIT -> READ.
    with db_manager.get_session("catalog") as session:
        entities = session.query(TopologyEntity).all()
        relations = session.query(TopologyRelation).all()
        sync = session.query(SyncMetadata).filter_by(sync_key="last_sync_ts").all()

    entity_ids = {e.entity_id for e in entities}
    assert entity_ids == {"device-test-001", "asset-test-001"}, entity_ids

    device = next(e for e in entities if e.entity_id == "device-test-001")
    assert device.entity_type == "DEVICE"
    assert device.name == "Device Test"
    assert device.label == "dev-label"
    assert device.telemetry_keys == ["temperature"]

    asset = next(e for e in entities if e.entity_id == "asset-test-001")
    assert asset.entity_type == "ASSET"
    assert asset.name == "Asset Test"

    assert len(relations) == 1, relations
    assert relations[0].from_id == "device-test-001"
    assert relations[0].to_id == "asset-test-001"
    assert relations[0].relation_type == "Contains"

    assert sync, "sync_metadata last_sync_ts not persisted"
    assert sync[0].sync_key == "last_sync_ts"
    assert sync[0].sync_value == "1700000100.0"


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main([__file__, "-v"]))
