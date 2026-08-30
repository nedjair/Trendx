"""Integration tests for migration 003 (relocalisation du catalogue Trendx).

Applies the real migration chain against a REAL PostgreSQL instance (CI
``postgres:16-alpine`` service) and proves the operator-validated guardrails:

  * #8  base vierge : 001..008 s'exécute sans bootstrap CI ad hoc de
         trendx_catalog (001 crée le schéma) ; toutes les tables du catalogue
         et la fonction sont dans trendx_catalog.
  * #9  base historique (état public) : après 003, tout le catalogue est
         relocalisé dans trendx_catalog ; AUCUNE perte de données (SET SCHEMA
         préserve les lignes).
  * #10 intégrité : 58 tables, 17 FK intra-catalogue, fonction
         is_cached_telemetry_timestamps_do_not_intersect présente dans
         trendx_catalog, CHECK valide et référençant la fonction qualifiée ;
         idempotence de 001 et de 003 (réexécution sans erreur, état stable).

Fail-closed : tout psql non-zéro (ON_ERROR_STOP=1) ou assertion échoue le job.
"""

from __future__ import annotations

import os
import subprocess
import tempfile
from pathlib import Path

import pytest

ROOT = Path(__file__).parents[2]
MIGRATIONS = ROOT / "migrations"

pytestmark = [pytest.mark.integration, pytest.mark.migration_apply]

# 58 tables du catalogue (inventaire validé B7 / plan 003).
CATALOG_TABLES = [
    "agent_ai",
    "anomaly",
    "anomaly_model_task_data",
    "api_key",
    "business_entity",
    "business_entity_field",
    "business_entity_field_metadata",
    "business_entity_metadata",
    "cached_telemetry",
    "cached_telemetry_point",
    "calculation_field",
    "calculation_field_task_data",
    "cluster_example",
    "cluster_info",
    "cluster_model",
    "custom_prediction_model",
    "custom_prompt",
    "custom_prompt_metadata",
    "custom_view_settings",
    "dataset_config",
    "datasource",
    "domain_tenant_pair",
    "latest_telemetry",
    "licence_data",
    "llm_config",
    "llm_settings",
    "llm_settings_chat_type_link",
    "manual_dataset",
    "metric_definition",
    "metric_definition_metadata",
    "metric_exploration",
    "ml_properties",
    "prediction_model",
    "prediction_model_last_item_point",
    "prediction_model_task_data",
    "relation",
    "scored_point_anomaly",
    "scored_point_centroid",
    "scored_point_cluster",
    "scored_point_histogram",
    "segment_data",
    "trendz_system_property",
    "trendz_task",
    "trendz_task_execution",
    "trendz_task_execution_progress_step",
    "trendz_task_execution_request",
    "trendz_task_execution_state_record",
    "trendz_task_scheduling_state_record",
    "trendz_task_sequence",
    "trendz_task_sequence_item",
    "user_metadata",
    "user_record",
    "view_assistance_chat",
    "view_assistance_chat_message",
    "view_assistance_token_usage",
    "view_collection",
    "view_config",
    "view_field",
]


def _env(key: str, default: str) -> str:
    return os.environ.get(key, default)


def _pg_conn_params() -> dict[str, str]:
    return {
        "host": _env("PG_ADMIN_HOST", _env("TRENDX_DB_HOST", "postgres")),
        "port": int(_env("PG_ADMIN_PORT", _env("TRENDX_DB_PORT", "5432"))),
        "dbname": _env("PG_ADMIN_DB", _env("TRENDX_DB_NAME", "trendx")),
        "user": _env("PG_ADMIN_USER", _env("TRENDX_DB_USER", "trendx_app")),
        "password": _env("PG_ADMIN_PASSWORD", _env("TRENDX_DB_PASSWORD", "trendx_app_pass")),
    }


def _psql_run(sql_file: Path, params: dict[str, str]) -> None:
    env = dict(os.environ)
    env["PGHOST"] = params["host"]
    env["PGPORT"] = str(params["port"])
    env["PGDATABASE"] = params["dbname"]
    env["PGUSER"] = params["user"]
    env["PGPASSWORD"] = params["password"]
    subprocess.run(["psql", "-v", "ON_ERROR_STOP=1", "-f", str(sql_file)], env=env, check=True)


def _psycopg(params: dict[str, str]):
    import psycopg2

    return psycopg2.connect(
        host=params["host"],
        port=params["port"],
        dbname=params["dbname"],
        user=params["user"],
        password=params["password"],
    )


def _reset_db(params: dict[str, str]) -> None:
    """Remet la base dans un état quasi-vierge (poonctuel, test uniquement)."""
    conn = _psycopg(params)
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("DROP SCHEMA IF EXISTS trendx_catalog CASCADE")
            for t in CATALOG_TABLES:
                cur.execute(
                    "SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename=%s",
                    (t,),
                )
                if cur.fetchone():
                    cur.execute(f'DROP TABLE public."{t}" CASCADE')
    finally:
        conn.close()


def _catalog_table_count(params: dict[str, str]) -> int:
    conn = _psycopg(params)
    try:
        with conn.cursor() as cur:
            # Compte uniquement les 58 tables du catalogue (les migrations
            # 005/006/007 ajoutent d'autres tables dans trendx_catalog).
            cur.execute(
                "SELECT count(*) FROM information_schema.tables "
                "WHERE table_schema='trendx_catalog' AND table_type='BASE TABLE' "
                "AND table_name = ANY(%s)",
                (CATALOG_TABLES,),
            )
            return int(cur.fetchone()[0])
    finally:
        conn.close()


def _public_catalog_table_count(params: dict[str, str]) -> int:
    conn = _psycopg(params)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM information_schema.tables "
                "WHERE table_schema='public' AND table_name = ANY(%s)",
                (CATALOG_TABLES,),
            )
            return int(cur.fetchone()[0])
    finally:
        conn.close()


def _fk_count_within_catalog(params: dict[str, str]) -> int:
    conn = _psycopg(params)
    try:
        with conn.cursor() as cur:
            # FK dont la table source ET la table référencée sont dans trendx_catalog
            cur.execute(
                """
                SELECT count(*) FROM pg_constraint c
                JOIN pg_class src ON src.oid = c.conrelid
                JOIN pg_namespace ns ON ns.oid = src.relnamespace
                JOIN pg_class dst ON dst.oid = c.confrelid
                JOIN pg_namespace nd ON nd.oid = dst.relnamespace
                WHERE c.contype = 'f'
                  AND ns.nspname = 'trendx_catalog'
                  AND nd.nspname = 'trendx_catalog'
                """
            )
            return int(cur.fetchone()[0])
    finally:
        conn.close()


def _function_in_catalog(params: dict[str, str]) -> bool:
    conn = _psycopg(params)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace "
                "WHERE n.nspname='trendx_catalog' "
                "AND p.proname='is_cached_telemetry_timestamps_do_not_intersect'"
            )
            return cur.fetchone() is not None
    finally:
        conn.close()


def _check_references_catalog_fn(params: dict[str, str]) -> bool:
    conn = _psycopg(params)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT pg_get_constraintdef(oid) FROM pg_constraint "
                "WHERE conname='is_timestamps_do_not_intersect_constraint'"
            )
            row = cur.fetchone()
            if not row:
                return False
            return "trendx_catalog.is_cached_telemetry_timestamps_do_not_intersect" in row[0]
    finally:
        conn.close()


def _original_001_file() -> Path | None:
    """Récupère la 001 historique (catalogue en public) depuis git HEAD."""
    import subprocess

    try:
        out = subprocess.run(
            ["git", "show", "HEAD:migrations/001_trendz_native_schema.sql"],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=True,
        )
    except Exception:
        return None
    tmp = tempfile.NamedTemporaryFile("w", suffix=".sql", delete=False, encoding="utf-8")
    tmp.write(out.stdout)
    tmp.close()
    return Path(tmp.name)


def test_003_blank_db_chain_catalog_in_trendx_catalog():
    params = _pg_conn_params()
    migration_001 = next(MIGRATIONS.glob("001_*.sql"))
    migration_003 = MIGRATIONS / "003_relocate_catalog.sql"
    migration_005 = MIGRATIONS / "005_ingestion_checkpoints.sql"
    migration_006 = MIGRATIONS / "006_prediction_model_status_history.sql"
    migration_007 = MIGRATIONS / "007_topology_tables.sql"
    migration_008 = MIGRATIONS / "008_add_model_uri.sql"
    for m in (migration_003, migration_005, migration_006, migration_007, migration_008):
        assert m.exists(), f"{m} missing"

    _reset_db(params)

    # Chaîne concernée sur base VIERGE, sans bootstrap ad hoc de trendx_catalog.
    _psql_run(migration_001, params)
    _psql_run(migration_003, params)
    _psql_run(migration_005, params)
    _psql_run(migration_006, params)
    _psql_run(migration_007, params)
    _psql_run(migration_008, params)

    # trendx_catalog créé par 001, pas de tables catalogue restées en public.
    assert _catalog_table_count(params) == len(CATALOG_TABLES), (
        f"trendx_catalog doit contenir {len(CATALOG_TABLES)} tables, "
        f"trouvé {_catalog_table_count(params)}"
    )
    assert _public_catalog_table_count(params) == 0, (
        f"aucune table catalogue ne doit rester en public, "
        f"trouvé {_public_catalog_table_count(params)}"
    )
    # 17 FK intra-catalogue préservées.
    assert (
        _fk_count_within_catalog(params) == 17
    ), f"17 FK intra-catalogue attendues, trouvées {_fk_count_within_catalog(params)}"
    # Fonction + CHECK dans trendx_catalog, CHECK référence la fonction qualifiée.
    assert _function_in_catalog(
        params
    ), "fonction is_cached_telemetry_timestamps_do_not_intersect absente de trendx_catalog"
    assert _check_references_catalog_fn(
        params
    ), "CHECK de cached_telemetry ne référence pas trendx_catalog.is_cached_telemetry_timestamps_do_not_intersect"
    # 008 a bien ajouté model_uri sur trendx_catalog.prediction_model.
    conn = _psycopg(params)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM information_schema.columns "
                "WHERE table_schema='trendx_catalog' AND table_name='prediction_model' "
                "AND column_name='model_uri'"
            )
            assert (
                cur.fetchone() is not None
            ), "model_uri manquant sur trendx_catalog.prediction_model"
    finally:
        conn.close()


def test_003_relocates_historical_public_catalog_without_data_loss():
    params = _pg_conn_params()
    original_001 = _original_001_file()
    if original_001 is None:
        pytest.skip("git HEAD 001 indisponible (pas de VCS) — scénario historique sauté")
    migration_003 = MIGRATIONS / "003_relocate_catalog.sql"
    migration_008 = MIGRATIONS / "008_add_model_uri.sql"
    _reset_db(params)

    # État HISTORIQUE : 001 pose tout le catalogue en public.
    _psql_run(original_001, params)
    assert _public_catalog_table_count(params) == len(CATALOG_TABLES), (
        f"précondition: {len(CATALOG_TABLES)} tables catalogue en public, "
        f"trouvé {_public_catalog_table_count(params)}"
    )

    # Injecte une ligne AVANT relocalisation (preuve de non-perte de données).
    conn = _psycopg(params)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO public.api_key (id, tenant_id, token) "
                "VALUES ('11111111-1111-1111-1111-111111111111', "
                "'22222222-2222-2222-2222-222222222222', 'tok-test')"
            )
        conn.commit()
    finally:
        conn.close()

    # Relocalisation + 008.
    _psql_run(migration_003, params)
    _psql_run(migration_008, params)

    # Tout le catalogue est maintenant dans trendx_catalog.
    assert _catalog_table_count(params) == len(CATALOG_TABLES), (
        f"après 003, trendx_catalog doit contenir {len(CATALOG_TABLES)} tables, "
        f"trouvé {_catalog_table_count(params)}"
    )
    assert _public_catalog_table_count(params) == 0, (
        f"après 003, aucune table catalogue ne doit rester en public, "
        f"trouvé {_public_catalog_table_count(params)}"
    )
    assert (
        _fk_count_within_catalog(params) == 17
    ), f"17 FK intra-catalogue attendues, trouvées {_fk_count_within_catalog(params)}"
    assert _function_in_catalog(params), "fonction absente de trendx_catalog après relocalisation"
    assert _check_references_catalog_fn(
        params
    ), "CHECK non réécrit vers trendx_catalog après relocalisation"

    # NON-PERTE DE DONNÉES : la ligne insérée en public est préservée.
    conn = _psycopg(params)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT token FROM trendx_catalog.api_key WHERE token='tok-test'")
            assert (
                cur.fetchone() is not None
            ), "donnée insérée en public perdue après relocalisation"
    finally:
        conn.close()


def test_003_idempotent_reapply():
    params = _pg_conn_params()
    migration_001 = next(MIGRATIONS.glob("001_*.sql"))
    migration_003 = MIGRATIONS / "003_relocate_catalog.sql"
    _reset_db(params)

    # Première application.
    _psql_run(migration_001, params)
    _psql_run(migration_003, params)
    count_after_first = _catalog_table_count(params)
    fk_after_first = _fk_count_within_catalog(params)

    # Réexécution (idempotence) : ne doit ni errer ni dupliquer.
    _psql_run(migration_001, params)
    _psql_run(migration_003, params)

    assert (
        _catalog_table_count(params) == count_after_first
    ), "réexécution 001/003 a modifié le nombre de tables"
    assert (
        _fk_count_within_catalog(params) == fk_after_first
    ), "réexécution 001/003 a modifié le nombre de FK"
    assert _function_in_catalog(params), "fonction absente après réexécution"
    assert _check_references_catalog_fn(params), "CHECK invalide après réexécution"


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main([__file__, "-v"]))
