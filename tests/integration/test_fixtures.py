"""Shared helpers for disposable-PostgreSQL migration tests (no fixtures here).

This module exposes plain functions only. The pytest fixtures
(``ephemeral_postgres``, ``provisioned_postgres``) live in ``tests/conftest.py``
so they are auto-discovered without being imported into test modules (which
would shadow the fixture name and trip the linter).

If ``TRENDX_TEST_PGPORT`` is set, the helpers use that already-running
PostgreSQL (e.g. the GitLab CI ``postgres`` service). Otherwise they start a
fresh ``postgres:16-alpine`` container via the local Docker daemon and tear it
down. No real Trendx/ThingsBoard database is ever touched.
"""

from __future__ import annotations

import os
import secrets
import shutil
import subprocess
import time

IMAGE = "postgres:16-alpine"


def docker_available() -> bool:
    if shutil.which("docker") is None:
        return False
    try:
        subprocess.run(
            ["docker", "info"],
            capture_output=True,
            text=True,
            timeout=20,
            check=True,
        )
    except Exception:
        return False
    return True


def _rand() -> str:
    return secrets.token_hex(8)


def _env_params() -> dict | None:
    port = os.environ.get("TRENDX_TEST_PGPORT")
    if not port:
        return None
    return {
        "host": os.environ.get("TRENDX_TEST_PGHOST", "127.0.0.1"),
        "port": int(port),
        "user": os.environ.get("TRENDX_TEST_PGUSER", "postgres"),
        "password": os.environ.get("TRENDX_TEST_PGPASSWORD", ""),
        "dbname": os.environ.get("TRENDX_TEST_PGDATABASE", "trendx"),
    }


def _start_container(password: str) -> tuple[str, int]:
    name = f"trendx-migtest-{_rand()}"
    subprocess.run(
        [
            "docker",
            "run",
            "-d",
            "--rm",
            "--name",
            name,
            "-e",
            f"POSTGRES_PASSWORD={password}",
            "-e",
            "POSTGRES_DB=trendx",
            "-p",
            "127.0.0.1::5432",
            IMAGE,
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    for _ in range(50):
        out = subprocess.run(
            ["docker", "port", name, "5432"],
            capture_output=True,
            text=True,
        ).stdout.strip()
        if out:
            port = out.splitlines()[0].rsplit(":", 1)[-1].strip()
            if port.isdigit():
                return name, int(port)
        time.sleep(1)
    raise RuntimeError("could not resolve mapped port for disposable postgres")


def _wait_ready(name: str, port: int, password: str) -> None:
    env = dict(os.environ)
    env["PGPASSWORD"] = password
    deadline = time.time() + 60
    while time.time() < deadline:
        r = subprocess.run(
            ["pg_isready", "-h", "127.0.0.1", "-p", str(port), "-U", "postgres", "-d", "trendx"],
            capture_output=True,
            text=True,
            env=env,
        )
        if r.returncode == 0:
            return
        time.sleep(1)
    raise RuntimeError("disposable postgres did not become ready")


def start_params() -> dict:
    """Return connection params for a usable disposable PostgreSQL.

    Uses the CI-provided service DB when TRENDX_TEST_PGPORT is set, otherwise
    spins up a Docker container. Raises RuntimeError if neither is available.
    """
    env = _env_params()
    if env is not None:
        return env
    if not docker_available():
        raise RuntimeError(
            "no disposable PostgreSQL available (set TRENDX_TEST_PGPORT or install docker)"
        )
    password = secrets.token_hex(16)
    name, port = _start_container(password)
    _wait_ready(name, port, password)
    return {
        "host": "127.0.0.1",
        "port": port,
        "user": "postgres",
        "password": password,
        "dbname": "trendx",
        "_container": name,
    }


def stop_params(params: dict) -> None:
    name = params.get("_container")
    if name:
        subprocess.run(["docker", "rm", "-f", name], capture_output=True, text=True)


def run_sql(params: dict, sql: str) -> None:
    env = dict(os.environ)
    env["PGHOST"] = params["host"]
    env["PGPORT"] = str(params["port"])
    env["PGUSER"] = params["user"]
    env["PGDATABASE"] = params["dbname"]
    env["PGPASSWORD"] = params["password"]
    subprocess.run(
        ["psql", "-v", "ON_ERROR_STOP=1", "-X", "-q", "-c", sql],
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )


def psql_run_file(params: dict, sql_file) -> subprocess.CompletedProcess:
    """Apply a SQL file via psql with ON_ERROR_STOP (real PostgreSQL, not a mock)."""
    env = dict(os.environ)
    env["PGHOST"] = params["host"]
    env["PGPORT"] = str(params["port"])
    env["PGUSER"] = params["user"]
    env["PGDATABASE"] = params["dbname"]
    env["PGPASSWORD"] = params["password"]
    return subprocess.run(
        ["psql", "-v", "ON_ERROR_STOP=1", "-X", "-f", str(sql_file)],
        env=env,
        capture_output=True,
        text=True,
    )


def pg_query(params: dict, sql: str) -> list:
    """Run a read query, return list of non-empty output lines."""
    env = dict(os.environ)
    env["PGHOST"] = params["host"]
    env["PGPORT"] = str(params["port"])
    env["PGUSER"] = params["user"]
    env["PGDATABASE"] = params["dbname"]
    env["PGPASSWORD"] = params["password"]
    r = subprocess.run(
        ["psql", "-A", "-t", "-X", "-c", sql],
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )
    return [line for line in r.stdout.splitlines() if line != ""]


def schema_of(params: dict, table: str) -> str | None:
    rows = pg_query(
        params,
        "SELECT table_schema FROM information_schema.tables " f"WHERE table_name = '{table}'",
    )
    return rows[0] if rows else None
