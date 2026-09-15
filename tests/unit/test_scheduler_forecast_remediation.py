"""Unit tests for forecast fan-out, reference keys and reconciliation.

No database, no threads, no execution: sessions, run stores and task
factories are faked in memory. Covers the remediation contract.
"""

from __future__ import annotations

import uuid
from types import SimpleNamespace
from typing import Any

import pytest
from trendx.scheduler.fanout import (
    build_forecast_payload,
    fanout_enqueue_forecast_jobs,
    forecast_reference_key,
    iter_forecast_pairs,
    reconcile_runs,
    window_bucket,
)

pytestmark = pytest.mark.unit


class _Obj:
    def __init__(self, **kwargs: Any) -> None:
        self.__dict__.update(kwargs)


class _CtxSession:
    """Context-manager stub for session factories (no DB)."""

    def __enter__(self) -> _CtxSession:
        return self

    def __exit__(self, *args: Any) -> bool:
        return False


def _entity(eid: str, tenant: str | None, metrics: list[str]) -> tuple[_Obj, list[_Obj]]:
    ent = _Obj(id=uuid.UUID(eid), tenant_id=tenant, name=f"ent-{eid[:4]}")
    mets = [_Obj(item_name=m) for m in metrics]
    return ent, mets


@pytest.fixture()
def pair_env(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    e1, m1 = _entity(
        "11111111-1111-4111-8111-111111111111", "tenant-a", ["temperature", "humidity"]
    )
    e2, m2 = _entity("22222222-2222-4222-8222-222222222222", "tenant-b", ["temperature"])
    e3, _ = _entity("33333333-3333-4333-8333-333333333333", None, ["temperature"])
    entities = [e1, e2, e3]
    by_entity = {str(e1.id): m1, str(e2.id): m2, str(e3.id): []}

    class _FakeBERepo:
        def __init__(self, session: Any) -> None:
            pass

        def list(self) -> list[Any]:
            return entities

    class _FakeMDRepo:
        def __init__(self, session: Any) -> None:
            pass

        def find_by_business_entity(self, eid: Any) -> list[Any]:
            return by_entity.get(str(eid), [])

    monkeypatch.setattr("trendx.database.repositories.BusinessEntityRepository", _FakeBERepo)
    monkeypatch.setattr("trendx.database.repositories.MetricDefinitionRepository", _FakeMDRepo)
    return {"session": _CtxSession()}


def test_fanout_empty_catalogue(monkeypatch: pytest.MonkeyPatch) -> None:
    import trendx.database.repositories as repos

    class _EmptyBE:
        def __init__(self, session: Any) -> None:
            pass

        def list(self) -> list[Any]:
            return []

    monkeypatch.setattr(repos, "BusinessEntityRepository", _EmptyBE)
    assert iter_forecast_pairs(object()) == []


def test_fanout_pairs_dedup_and_tenant_scope(pair_env: dict[str, Any]) -> None:
    pairs = iter_forecast_pairs(pair_env["session"])
    # e1 x 2 metrics + e2 x 1 metric ; e3 skipped (no tenant).
    assert len(pairs) == 3
    tenants = {(p["tenant_id"], p["entity_id"], p["metric_name"]) for p in pairs}
    assert len(tenants) == 3
    assert all(p["entity_type"] == "DEVICE" for p in pairs)
    assert all("33333333" not in p["entity_id"] for p in pairs)


def test_build_payload_complete_and_typed() -> None:
    pair = {"tenant_id": "t", "entity_type": "DEVICE", "entity_id": "e", "metric_name": "m"}
    payload = build_forecast_payload(pair=pair, job_type="trendx_forecast", window="2026-01-01T00")
    assert payload["tenant_id"] == "t"
    assert payload["entity_type"] == "DEVICE"
    assert payload["entity_id"] == "e"
    assert payload["metric_name"] == "m"
    assert payload["job_type"] == "trendx_forecast"
    assert payload["reference_key"] == "trendx_forecast:t:e:m:2026-01-01T00"


def test_build_payload_rejects_bad_type_and_incomplete() -> None:
    with pytest.raises(ValueError):
        build_forecast_payload(
            pair={"tenant_id": "t", "entity_id": "e", "metric_name": "m"},
            job_type="trendx_train_typo",
            window="w",
        )
    with pytest.raises(ValueError):
        build_forecast_payload(
            pair={"tenant_id": "", "entity_id": "e", "metric_name": "m"},
            job_type="trendx_train",
            window="w",
        )


def test_reference_key_deterministic() -> None:
    kwargs: dict[str, str] = {
        "tenant_id": "t",
        "entity_id": "e",
        "metric_name": "m",
        "job_type": "trendx_train",
        "window": "2026-01-02",
    }
    assert forecast_reference_key(**kwargs) == forecast_reference_key(**kwargs)
    assert "t" in forecast_reference_key(**kwargs)


def test_window_bucket_shapes() -> None:
    from datetime import datetime

    assert window_bucket(job_id="forecast-train").count("-") == 2
    assert "T" in window_bucket(job_id="forecast-run")
    assert window_bucket(job_id="x", at=datetime(2026, 1, 2, 3, 4)) == "2026-01-02T03"


class _FakeRunStore:
    def __init__(self) -> None:
        self.begun: list[dict[str, Any]] = []
        self.terminals: list[tuple[str, str]] = []

    def begin_run(self, **kwargs: Any) -> None:
        self.begun.append(kwargs)

    def finish_run(
        self,
        *,
        run_id: str,
        status: str,
        error: str | None = None,
        task_id: str | None = None,
    ) -> None:
        # Mirrors DbRunStore.finish_run, including its terminal guard.
        if status not in ("ok", "failed", "enqueued"):
            raise ValueError(status)
        self.terminals.append((run_id, status))


class _FakeTasks:
    def __init__(self) -> None:
        self.created: list[dict[str, Any]] = []

    def __call__(self, **kwargs: Any) -> SimpleNamespace:
        self.created.append(kwargs)
        return SimpleNamespace(id=uuid.uuid4())


def test_fanout_enqueues_typed_tasks(pair_env: dict[str, Any]) -> None:
    store, tasks = _FakeRunStore(), _FakeTasks()
    out = fanout_enqueue_forecast_jobs(
        run_store=store,
        create_task=tasks,
        session_factory=lambda: pair_env["session"],
        instance_id="i-1",
        job_id="forecast-run",
        job_type="trendx_forecast",
        window="2026-01-01T00",
    )
    assert out["status"] == "ok" and out["fanned_out"] == 3 and out["skipped"] == 0
    assert {c["job_type"] for c in tasks.created} == {"trendx_forecast"}
    assert len(store.begun) == 3
    assert all(b["mode"] == "enqueue" for b in store.begun)


def test_fanout_empty_returns_ok_zero(
    pair_env: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    import trendx.scheduler.fanout as fanout_module

    monkeypatch.setattr(fanout_module, "iter_forecast_pairs", lambda session: [])
    store, tasks = _FakeRunStore(), _FakeTasks()
    out = fanout_enqueue_forecast_jobs(
        run_store=store,
        create_task=tasks,
        session_factory=lambda: pair_env["session"],
        instance_id="i-1",
        job_id="forecast-run",
        job_type="trendx_forecast",
    )
    assert out == {"status": "ok", "fanned_out": 0, "skipped": 0, "window": out["window"]}
    assert tasks.created == [] and store.begun == []


def _run_row(run_id: str, task_id: str | None, status: str = "running") -> SimpleNamespace:
    return SimpleNamespace(run_id=run_id, task_id=task_id, status=status, job_id="forecast-run")


_TASK_OK = "11111111-1111-4111-8111-111111111111"
_TASK_BAD = "22222222-2222-4222-8222-222222222222"
_TASK_WAIT = "33333333-3333-4333-8333-333333333333"


def _exec_row(task_id: str, status: str, error: str = "") -> SimpleNamespace:
    return SimpleNamespace(task_id=task_id, status=status, json_result=f'{{"error": "{error}"}}')


def test_reconcile_success_failed_missing_and_terminal() -> None:
    seen: list[tuple[str, str]] = []

    class _Store:
        def finish_run(
            self,
            *,
            run_id: str,
            status: str,
            error: str | None = None,
            task_id: str | None = None,
        ) -> None:
            seen.append((run_id, status))

    run_ok = _run_row("run-ok", _TASK_OK)
    run_bad = _run_row("run-bad", _TASK_BAD)
    run_wait = _run_row("run-wait", _TASK_WAIT)
    run_notask = _run_row("run-notask", None)
    # Deterministic fake: runs first, then one execution lookup per tasked run.
    states = iter(
        [
            [run_ok, run_bad, run_wait, run_notask],
            [_exec_row(_TASK_OK, "FINISHED")],
            [_exec_row(_TASK_BAD, "FAILED", "boom")],
            [],
        ]
    )

    class _Session:
        def __enter__(self) -> _Session:
            return self

        def __exit__(self, *args: Any) -> bool:
            return False

        def scalars(self, stmt: Any) -> Any:
            class _R:
                def all(self) -> list[Any]:
                    return next(states)

            return _R()

    out = reconcile_runs(
        run_store=_Store(), session_factory=lambda: _Session(), job_ids=["forecast-run"]
    )
    assert out["status"] == "ok"
    assert ("run-ok", "ok") in seen
    assert ("run-bad", "failed") in seen
    assert out["left_running"] == 1  # task-wait has no terminal execution
    assert all(r != "run-notask" for r, _ in seen)  # task-less rows untouched


def test_reconcile_never_rewrites_worker_failure_as_success() -> None:
    seen: list[tuple[str, str]] = []

    class _Store:
        def finish_run(
            self,
            *,
            run_id: str,
            status: str,
            error: str | None = None,
            task_id: str | None = None,
        ) -> None:
            seen.append((run_id, status))
            assert status != "ok" or run_id != "run-bad"

    states = iter(
        [[_run_row("run-bad", _TASK_BAD)], [_exec_row(_TASK_BAD, "FAILED", "missing keys")]]
    )

    class _Session:
        def __enter__(self) -> _Session:
            return self

        def __exit__(self, *args: Any) -> bool:
            return False

        def scalars(self, stmt: Any) -> Any:
            class _R:
                def all(self) -> list[Any]:
                    return next(states)

            return _R()

    reconcile_runs(run_store=_Store(), session_factory=lambda: _Session())
    assert seen == [("run-bad", "failed")]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main([__file__, "-v"]))
