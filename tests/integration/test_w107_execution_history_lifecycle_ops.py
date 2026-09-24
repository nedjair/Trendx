"""W107 operational checks using real local processes and temporary stores."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import textwrap
import time
import tracemalloc
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from trendx.forecasting.execution import (
    DurableExecutionStore,
    ExecutionProvenance,
    ExecutionRecord,
    MemoryExecutionStore,
)
from trendx.forecasting.lifecycle import (
    ExecutionHistoryLifecycle,
    ExecutionRetentionPolicy,
    FileSystemArchiveStore,
)

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
NOW = "2026-03-02T00:00:00+00:00"
RETENTION_DAYS = 30


def _env() -> dict[str, str]:
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(SRC) + os.pathsep + environment.get("PYTHONPATH", "")
    return environment


def _run(script: str, *args: str) -> str:
    completed = subprocess.run(
        [sys.executable, "-c", textwrap.dedent(script), *args],
        cwd=ROOT,
        env=_env(),
        check=True,
        capture_output=True,
        text=True,
    )
    return completed.stdout.strip()


def _seed_process(path: Path, archive_path: Path) -> str:
    script = """
        import json
        import sys
        from datetime import UTC, datetime
        from pathlib import Path
        from trendx.forecasting.execution import (
            DurableExecutionStore,
            ExecutionProvenance,
            ExecutionRecord,
        )
        from trendx.forecasting.lifecycle import (
            ExecutionHistoryLifecycle,
            ExecutionRetentionPolicy,
            FileSystemArchiveStore,
        )

        store_path = Path(sys.argv[1])
        archive_path = Path(sys.argv[2])
        store = DurableExecutionStore(store_path)
        record = ExecutionRecord(
            execution_id="process-a",
            reference_key="w107:process-a",
            tenant_id="tenant-A",
            entity_type="DEVICE",
            entity_id="device-a",
            target_metric="temperature",
            frequency="1h",
            horizon=24,
            created_at="2025-01-01T00:00:00Z",
            metadata={"source": "w107-process"},
        )
        store.create(record)
        store.mark_started(record.execution_id)
        store.mark_success(
            record.execution_id,
            ExecutionProvenance(
                model_id="model-w107",
                model_version="1",
                algorithm="Fourier",
                prediction_count=3,
                result={"values": [1.0, 2.0, 3.0]},
            ),
            completed_at="2025-01-01T00:01:00Z",
        )
        archive = FileSystemArchiveStore(archive_path)
        lifecycle = ExecutionHistoryLifecycle(
            store,
            archive,
            lambda: datetime(2026, 3, 2, tzinfo=UTC),
        )
        report = lifecycle.purge_eligible(
            "tenant-A",
            ExecutionRetentionPolicy(retention_days=30, dry_run=False),
        )
        assert report.purged == 1
        assert store.get("process-a") is None
        archived = archive.read("process-a")
        print(json.dumps(archived.record.to_dict(), sort_keys=True))
    """
    return _run(script, str(path), str(archive_path))


def _read_process(path: Path, archive_path: Path) -> str:
    script = """
        import json
        import sys
        from pathlib import Path
        from trendx.forecasting.execution import DurableExecutionStore
        from trendx.forecasting.lifecycle import FileSystemArchiveStore

        store = DurableExecutionStore(Path(sys.argv[1]))
        archive = FileSystemArchiveStore(Path(sys.argv[2]))
        assert store.get("process-a") is None
        archived = archive.read("process-a")
        print(json.dumps(archived.record.to_dict(), sort_keys=True))
    """
    return _run(script, str(path), str(archive_path))


def _seed_one(store: MemoryExecutionStore, execution_id: str) -> None:
    record = ExecutionRecord(
        execution_id=execution_id,
        reference_key=f"w107:{execution_id}",
        tenant_id="tenant-A",
        entity_type="DEVICE",
        entity_id=f"device-{execution_id}",
        target_metric="temperature",
        frequency="1h",
        horizon=24,
        model_id="model-w107",
        model_version="1",
        algorithm="Fourier",
        created_at="2025-01-01T00:00:00Z",
        metadata={"source": "w107-concurrency"},
    )
    store.create(record)
    store.mark_started(execution_id)
    store.mark_success(
        execution_id,
        ExecutionProvenance(
            model_id=record.model_id,
            model_version=record.model_version,
            algorithm=record.algorithm,
            prediction_count=1,
            result={"values": [1.0]},
        ),
        completed_at="2025-01-01T00:01:00Z",
    )


def test_w107_process_a_b(tmp_path: Path) -> None:
    active_path = tmp_path / "active.json"
    archive_path = tmp_path / "archive"
    process_a = _seed_process(active_path, archive_path)
    process_b = _read_process(active_path, archive_path)
    assert process_a == process_b
    assert json.loads(process_b)["execution_id"] == "process-a"
    assert json.loads(process_b)["tenant_id"] == "tenant-A"


def _concurrent_script(mode: str) -> str:
    return f"""
        import json
        import sys
        from datetime import UTC, datetime
        from pathlib import Path
        from trendx.forecasting.execution import DurableExecutionStore
        from trendx.forecasting.lifecycle import (
            ExecutionHistoryLifecycle,
            ExecutionRetentionPolicy,
            FileSystemArchiveStore,
        )

        store = DurableExecutionStore(Path(sys.argv[1]))
        archive = FileSystemArchiveStore(Path(sys.argv[2]))
        lifecycle = ExecutionHistoryLifecycle(
            store,
            archive,
            lambda: datetime(2026, 3, 2, tzinfo=UTC),
        )
        policy = ExecutionRetentionPolicy(retention_days=30, dry_run=False)
        if {mode!r} == "preview":
            report = lifecycle.preview("tenant-A", policy)
            print(json.dumps(report.to_dict(), sort_keys=True))
        elif {mode!r} == "archive":
            report = lifecycle.archive_eligible("tenant-A", policy)
            print(json.dumps({{"created": report.archived, "already": len(report.already_archived), "failed": report.failed}}, sort_keys=True))
        else:
            report = lifecycle.purge_eligible("tenant-A", policy)
            print(json.dumps({{"purged": report.purged, "already_purged": report.already_purged, "failed": report.failed}}, sort_keys=True))
    """


def _run_two(mode: str, active_path: Path, archive_path: Path) -> list[str]:
    processes = [
        subprocess.Popen(
            [
                sys.executable,
                "-c",
                textwrap.dedent(_concurrent_script(mode)),
                str(active_path),
                str(archive_path),
            ],
            cwd=ROOT,
            env=_env(),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        for _ in range(2)
    ]
    outputs: list[str] = []
    for process in processes:
        stdout, stderr = process.communicate(timeout=60)
        assert process.returncode == 0, stderr
        outputs.append(stdout.strip())
    return outputs


def test_w107_concurrent_archive(tmp_path: Path) -> None:
    active_path = tmp_path / "active.json"
    archive_path = tmp_path / "archive"
    store = DurableExecutionStore(active_path)
    _seed_one(store, "concurrent-archive")
    store.close()
    outputs = _run_two("archive", active_path, archive_path)
    decoded = [json.loads(output) for output in outputs]
    assert sorted((item["created"], item["already"]) for item in decoded) == [(0, 1), (1, 0)]
    archive = FileSystemArchiveStore(archive_path)
    assert archive.verify("concurrent-archive").valid
    reopened = DurableExecutionStore(active_path)
    try:
        assert reopened.get("concurrent-archive") is not None
    finally:
        reopened.close()


def test_w107_concurrent_purge_is_single_delete(tmp_path: Path) -> None:
    active_path = tmp_path / "active.json"
    archive_path = tmp_path / "archive"
    store = DurableExecutionStore(active_path)
    _seed_one(store, "concurrent-purge")
    store.close()
    outputs = _run_two("purge", active_path, archive_path)
    decoded = [json.loads(output) for output in outputs]
    assert sum(item["purged"] for item in decoded) == 1
    assert sum(item["already_purged"] for item in decoded) <= 1
    assert all(item["failed"] == 0 for item in decoded)
    assert all(item["purged"] <= 1 for item in decoded)
    reopened = DurableExecutionStore(active_path)
    try:
        assert reopened.get("concurrent-purge") is None
    finally:
        reopened.close()
    assert FileSystemArchiveStore(archive_path).verify("concurrent-purge").valid


def test_w107_concurrent_preview_is_read_only(tmp_path: Path) -> None:
    active_path = tmp_path / "active.json"
    archive_path = tmp_path / "archive"
    store = DurableExecutionStore(active_path)
    _seed_one(store, "preview-1")
    _seed_one(store, "preview-2")
    store.close()
    before = active_path.read_bytes()
    outputs = _run_two("preview", active_path, archive_path)
    assert outputs[0] == outputs[1]
    assert active_path.read_bytes() == before


def test_w107_performance_10_100_500(tmp_path: Path) -> None:
    observed: list[dict[str, Any]] = []
    for size in (10, 100, 500):
        store = MemoryExecutionStore()
        for index in range(size):
            _seed_one(store, f"perf-{size}-{index}")
        archive = FileSystemArchiveStore(tmp_path / f"archive-{size}")
        lifecycle = ExecutionHistoryLifecycle(
            store, archive, lambda: datetime(2026, 3, 2, tzinfo=UTC)
        )
        tracemalloc.start()
        started = time.perf_counter()
        preview = lifecycle.preview(
            "tenant-A", ExecutionRetentionPolicy(retention_days=RETENTION_DAYS, dry_run=True)
        )
        preview_seconds = time.perf_counter() - started
        started = time.perf_counter()
        purge = lifecycle.purge_eligible(
            "tenant-A", ExecutionRetentionPolicy(retention_days=RETENTION_DAYS, dry_run=False)
        )
        purge_seconds = time.perf_counter() - started
        _, peak_bytes = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        assert preview.scanned == size
        assert preview.eligible == size
        assert purge.archived == size
        assert purge.purged == size
        observed.append(
            {
                "records": size,
                "preview_seconds": preview_seconds,
                "purge_seconds": purge_seconds,
                "peak_bytes": peak_bytes,
                "archive_operations": purge.archived,
                "purge_operations": purge.purged,
            }
        )
    assert [item["records"] for item in observed] == [10, 100, 500]
    assert all(item["peak_bytes"] >= 0 for item in observed)
