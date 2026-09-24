"""W108 operational recovery checks with real processes and temporary data."""

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
    ExecutionRecord,
    ExecutionStatus,
    MemoryExecutionStore,
)
from trendx.forecasting.history import ExecutionHistoryService, ExecutionQuery
from trendx.forecasting.lifecycle import ExecutionRetentionPolicy, FileSystemArchiveStore
from trendx.forecasting.recovery import ExecutionRestoreRequest, RecoveryService

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
NOW = datetime(2026, 3, 2, tzinfo=UTC)
POLICY = ExecutionRetentionPolicy(retention_days=30, dry_run=False)


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


def _record(execution_id: str, tenant_id: str = "tenant-A") -> ExecutionRecord:
    return ExecutionRecord(
        execution_id=execution_id,
        reference_key=f"w108:{execution_id}",
        tenant_id=tenant_id,
        entity_type="DEVICE",
        entity_id=f"device-{execution_id}",
        target_metric="temperature",
        frequency="1h",
        horizon=24,
        model_id="model-w108",
        model_version="1",
        algorithm="Fourier",
        feature_schema_version="schema-w108",
        feature_schema_fingerprint="fingerprint-w108",
        artifact_uri="file:///w108/model.bin",
        status=ExecutionStatus.SUCCESS,
        created_at="2025-01-01T00:00:00Z",
        started_at="2025-01-01T00:00:01Z",
        completed_at="2025-01-01T00:00:02Z",
        prediction_count=1,
        metadata={"source": "w108-ops"},
        result={"values": [1.0]},
    )


def _archive_records(
    records: tuple[ExecutionRecord, ...],
    root: Path,
) -> tuple[FileSystemArchiveStore, dict[str, str]]:
    archive = FileSystemArchiveStore(root)
    archive.archive(records, policy=POLICY, archived_at=NOW)
    checksums = {}
    for record in records:
        verification = archive.verify(record.execution_id)
        assert verification.valid and verification.checksum is not None
        checksums[record.execution_id] = verification.checksum
    return archive, checksums


def test_w108_process_a_b_c(tmp_path: Path) -> None:
    active_path = tmp_path / "active.json"
    archive_path = tmp_path / "archive"
    process_a = _run(
        """
        import json
        import sys
        from datetime import UTC, datetime
        from pathlib import Path
        from trendx.forecasting.execution import ExecutionRecord, ExecutionStatus
        from trendx.forecasting.lifecycle import ExecutionRetentionPolicy, FileSystemArchiveStore

        record = ExecutionRecord(
            execution_id="process-a",
            reference_key="w108:process-a",
            tenant_id="tenant-A",
            entity_type="DEVICE",
            entity_id="device-a",
            target_metric="temperature",
            frequency="1h",
            horizon=24,
            model_id="model-w108",
            model_version="1",
            algorithm="Fourier",
            status=ExecutionStatus.SUCCESS,
            created_at="2025-01-01T00:00:00Z",
            started_at="2025-01-01T00:00:01Z",
            completed_at="2025-01-01T00:00:02Z",
            prediction_count=1,
            metadata={"source": "process-a"},
            result={"values": [1.0]},
        )
        archive = FileSystemArchiveStore(Path(sys.argv[1]))
        archive.archive((record,), policy=ExecutionRetentionPolicy(retention_days=30, dry_run=False), archived_at=datetime(2026, 3, 2, tzinfo=UTC))
        checksum = archive.verify(record.execution_id).checksum
        print(json.dumps({"record": record.to_dict(), "checksum": checksum}, sort_keys=True))
        """,
        str(archive_path),
    )
    archive_info = json.loads(process_a)
    checksum = archive_info["checksum"]
    process_b = _run(
        """
        import json
        import sys
        from datetime import UTC, datetime
        from pathlib import Path
        from trendx.forecasting.execution import DurableExecutionStore
        from trendx.forecasting.lifecycle import FileSystemArchiveStore
        from trendx.forecasting.recovery import ExecutionRestoreRequest, RecoveryService

        store = DurableExecutionStore(Path(sys.argv[1]))
        archive = FileSystemArchiveStore(Path(sys.argv[2]))
        result = RecoveryService(store, archive, lambda: datetime(2026, 3, 2, tzinfo=UTC)).restore(
            ExecutionRestoreRequest(
                execution_id="process-a",
                tenant_id="tenant-A",
                expected_archive_checksum=sys.argv[3],
            )
        )
        assert result.status.value == "RESTORED"
        print(json.dumps(store.get("process-a").to_dict(), sort_keys=True))
        """,
        str(active_path),
        str(archive_path),
        checksum,
    )
    process_c = _run(
        """
        import json
        import sys
        from pathlib import Path
        from trendx.forecasting.execution import DurableExecutionStore
        from trendx.forecasting.history import ExecutionHistoryService, ExecutionQuery

        service = ExecutionHistoryService(DurableExecutionStore(Path(sys.argv[1])))
        record = service.list_records(ExecutionQuery(tenant_id="tenant-A"))[0]
        print(json.dumps(record.to_dict(), sort_keys=True))
        """,
        str(active_path),
    )
    assert json.loads(process_b) == archive_info["record"]
    assert json.loads(process_c) == archive_info["record"]


def _restore_script() -> str:
    return """
        import json
        import sys
        from datetime import UTC, datetime
        from pathlib import Path
        from trendx.forecasting.execution import DurableExecutionStore
        from trendx.forecasting.lifecycle import FileSystemArchiveStore
        from trendx.forecasting.recovery import ExecutionRestoreRequest, RecoveryService

        store = DurableExecutionStore(Path(sys.argv[1]))
        archive = FileSystemArchiveStore(Path(sys.argv[2]))
        result = RecoveryService(store, archive, lambda: datetime(2026, 3, 2, tzinfo=UTC)).restore(
            ExecutionRestoreRequest(
                execution_id="concurrent",
                tenant_id="tenant-A",
                expected_archive_checksum=sys.argv[3],
            )
        )
        print(json.dumps({"status": result.status.value, "restored": result.record_restored}, sort_keys=True))
    """


def test_w108_concurrent_restore(tmp_path: Path) -> None:
    active_path = tmp_path / "active.json"
    archive_path = tmp_path / "archive"
    record = _record("concurrent")
    archive, checksums = _archive_records((record,), archive_path)
    before_hash = hashlib_sha256(archive._path_for(record.execution_id))
    processes = [
        subprocess.Popen(
            [
                sys.executable,
                "-c",
                textwrap.dedent(_restore_script()),
                str(active_path),
                str(archive_path),
                checksums[record.execution_id],
            ],
            cwd=ROOT,
            env=_env(),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        for _ in range(2)
    ]
    outputs = []
    for process in processes:
        stdout, stderr = process.communicate(timeout=60)
        assert process.returncode == 0, stderr
        outputs.append(json.loads(stdout))
    assert sorted(item["status"] for item in outputs) == ["ALREADY_PRESENT", "RESTORED"]
    assert sum(item["restored"] for item in outputs) == 1
    store = DurableExecutionStore(active_path)
    try:
        assert store.get(record.execution_id) == record
    finally:
        store.close()
    assert hashlib_sha256(archive._path_for(record.execution_id)) == before_hash
    assert archive.verify(record.execution_id).valid


def test_w108_w99_read_during_restore(tmp_path: Path) -> None:
    record = _record("read-during")
    archive, checksums = _archive_records((record,), tmp_path / "archive")
    active_path = tmp_path / "active.json"
    store = DurableExecutionStore(active_path)
    history = ExecutionHistoryService(store)
    failures: list[Exception] = []

    def read_history() -> None:
        try:
            for _ in range(30):
                page = history.query(ExecutionQuery(tenant_id="tenant-A"))
                assert page.total in {0, 1}
                if page.total:
                    assert page.records[0].to_dict() == record.to_dict()
        except Exception as exc:  # pragma: no cover - asserted below
            failures.append(exc)

    from concurrent.futures import ThreadPoolExecutor

    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            reader = executor.submit(read_history)
            result = RecoveryService(
                store,
                archive,
                lambda: NOW,
            ).restore(
                ExecutionRestoreRequest(
                    execution_id=record.execution_id,
                    tenant_id=record.tenant_id,
                    expected_archive_checksum=checksums[record.execution_id],
                )
            )
            reader.result()
        assert result.status.value == "RESTORED"
        assert failures == []
    finally:
        store.close()


def test_w108_performance(tmp_path: Path) -> None:
    observations: list[dict[str, Any]] = []
    for size in (10, 100, 500):
        records = tuple(_record(f"perf-{size}-{index}") for index in range(size))
        archive, checksums = _archive_records(records, tmp_path / f"archive-{size}")
        target = MemoryExecutionStore()
        service = RecoveryService(target, archive, lambda: NOW)
        requests = tuple(
            ExecutionRestoreRequest(
                execution_id=record.execution_id,
                tenant_id=record.tenant_id,
                expected_archive_checksum=checksums[record.execution_id],
            )
            for record in records
        )
        tracemalloc.start()
        verify_started = time.perf_counter()
        for request in requests:
            assert service.verify_archive(request).status.value == "VERIFIED"
        verify_seconds = time.perf_counter() - verify_started
        restore_started = time.perf_counter()
        for request in requests:
            assert service.restore(request).status.value == "RESTORED"
        restore_seconds = time.perf_counter() - restore_started
        read_started = time.perf_counter()
        assert ExecutionHistoryService(target).count(ExecutionQuery(tenant_id="tenant-A")) == size
        read_seconds = time.perf_counter() - read_started
        _, peak_bytes = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        observations.append(
            {
                "records": size,
                "verify_seconds": verify_seconds,
                "restore_seconds": restore_seconds,
                "w99_read_seconds": read_seconds,
                "peak_bytes": peak_bytes,
                "restored": size,
            }
        )
    assert [item["records"] for item in observations] == [10, 100, 500]
    assert all(item["restored"] == item["records"] for item in observations)
    assert all(item["peak_bytes"] >= 0 for item in observations)


def hashlib_sha256(path: Path) -> str:
    import hashlib

    return hashlib.sha256(path.read_bytes()).hexdigest()
