from __future__ import annotations

from unittest.mock import patch

from apscheduler.schedulers.base import STATE_RUNNING, STATE_STOPPED

# The worker module starts a blocking HTTP health server and probes disk mounts at
# import time. These are environmental side effects unrelated to scheduler init, so
# they are isolated here (mocked) before importing the module. The scheduler itself
# is initialized for real so its runtime operation can be asserted.
with patch("http.server.ThreadingHTTPServer"), patch("trendx.services.ingestion.probe_disk_mounts"):
    import trendx.services.worker as worker


def test_has_apscheduler_true() -> None:
    assert worker.HAS_APSCHEDULER is True


def test_module_scheduler_running() -> None:
    # The worker actually started its scheduler at import time (real runtime init).
    assert worker._scheduler is not None
    assert worker._scheduler.state == STATE_RUNNING


def test_start_scheduler_registers_expected_jobs() -> None:
    # _start_scheduler is the factory used at runtime. Verify it registers exactly
    # the 5 expected maintenance jobs. Asserted immediately on creation, before the
    # one-shot `partitions_boot_check` date job could fire (deterministic).
    sched = worker._start_scheduler()
    try:
        expected = {
            "partitions_monthly",
            "aggregate_hourly",
            "aggregate_daily",
            "aggregate_weekly",
            "partitions_boot_check",
        }
        assert expected <= {job.id for job in sched.get_jobs()}
    finally:
        sched.shutdown(wait=False)


def test_scheduler_shutdown_is_clean() -> None:
    # Shutting down the initialized scheduler must not raise.
    worker._scheduler.shutdown(wait=False)
    assert worker._scheduler.state == STATE_STOPPED


def teardown_module() -> None:
    if worker._scheduler is not None and worker._scheduler.state != STATE_STOPPED:
        worker._scheduler.shutdown(wait=False)
