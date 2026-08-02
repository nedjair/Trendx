from __future__ import annotations

import os
import socket
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from loguru import logger

from trendx.config import settings

EXECUTOR_PORT = int(os.environ.get("SERVER_PORT", settings.trendx_python_executor_port))

logger.remove()
logger.add(
    sys.stderr,
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | trendx-worker | <level>{message}</level>",
    colorize=True,
)


class _HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802
        try:
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"status":"ok","service":"trendx-worker","port":' + str(EXECUTOR_PORT).encode() + b'}')
        except Exception:  # noqa: BLE001
            pass

    def log_message(self, format: *args: Any, **kwargs: Any) -> None:  # noqa: A003,ARG002
        return


def _start_executor_http_server(port: int) -> None:
    addr = ("0.0.0.0", port)
    retries = 0
    while retries < 10:
        try:
            srv = ThreadingHTTPServer(addr, _HealthHandler)
            logger.info(f"trendx-worker executor HTTP listen on :{port}")
            srv.serve_forever()
            return
        except OSError as exc:
            retries += 1
            logger.warning(f"executor HTTP bind error port {port} (retry {retries}/10): {exc}")
            time.sleep(2)
    logger.error(f"Unable to bind executor HTTP server on port {port}")


_health_server_thread = threading.Thread(
    target=_start_executor_http_server,
    args=(EXECUTOR_PORT,),
    name="trendx-worker-health-server",
    daemon=True,
)
_health_server_thread.start()


def task_ping(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    return {
        "status": "pong",
        "ts": time.time(),
        "service": "trendx-worker",
        "executor_engine": int(os.environ.get("EXECUTOR_SCRIPT_ENGINE", "6")),
        "throttling_capacity": int(os.environ.get("THROTTLING_QUEUE_CAPACITY", "10")),
    }


def task_noop() -> bool:
    return True


def task_forecast_dryrun(device_id: str, metric_name: str, horizon: int = 24) -> dict[str, Any]:
    logger.info(f"Prophet dryrun requested device={device_id} metric={metric_name} horizon={horizon}")
    return {
        "ok": True,
        "placeholder": True,
        "note": "Phase 2 infrastructure only. Model training arrives in Phase 3.",
        "device_id": device_id,
        "metric_name": metric_name,
        "horizon": horizon,
    }


logger.info(
    "trendx-worker initialized (APScheduler process, no broker). port={port}, engine={engine}",
    port=EXECUTOR_PORT,
    engine=os.environ.get("EXECUTOR_SCRIPT_ENGINE", "6"),
)
