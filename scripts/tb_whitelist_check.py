#!/usr/bin/env python3
"""Vérifie que le client ThingsBoard applique bien la whitelist (GET uniquement + POST /api/auth/login).

Sort :
- OK    si un POST quelconque (hors /api/auth/login) est rejeté par la whitelist (code 0) ;
- FAIL  sinon (code 1).
Ne touche pas à ThingsBoard ; aucune écriture.
"""
import asyncio
import sys
from pathlib import Path

# Allow running from repo root without PYTHONPATH
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from trendx.thingsboard.client import ThingsBoardClient, ThingsBoardWriteDisabledError


async def main() -> int:
    client = ThingsBoardClient(
        base_url="http://test.tb:8080",
        username="test@test.com",
        password="test_pass",
        request_timeout=10,
        retry_max_attempts=1,
        retry_backoff_seconds=1,
        jwt_leeway_seconds=300,
        page_size=10,
    )
    client._token = "test_token"
    client._token_expiry = None  # force re-auth path for completeness

    # Tentative d'écriture via _request (POST hors /api/auth/login) -> doit lever ThingsBoardWriteDisabledError
    try:
        await client._request(
            "POST",
            "/api/plugins/telemetry/DEVICE/dev-001/timeseries/ANY",
            json_data=[{"ts": 1700000000000, "value": 22.5}],
        )
        print("FAIL — whitelist inactive : POST telemetry non rejeté")
        return 1
    except ThingsBoardWriteDisabledError:
        print("OK — whitelist active : POST telemetry rejeté")
        return 0
    except Exception as exc:
        print(f"FAIL — exception inattendue : {exc}")
        return 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
