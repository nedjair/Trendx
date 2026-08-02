#!/usr/bin/env python3
"""Audit complémentaire : version TB, données device alternatif riche, SSH 10.0.0.1 check infos."""
import urllib.request
import json
import ssl
import sys


def get_env_var(key, filepath=".env"):
    with open(filepath, "r") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                if k == key:
                    return v.strip()
    return None


def main():
    TB_BASE_URL = get_env_var("TB_BASE_URL") or "https://10.0.0.1:8081"
    TB_USERNAME = get_env_var("TB_USERNAME") or ""
    TB_PASSWORD = get_env_var("TB_PASSWORD") or ""

    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE

    def api_get(path, headers_extra=None, params=None):
        url = f"{TB_BASE_URL.rstrip('/')}{path}"
        if params:
            from urllib.parse import urlencode
            url += "?" + urlencode(params)
        h = {"Content-Type": "application/json"}
        if headers_extra:
            h.update(headers_extra)
        req = urllib.request.Request(url, headers=h)
        with urllib.request.urlopen(req, context=ctx, timeout=20) as resp:
            return json.loads(resp.read().decode()), resp.headers, resp.status

    # Auth
    login_data = json.dumps({"username": TB_USERNAME, "password": TB_PASSWORD}).encode()
    req = urllib.request.Request(
        f"{TB_BASE_URL.rstrip('/')}/api/auth/login",
        data=login_data,
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, context=ctx, timeout=10) as resp:
        auth_data = json.loads(resp.read().decode())
        token = auth_data.get("token", "")

    auth_headers = {"X-Authorization": f"Bearer {token}"}

    # 1. Version via d'autres endpoints avec analyse des headers
    print("=== Récupération version ThingsBoard ===")
    for method, path, body in [
        ("GET", "/api/info", None),
        ("GET", "/api/session", None),
        ("GET", "/api/vault/config", None),
        ("POST", "/api/auth/logout", "{}"),
    ]:
        try:
            url = f"{TB_BASE_URL.rstrip('/')}{path}"
            data = body.encode() if body else None
            req = urllib.request.Request(url, data=data, method=method,
                                         headers=auth_headers)
            with urllib.request.urlopen(req, context=ctx, timeout=10) as resp:
                headers = dict(resp.headers)
                # Chercher la version dans les headers ou le body
                version_found = ""
                for k, v in headers.items():
                    if "version" in k.lower() or "x-" in k.lower():
                        version_found += f" {k}={v[:80]};"
                body_raw = resp.read().decode()[:200]
                print(f"  {method} {path} -> status={resp.status} headers_version={version_found} body_preview={body_raw}")
        except urllib.error.HTTPError as e:
            error_body = ""
            try:
                error_body = e.read().decode()[:200]
            except Exception:
                pass
            print(f"  {method} {path} -> HTTP {e.code} body={error_body}")
        except Exception as e:
            print(f"  {method} {path} -> {type(e).__name__}: {e}")
    print()

    # 2. Données pour le device ALG16025001 (292 clés, le plus riche)
    print("=== Données device ALG16025001 (plus riche) ===")
    try:
        devices_data, _, _ = api_get("/api/tenant/devices",
                                     headers_extra=auth_headers,
                                     params={"pageSize": 100, "page": 0})
        target_dev = None
        for d in devices_data.get("data", []):
            if d.get("name") == "ALG16025001":
                target_dev = d
                break
        if not target_dev:
            print("[ERREUR] Device ALG16025001 non trouvé")
            return 1
        did = target_dev["id"]["id"]
        print(f"Device '{target_dev['name']}' | type={target_dev['type']} | id={did[:12]}...")

        from datetime import datetime, timedelta, timezone
        now = datetime.now(timezone.utc)
        start30 = now - timedelta(days=30)

        keys_ts, _, _ = api_get(f"/api/plugins/telemetry/DEVICE/{did}/keys/timeseries",
                                headers_extra=auth_headers)
        print(f"Total clés: {len(keys_ts)}")

        # Focus sur les clés battery, énergie, température, les plus importantes
        priority_keys = [k for k in keys_ts if any(kw in k.lower() for kw in
            ["battery_soc", "battery_v", "battery_i", "battery_p", "battery_ce",
             "battery_t", "state_of_charge", "voltage", "current", "power",
             "temperature", "humidity", "yield", "energy", "consumption",
             "solar", "frequency"])]
        # Limiter à 40 clés pour ne pas surcharger
        priority_keys = priority_keys[:40]
        if not priority_keys:
            priority_keys = keys_ts[:30]

        print(f"\nAnalyse de {len(priority_keys)} clés prioritaires sur 30 jours:")
        for i in range(0, len(priority_keys), 10):
            batch = priority_keys[i:i+10]
            params = {
                "keys": ",".join(batch),
                "startTs": int(start30.timestamp() * 1000),
                "endTs": int(now.timestamp() * 1000),
                "limit": 10000,
            }
            try:
                data, _, _ = api_get(
                    f"/api/plugins/telemetry/DEVICE/{did}/values/timeseries",
                    headers_extra=auth_headers, params=params)
                for k in batch:
                    vals = data.get(k, [])
                    n = len(vals)
                    if n > 0:
                        numeric_vals = []
                        for v in vals:
                            try:
                                numeric_vals.append(float(v.get("value", 0)))
                            except Exception:
                                pass
                        if numeric_vals:
                            print(f"  {k}: {n} pts | min={min(numeric_vals):.2f} | max={max(numeric_vals):.2f} | moy={sum(numeric_vals)/len(numeric_vals):.2f} | dernier={vals[0].get('ts','?')}")
                        else:
                            print(f"  {k}: {n} pts (non numérique)")
                    else:
                        # Essayer avec intervalle 1 an
                        start365 = now - timedelta(days=365)
                        params2 = {
                            "keys": k,
                            "startTs": int(start365.timestamp() * 1000),
                            "endTs": int(now.timestamp() * 1000),
                            "limit": 10,
                        }
                        try:
                            data2, _, _ = api_get(
                                f"/api/plugins/telemetry/DEVICE/{did}/values/timeseries",
                                headers_extra=auth_headers, params=params2)
                            n2 = len(data2.get(k, []))
                            print(f"  {k}: 0/30j, {n2}/1an")
                        except Exception:
                            print(f"  {k}: 0/30j")
            except Exception as e:
                print(f"  [ERREUR]: {type(e).__name__}")
    except Exception as e:
        print(f"[ERREUR]: {type(e).__name__}: {e}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
