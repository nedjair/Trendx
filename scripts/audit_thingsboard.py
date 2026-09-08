#!/usr/bin/env python3
"""Audit ThingsBoard en lecture seule - Phase 1."""

import json
import ssl
import sys
import urllib.request
from collections import Counter
from datetime import UTC


def get_env_var(key, filepath=".env"):
    with open(filepath) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                if k == key:
                    return v.strip()
    return None


def main():
    tb_base_url = get_env_var("TB_BASE_URL") or "https://10.0.0.1:8081"
    tb_username = get_env_var("TB_USERNAME") or ""
    tb_password = get_env_var("TB_PASSWORD") or ""

    print(f"=== ThingsBoard Base URL: {tb_base_url} ===")
    print()

    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE

    # 1. Version / info endpoint
    print("=== Version ThingsBoard ===")
    try:
        req = urllib.request.Request(f"{tb_base_url.rstrip('/')}/api/info")
        with urllib.request.urlopen(req, context=ctx, timeout=10) as resp:
            data = json.loads(resp.read().decode())
            print(json.dumps(data, indent=2))
    except Exception as e:
        print(f"[WARN] /api/info: {type(e).__name__}: {e}")
    print()

    # 2. Authentification
    print("=== Authentification ===")
    token = None
    try:
        login_data = json.dumps({"username": tb_username, "password": tb_password}).encode()
        req = urllib.request.Request(
            f"{tb_base_url.rstrip('/')}/api/auth/login",
            data=login_data,
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req, context=ctx, timeout=10) as resp:
            auth_data = json.loads(resp.read().decode())
            token = auth_data.get("token", "")
            refresh_token = auth_data.get("refreshToken", "")
            print(f"[OK] Authentification réussie. Token len={len(token)}")
            print(f"     Refresh token: {bool(refresh_token)}")
    except Exception as e:
        print(f"[ERREUR] Auth: {type(e).__name__}: {e}")
    print()

    if not token:
        print("Arrêt - pas de token valide.")
        return 1

    auth_headers = {
        "Content-Type": "application/json",
        "X-Authorization": f"Bearer {token}",
    }

    def api_get(path, params=None):
        url = f"{tb_base_url.rstrip('/')}{path}"
        if params:
            from urllib.parse import urlencode

            url += "?" + urlencode(params)
        req = urllib.request.Request(url, headers=auth_headers)
        with urllib.request.urlopen(req, context=ctx, timeout=20) as resp:
            return json.loads(resp.read().decode())

    # 3. Devices
    print("=== Devices ===")
    all_devices = []
    try:
        page = 0
        page_size = 100
        while True:
            data = api_get("/api/tenant/devices", {"pageSize": page_size, "page": page})
            batch = data.get("data", [])
            all_devices.extend(batch)
            if page == 0:
                print(f"Total annoncé: {data.get('totalElements', 0)}")
            if len(batch) < page_size:
                break
            page += 1
            if page > 50:
                break

        print(f"Récupérés: {len(all_devices)}")
        types = Counter(d.get("type", "unknown") for d in all_devices)
        for t, c in sorted(types.items()):
            print(f"  - {t}: {c}")

        print("\nÉchantillon:")
        for d in all_devices[:8]:
            did = d.get("id", {}).get("id", "?")
            print(f"  * {d.get('name','?')} | type={d.get('type','?')} | id={did[:8]}...")
    except Exception as e:
        print(f"[ERREUR] Devices: {type(e).__name__}: {e}")
    print()

    # 4. Assets
    print("=== Assets ===")
    all_assets = []
    try:
        page = 0
        page_size = 100
        while True:
            data = api_get("/api/tenant/assets", {"pageSize": page_size, "page": page})
            batch = data.get("data", [])
            all_assets.extend(batch)
            if page == 0:
                print(f"Total annoncé: {data.get('totalElements', 0)}")
            if len(batch) < page_size:
                break
            page += 1
            if page > 50:
                break
        print(f"Récupérés: {len(all_assets)}")
        types = Counter(a.get("type", "unknown") for a in all_assets)
        for t, c in sorted(types.items()):
            print(f"  - {t}: {c}")
    except Exception as e:
        print(f"[ERREUR] Assets: {type(e).__name__}: {e}")
    print()

    # 5. Device Profiles
    print("=== Device Profiles ===")
    profiles = []
    try:
        data = api_get("/api/deviceProfiles", {"pageSize": 100, "page": 0})
        profiles = data.get("data", [])
        print(f"Profils: {len(profiles)}")
        for p in profiles:
            pid = p.get("id", {}).get("id", "?")
            print(f"  - {p.get('name','?')} (id={pid[:8]}...)")
    except Exception as e:
        print(f"[ERREUR] Profiles: {type(e).__name__}: {e}")
    print()

    # 6. Customers
    print("=== Customers ===")
    customers = []
    try:
        data = api_get("/api/customers", {"pageSize": 100, "page": 0})
        customers = data.get("data", [])
        print(f"Customers: {len(customers)}")
        for c in customers:
            cid = c.get("id", {}).get("id", "?")
            print(f"  - {c.get('title','?')} | {c.get('email','?')} (id={cid[:8]}...)")
    except Exception as e:
        print(f"[ERREUR] Customers: {type(e).__name__}: {e}")
    print()

    # 7. Timeseries keys - échantillon
    print("=== Clés de télémétrie (échantillon 15 premiers devices) ===")
    all_keys_global = Counter()
    device_keys = {}
    sample_devices = all_devices[:15] if all_devices else []
    for d in sample_devices:
        did = d.get("id", {}).get("id", "")
        etype = d.get("id", {}).get("entityType", "DEVICE")
        dname = d.get("name", "?")
        try:
            keys = api_get(f"/api/plugins/telemetry/{etype}/{did}/keys/timeseries")
            if keys:
                device_keys[dname] = keys
                for k in keys:
                    all_keys_global[k] += 1
        except Exception:
            pass

    for dname, keys in device_keys.items():
        keys_str = ", ".join(keys[:12])
        if len(keys) > 12:
            keys_str += "..."
        print(f"  * {dname}: {len(keys)} clés - {keys_str}")

    print("\n  Clés les plus fréquentes:")
    for k, c in all_keys_global.most_common(30):
        print(f"    - {k}: {c} devices")
    print()

    # 8. Données récentes pour le device MVP cible
    print("=== Données récentes device MVP (batterylevel) ===")
    target_device_id = get_env_var("TB_DEVICE_ID")
    target_metric = get_env_var("TB_METRIC_NAME")
    if target_device_id and target_device_id != "replace_me":
        try:
            from datetime import datetime, timedelta

            now = datetime.now(UTC)
            start = now - timedelta(days=7)
            params = {
                "keys": target_metric,
                "startTs": int(start.timestamp() * 1000),
                "endTs": int(now.timestamp() * 1000),
                "limit": 10000,
            }
            data = api_get(
                f"/api/plugins/telemetry/DEVICE/{target_device_id}/values/timeseries", params
            )
            vals = data.get(target_metric, [])
            print(f"Points récupérés (7 derniers jours): {len(vals)}")
            if vals:
                numeric_vals = []
                for v in vals:
                    try:
                        numeric_vals.append(float(v.get("value", 0)))
                    except Exception:
                        pass
                if numeric_vals:
                    print(f"  Min: {min(numeric_vals):.2f}")
                    print(f"  Max: {max(numeric_vals):.2f}")
                    print(f"  Moy: {sum(numeric_vals)/len(numeric_vals):.2f}")
                print(f"  Premier point: {vals[-1].get('ts','?')}")
                print(f"  Dernier point: {vals[0].get('ts','?')}")
        except Exception as e:
            print(f"[ERREUR] Télémétrie MVP: {type(e).__name__}: {e}")
    else:
        print("Pas de TB_DEVICE_ID configuré, skip.")
    print()

    # 9. Relations: échantillon sur quelques devices
    print("=== Relations (échantillon) ===")
    relation_count = 0
    for d in all_devices[:5]:
        did = d.get("id", {}).get("id", "")
        dname = d.get("name", "?")
        try:
            data = api_get("/api/relations/info", {"fromId": did, "fromType": "DEVICE"})
            if data:
                print(f"  * {dname}: {len(data)} relations sortantes")
                relation_count += len(data)
            data2 = api_get("/api/relations/info", {"toId": did, "toType": "DEVICE"})
            if data2:
                print(f"  * {dname}: {len(data2)} relations entrantes")
                relation_count += len(data2)
        except Exception:
            pass
    print(f"  Total relations vues: {relation_count}")
    print()

    # 10. Attributs: échantillon
    print("=== Attributs (échantillon) ===")
    for d in all_devices[:3]:
        did = d.get("id", {}).get("id", "")
        dname = d.get("name", "?")
        try:
            server_attrs = api_get(
                f"/api/plugins/telemetry/DEVICE/{did}/values/attributes/SERVER_SCOPE"
            )
            client_attrs = api_get(
                f"/api/plugins/telemetry/DEVICE/{did}/values/attributes/CLIENT_SCOPE"
            )
            shared_attrs = api_get(
                f"/api/plugins/telemetry/DEVICE/{did}/values/attributes/SHARED_SCOPE"
            )
            print(
                f"  * {dname}: server={len(server_attrs)}, client={len(client_attrs)}, shared={len(shared_attrs)}"
            )
            if server_attrs:
                for a in server_attrs[:5]:
                    print(
                        f"    SERVER: {a.get('key')} = {a.get('value')[:60] if isinstance(a.get('value'),str) else a.get('value')}"
                    )
        except Exception:
            pass
    print()

    print("=== FIN DE L'AUDIT ===")
    return 0


if __name__ == "__main__":
    sys.exit(main())
