#!/usr/bin/env python3
"""Audit complémentaire - version TB, clés du device MVP, données."""
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
    TARGET_DEVICE_ID = get_env_var("TB_DEVICE_ID")

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
            return json.loads(resp.read().decode())

    # Authentification
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

    # 1. Récupérer la version via endpoint nécessitant une auth
    print("=== Tentatives de récupération de la version ThingsBoard ===")
    for endpoint in [
        "/api/info",
        "/api/auth/logout",
        "/api/user",
        "/api/usage/features",
        "/api/tenant",
    ]:
        try:
            data = api_get(endpoint, headers_extra=auth_headers)
            print(f"  {endpoint}: OK (type={type(data).__name__})")
            if isinstance(data, dict):
                for k in ["version", "build", "additionalInfo", "name", "title"]:
                    if k in data:
                        print(f"    -> {k} = {str(data[k])[:120]}")
        except Exception as e:
            print(f"  {endpoint}: {type(e).__name__}")
    print()

    # 2. Device MVP cible : toutes les clés
    if TARGET_DEVICE_ID and TARGET_DEVICE_ID != "replace_me":
        print(f"=== Clés télémétriques du device MVP ID={TARGET_DEVICE_ID[:12]}... ===")
        try:
            # Infos sur le device
            device_info = api_get(f"/api/device/{TARGET_DEVICE_ID}", headers_extra=auth_headers)
            print(f"Nom: {device_info.get('name','?')}")
            print(f"Type: {device_info.get('type','?')}")
            label = device_info.get('label', '')
            if label:
                print(f"Label: {label}")

            # Clés timeseries
            keys_ts = api_get(f"/api/plugins/telemetry/DEVICE/{TARGET_DEVICE_ID}/keys/timeseries",
                              headers_extra=auth_headers)
            print(f"\nClés timeseries ({len(keys_ts)}):")
            for k in sorted(keys_ts):
                print(f"  - {k}")

            # Clés attributes
            for scope in ["SERVER_SCOPE", "CLIENT_SCOPE", "SHARED_SCOPE"]:
                attrs = api_get(
                    f"/api/plugins/telemetry/DEVICE/{TARGET_DEVICE_ID}/keys/attributes/{scope}",
                    headers_extra=auth_headers)
                if attrs:
                    print(f"\nAttributs {scope} ({len(attrs)}):")
                    for a in attrs:
                        print(f"  - {a}")

            # 3. Données historiques sur 90j pour chaque clé numérique
            print("\n=== Données 90j pour chaque clé (présence + stats) ===")
            from datetime import datetime, timedelta, timezone
            now = datetime.now(timezone.utc)
            start90 = now - timedelta(days=90)

            # Regrouper par lots de 10 clés max
            for i in range(0, min(len(keys_ts), 60), 10):
                batch = keys_ts[i:i+10]
                params = {
                    "keys": ",".join(batch),
                    "startTs": int(start90.timestamp() * 1000),
                    "endTs": int(now.timestamp() * 1000),
                    "limit": 5000,
                }
                try:
                    data = api_get(
                        f"/api/plugins/telemetry/DEVICE/{TARGET_DEVICE_ID}/values/timeseries",
                        headers_extra=auth_headers,
                        params=params,
                    )
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
                                print(f"  {k}: {n} points | min={min(numeric_vals):.3f} | max={max(numeric_vals):.3f} | moy={sum(numeric_vals)/len(numeric_vals):.3f}")
                            else:
                                print(f"  {k}: {n} points (non numériques)")
                        else:
                            print(f"  {k}: 0 point")
                except Exception as e:
                    print(f"  [ERREUR batch]: {type(e).__name__}: {e}")
        except Exception as e:
            print(f"[ERREUR] Device MVP: {type(e).__name__}: {e}")
    else:
        print("Pas de TB_DEVICE_ID valide.")
    print()

    # 4. Vérifier si batterylevel est sur un autre device
    print("=== Recherche de la clé 'batterylevel' ou similaire sur TOUS les devices ===")
    devices = []
    page = 0
    while True:
        data = api_get("/api/tenant/devices", headers_extra=auth_headers,
                       params={"pageSize": 100, "page": page})
        devices.extend(data.get("data", []))
        if len(data.get("data", [])) < 100:
            break
        page += 1

    for d in devices:
        did = d["id"]["id"]
        dname = d.get("name", "?")
        try:
            keys = api_get(f"/api/plugins/telemetry/DEVICE/{did}/keys/timeseries",
                           headers_extra=auth_headers)
            interesting = [k for k in keys if "battery" in k.lower() or "soc" in k.lower() or "level" in k.lower()]
            if interesting:
                print(f"  * {dname} ({d['type']}): {interesting}")
        except Exception:
            pass
    print()

    return 0


if __name__ == "__main__":
    sys.exit(main())
