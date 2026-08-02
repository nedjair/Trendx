#!/usr/bin/env python3
"""Vérifie l'authentification ThingsBoard (compte service) SANS journaliser le mot de passe.

Lit TB_BASE_URL / TB_USERNAME / TB_PASSWORD dans .env. Sort :
- SKIP  si les identifiants sont encore des placeholders (code 0) ;
- OK    si /api/auth/login répond 200 (code 0) ;
- FAIL  sinon (code 1).

Ne jamais afficher ni le mot de passe ni le jeton JWT.
"""
import json
import ssl
import sys
import urllib.request
from urllib.error import HTTPError, URLError


def get_env_var(key, filepath=".env"):
    try:
        with open(filepath, "r") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    if k == key:
                        return v.strip()
    except OSError:
        pass
    return None


def main() -> int:
    base_url = (get_env_var("TB_BASE_URL") or "https://10.0.0.1:8081").rstrip("/")
    username = get_env_var("TB_USERNAME") or ""
    password = get_env_var("TB_PASSWORD") or ""

    is_placeholder = (
        not username
        or not password
        or len(password) < 8
        or password in ("CHANGE_ME",)
    )
    if is_placeholder:
        # Fallback sur le coffre (TB_SERVICE_USER_EMAIL / TB_SERVICE_USER_PASSWORD)
        username = get_env_var("TB_SERVICE_USER_EMAIL", ".secrets/service-accounts.env") or username
        password = get_env_var("TB_SERVICE_USER_PASSWORD", ".secrets/service-accounts.env") or password
        is_placeholder = (
            not username
            or not password
            or len(password) < 8
            or password in ("CHANGE_ME",)
        )
    if is_placeholder:
        print("SKIP — identifiants ThingsBoard non fournis (TB_PASSWORD / TB_SERVICE_USER_PASSWORD placeholders)")
        return 0

    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE

    body = json.dumps({"username": username, "password": password}).encode("utf-8")
    req = urllib.request.Request(
        f"{base_url}/api/auth/login",
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, context=ctx, timeout=20) as resp:
            if resp.status == 200:
                print("OK — authentification ThingsBoard réussie (compte service)")
                return 0
            print(f"FAIL — statut HTTP inattendu ({resp.status})")
            return 1
    except HTTPError as exc:
        print(f"FAIL — authentification refusée par ThingsBoard (HTTP {exc.code})")
        return 1
    except URLError as exc:
        print(f"FAIL — ThingsBoard injoignable ({exc.reason})")
        return 1


if __name__ == "__main__":
    sys.exit(main())
