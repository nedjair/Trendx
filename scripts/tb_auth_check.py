#!/usr/bin/env python3
"""Vérifie l'authentification ThingsBoard (compte service) SANS journaliser le mot de passe.

Règle (post-signalement sécurité) :
- TB_AUTH_CONFIGURED=true  => authentification OBLIGATOIREment réussie (code 0 = OK, code 1 = FAIL).
- TB_AUTH_CONFIGURED absent/false/vide => SKIP (code 0) ; l'opérateur n'a pas activé l'auth.

Ne jamais afficher ni le mot de passe ni le jeton JWT.
"""

import json
import os
import ssl
import sys
import urllib.request
from urllib.error import HTTPError, URLError


def get_env_var(key, filepath=".env"):
    try:
        with open(filepath) as f:
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

    # Fallback coffre si .env incomplet
    if not username or not password:
        username = get_env_var("TB_SERVICE_USER_EMAIL", ".secrets/service-accounts.env") or username
        password = (
            get_env_var("TB_SERVICE_USER_PASSWORD", ".secrets/service-accounts.env") or password
        )

    auth_configured = (
        get_env_var("TB_AUTH_CONFIGURED") or os.environ.get("TB_AUTH_CONFIGURED", "") or ""
    ).lower() in ("true", "1", "yes")

    if not auth_configured:
        print("SKIP — TB_AUTH_CONFIGURED non activé ; authentification ThingsBoard non vérifiée")
        return 0

    if not username or not password:
        print("FAIL — TB_AUTH_CONFIGURED=true mais TB_USERNAME/TB_PASSWORD manquants")
        return 1

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
