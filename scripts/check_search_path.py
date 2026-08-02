#!/usr/bin/env python3
"""Vérifie le search_path effectif de chaque moteur Trendx.

Règles :
- le moteur en lecture seule vers ThingsBoard (tb_readonly) ne porte AUCUN schéma
  Trendx dans son search_path ;
- aucun moteur ne porte simultanément trendx_catalog et trendx_analytics.

Sortie : SHOW search_path exécuté sur chaque connexion. Code de sortie != 0 en cas
de violation. Ne journalise aucun secret.
"""
import sys

from loguru import logger

logger.remove()

from trendx.database.connection import manager

TRENDX_SCHEMAS = ("trendx_catalog", "trendx_analytics")


def main() -> int:
    paths = manager.engine_search_paths()
    failed = False
    for name in sorted(paths):
        sp = paths[name]
        print(f"{name}: SHOW search_path = '{sp}'")
        if sp.startswith("ERROR"):
            failed = True
            continue
        tokens = {t.strip() for t in sp.split(",")}
        has_catalog = "trendx_catalog" in tokens
        has_analytics = "trendx_analytics" in tokens
        if has_catalog and has_analytics:
            print(f"FAIL: moteur '{name}' porte trendx_catalog ET trendx_analytics")
            failed = True
        if name == "tb_readonly":
            trendx_schemas = [t for t in TRENDX_SCHEMAS if t in tokens]
            if trendx_schemas:
                print(f"FAIL: moteur tb_readonly porte un schéma Trendx: {trendx_schemas}")
                failed = True
    print("RÈGLES: aucun moteur à double schéma Trendx ; tb_readonly sans schéma Trendx")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
