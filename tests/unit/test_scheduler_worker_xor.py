from __future__ import annotations

from unittest.mock import patch

# Mêmes effets de bord d'import que test_services_worker_scheduler.py :
# serveur HTTP de health + probe disques neutralisés, scheduler réel.
with patch("http.server.ThreadingHTTPServer"), patch("trendx.services.ingestion.probe_disk_mounts"):
    import trendx.services.worker as worker


def test_single_planner_valid_configs() -> None:
    # (worker_ingestion, scheduler_enabled) -> attendu. Boucle explicite
    # plutôt que parametrize typé bool (convention lint FBT du dépôt).
    cases: list[tuple[tuple[bool, bool], bool]] = [
        ((True, False), True),  # état historique : worker seul planifie
        ((False, True), True),  # état cible MR-5 : scheduler seul planifie
        ((False, False), True),  # gap transitoire autorisé (recouvrement)
    ]
    for (worker_ingestion, scheduler_enabled), expected in cases:
        assert (
            worker.is_single_planner_config(
                worker_ingestion_enabled=worker_ingestion,
                scheduler_enabled=scheduler_enabled,
            )
            is expected
        )


def test_single_planner_forbids_dual_scheduling() -> None:
    # Contrat XOR MR-5 : worker + scheduler simultanés = configuration
    # interdite. Ce test DOIT échouer (ici : retourner False) si la double
    # planification ingestion redevient possible.
    assert (
        worker.is_single_planner_config(
            worker_ingestion_enabled=True,
            scheduler_enabled=True,
        )
        is False
    )


def test_effective_process_config_is_valid() -> None:
    # La configuration effective du processus (settings réels) ne doit jamais
    # activer les deux planificateurs à la fois. En CI : worker=true (défaut)
    # + scheduler=false (défaut) → valide.
    assert (
        worker.is_single_planner_config(
            worker_ingestion_enabled=worker.settings.trendx_worker_ingestion_enabled,
            scheduler_enabled=worker.settings.trendx_scheduler_enabled,
        )
        is True
    )


def test_flag_default_preserves_historical_behavior() -> None:
    # Garde-fou : le défaut du flag est true (comportement historique).
    # Toute inversion silencieuse du défaut casserait la production qui ne
    # définit pas encore la variable.
    assert worker.settings.model_fields["trendx_worker_ingestion_enabled"].default is True
