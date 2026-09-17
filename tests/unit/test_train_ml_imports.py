"""Smoke test des dépendances ML du chemin trendx_train.

 verrouille la remédiation du ModuleNotFoundError: No module named 'sklearn'
constaté sur les images b1-b6addc8 (training.py -> Normalizer importe
sklearn/scipy au niveau module ; prophet/statsmodels/pmdarima/mlflow sont
requis en lazy ou via MLflowTracker sur le chemin d'entraînement).
"""

from __future__ import annotations

import importlib


def test_training_import_chain_loads() -> None:
    import trendx.services.training  # noqa: F401
    from trendx.preprocessing.normalizer import Normalizer  # noqa: F401


def test_ml_runtime_dependencies_importable() -> None:
    for module in (
        "sklearn",
        "scipy",
        "statsmodels",
        "prophet",
        "mlflow",
        "pmdarima",
    ):
        assert importlib.import_module(module) is not None
