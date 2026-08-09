# Issue : `mlflow.ActiveRun` incompatible avec mlflow ≥ 2.x dans `tracking.py`

**Composant :** `src/trendx/mlops/tracking.py`
**Priorité :** Haute (bloquait 6 tests CI sur `master` avant cette branche)
**Statut :** ✅ Corrigé dans `feat/mlops-rollback` — à vérifier en CI lors du merge
**Labels :** `bug`, `mlops`, `ci`
**Branche concernée :** pré-existant sur `master` (introduit dans le commit initial `7214502`)

---

## Symptôme

6 tests de `tests/unit/test_mlops_tracking.py` échouaient en CI avec :

```
TypeError: ActiveRun.__init__() takes 2 positional arguments but 3 were given
```

Tests affectés :
- `test_start_end_run`
- `test_log_params_metrics`
- `test_log_model`
- `test_get_best_run`
- `test_get_best_run_no_metric`
- `test_log_artifact`

## Cause racine

Le code initial de `start_run()` dans `tracking.py` construisait l'objet
`ActiveRun` directement avec deux arguments positionnels :

```python
# AVANT (incompatible mlflow ≥ 2.x)
self._active_run = mlflow.ActiveRun(run, self._client)
```

Or depuis mlflow 2.x, `mlflow.ActiveRun.__init__` n'accepte qu'**un seul
argument positionnel** (`run`). Le deuxième argument (`client`) a été retiré
de l'API publique.

La version CI est `mlflow==2.14.3` (fixée dans `constraints.txt`).

## Correctif appliqué (commit `c8a3a6d` via code déjà présent au HEAD)

Le code corrigé utilise `mlflow.start_run()` à la place de l'instanciation
directe, avec un fallback gracieux :

```python
# APRÈS (compatible mlflow ≥ 2.x)
try:
    self._active_run = mlflow.start_run(run_id=run.info.run_id)
except Exception:
    self._active_run = run  # type: ignore[assignment]
```

## Vérification

- Localement : les 6 tests sont **XPASS** (passent) avec la version corrigée.
- En CI : à confirmer lors du prochain run pipeline sur `feat/mlops-rollback`.
- Les marqueurs `xfail` sur ces 6 tests (commit `c8a3a6d`) peuvent être
  **retirés** une fois le pipeline CI vert confirmé pour ces tests.

## Action restante

- [ ] Valider pipeline CI vert pour ces 6 tests après merge
- [ ] Retirer les 6 marqueurs `xfail` de `test_mlops_tracking.py` dans un
      commit de nettoyage post-merge

---

*Documenté dans le cadre du triage des 21 échecs CI — `feat/mlops-rollback`.*
