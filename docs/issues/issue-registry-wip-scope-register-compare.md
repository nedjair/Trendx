# Issue : Tests `register_model` / `compare_models` dépendent du WIP non stagé

**Composant :** `src/trendx/mlops/registry.py` + `tests/unit/test_mlops_registry.py`
**Priorité :** Moyenne (bloquait 2 tests CI ; contenus dans le scope WIP déclaré)
**Statut :** 🟡 XFAIL documenté — attend le commit `feat/detector-scoring-and-forecasting-fixes`
**Labels :** `test`, `mlops`, `wip-scope`
**Branche concernée :** `feat/mlops-rollback` (tests ajoutés sur cette branche pour la future signature)

---

## Symptôme

2 tests de `tests/unit/test_mlops_registry.py` échouaient en CI :

```
TypeError: register() got an unexpected keyword argument 'entity_id'
KeyError: 'algorithm'
```

Tests affectés :
- `test_register_model`
- `test_compare_models`

## Cause racine

Ces deux tests ont été écrits pour la **nouvelle signature** de `register()` et
`compare_models()` (qui accepte `entity_id` et retourne un dict avec clé
`"algorithm"`), version qui **n'existe que dans le WIP non stagé** de
`registry.py` au moment du commit `c8a3a6d`.

Le code **commité** de `registry.py` (commit `7dcf0f1`, introduit par
`feat(mlops): add auditable model rollback`) ne contient pas encore ces
paramètres.

**Conclusion :** erreur de scope — les tests testaient du code WIP non inclus
dans la branche. Option retenue : `xfail` documenté (voir commit `c8a3a6d`)
plutôt que revert, pour conserver le contrat d'API prévu.

## Correctif appliqué (commit `c8a3a6d`)

```python
@pytest.mark.xfail(
    reason="teste la signature register()/compare_models() du WIP non stagé "
           "(entity_id, algorithm) — attend le future commit sur "
           "feat/detector-scoring-and-forecasting-fixes"
)
def test_register_model(registry): ...

@pytest.mark.xfail(
    reason="teste la signature register()/compare_models() du WIP non stagé "
           "(entity_id, algorithm) — attend le future commit sur "
           "feat/detector-scoring-and-forecasting-fixes"
)
def test_compare_models(registry): ...
```

## Contrat d'API documenté (pour le commit suivant)

La nouvelle signature attendue par les tests est :

```python
# register() doit accepter :
registry.register(
    model=...,
    entity_id="entity-abc",   # nouveau paramètre obligatoire
    metric_name="temperature",
    run_id="run-123",
    version="1.0",
)

# compare_models() doit retourner un dict incluant :
{
    "algorithm": "IsolationForest",
    ...
}
```

## Action restante

- [ ] Implémenter la nouvelle signature de `register()` dans `registry.py`
      dans le commit `feat/detector-scoring-and-forecasting-fixes`
- [ ] Implémenter le retour `"algorithm"` dans `compare_models()`
- [ ] Retirer les 2 marqueurs `xfail` de `test_mlops_registry.py` une fois
      les fonctions implémentées et les tests verts en CI

---

*Documenté dans le cadre du triage des 21 échecs CI — `feat/mlops-rollback`.*
