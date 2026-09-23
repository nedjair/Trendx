# Isolement Train des batches synthétiques (septembre 2026)

**Statut :** solution temporaire explicite, en attente d’une provenance native (`synthetic`/`provenance`) si introduite ultérieurement.
**Référence gate :** `TRENDX-TSKV-SYNTHETIC-PROVENANCE-DECISION-GATE-V1` → `PASS_PROVENANCE_CONFIRMED_SYNTHETIC`.

## Pourquoi cette exclusion existe

`trendx_analytics.ts_kv` contient 20 lignes résiduelles de fenêtres de test ThingsBoard (rampe `21.0→21.9` dupliquée, `2026-09-15T22:00Z→2026-09-16T21:00Z`, `entity 7c5d0442-6d93-5f05-be3e-2fc803d4c1b0 / temperature`), collectées via l’API TB les 16/09/2026 puis orphelines après nettoyage TB (`temperature` absente des 38 clés actuelles). Sans filtre, `TrainingService._fetch_training_data()` les lirait (`entity_id + metric_key + fenêtre 90j + dbl_v NOT NULL`) et les utiliserait pour Train/compétition. Elles sont qualifiées `CONFIRMED_SYNTHETIC_TEST_DATA` et doivent être exclues.

## Données protégées

Les 11 `ingestion_id` (format `uuid4()[:12]`, un batch initial de 10 + 10 batches horaires) :

```text
1c74878e-2c6
20cb60c9-13d
35b45778-125
5b55fd02-043
6123a926-d02
8e88d1ba-0e0
95b8debc-b40
bd0c4482-dad
cae64d2c-18c
e6c0b750-6ba
f90f3866-116
```

Périmètre : `entity_id=7c5d0442-6d93-5f05-be3e-2fc803d4c1b0`, `metric_key=temperature`, 20 lignes ci-dessus. Ne jamais présenter ces lignes comme opérationnelles.

## Configuration

Variable unique (config `trendx/config.py`, exemple `.env.example`) :

```text
TRAINING_EXCLUDED_INGESTION_IDS=<csv>
```

- Vide (`""`/absente) → **aucune exclusion** (comportement normal historique).
- Liste explicite `a,b,c` → exclusion uniquement des IDs indiqués via `AND (ingestion_id IS NULL OR ingestion_id NOT IN :excluded)` (bindparam expanding, paramétré, sans concaténation).
- Parsing : séparation virgule, trim, suppression vides, déduplication (premier vu), déterministe (`parse_excluded_ingestion_ids`).
- `NULL` conservés (anciennes lignes sans batch jamais exclues).

Exemple (ne pas activer par défaut) :

```text
TRAINING_EXCLUDED_INGESTION_IDS=1c74878e-2c6,20cb60c9-13d,35b45778-125,5b55fd02-043,6123a926-d02,8e88d1ba-0e0,95b8debc-b40,bd0c4482-dad,cae64d2c-18c,e6c0b750-6ba,f90f3866-116
```

## Ajouter/remplacer une liste

1. Relever les `ingestion_id` en SELECT seul (`SELECT DISTINCT ingestion_id FROM trendx_analytics.ts_kv ...`).
2. Poser la CSV dans l’environnement du scheduler/worker (jamais en dur sans documentation).
3. Redémarrer uniquement via gate autorisée (cette gate ne redémarre rien).
4. Vérifier par test `test_training_synthetic_exclusion.py` (20→0, témoin conservé).

## Pourquoi `source=thingsboard` et la fenêtre temporelle sont interdits

- `source` homogène (`thingsboard` 20/20, identique pour tout futur valide, jamais lu par Train) : l’utiliser exclurait tout ou rien.
- Fenêtre temporelle seule fragile (chevauchement futur, maintenance manuelle) : refusée comme critère unique par la gate.

## Temporaire

Si une colonne native `provenance/synthetic/validity` est introduite (migration dédiée), migrer le filtre vers ce marqueur et décommissionner cette denylist. En attendant, ne pas étendre la liste sans gate de provenance.
