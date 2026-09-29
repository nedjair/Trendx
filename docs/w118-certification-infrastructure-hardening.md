# W118 — Durcissement de l'infrastructure de certification

**Phase** : W118 — *certification-infrastructure hardening*
**Périmètre** : clôture des constats **I-04** (maîtrise du contrôle
orthographique de la documentation) et **I-05** (versionner la Global Closure
Gate), en écriture strictement contrôlée et en **un seul commit**.
**Résultat** : `PASS-W118-CERTIFICATION-INFRASTRUCTURE-HARDENING`

---

## 1. Périmètre

### 1.1 Dans le périmètre

| Constat | Objets modifiés |
|---|---|
| I-04 — contrôle orthographique | `typos.toml`, `pyproject.toml`, `.pre-commit-config.yaml`, `.gitea/workflows/ci.yaml` |
| I-05 — gate de clôture versionnée | `tests/integration/test_w118_global_closure_gate.py` (nouveau) |
| Documentation de phase | `docs/w118-certification-infrastructure-hardening.md` (nouveau) |

**Justification des deux fichiers de configuration qualité** (hors liste stricte
`pyproject`/`tests`/`docs`) : la clôture d'I-04 consiste précisément à replier
deux sources de configuration typos en une seule. Sans basculer les deux
pointeurs d'exécution (`args` du hook, `run` de la CI), le glossaire resterait
dans `typos.toml` pendant que les exécutions continueraient de viser
`pyproject.toml` : la correction serait cosmétique et non fonctionnelle.

### 1.2 Hors périmètre (non touchés)

`src/trendx/**`, `migrations/**`, `.env` et `.env.example`, drapeaux
d'exécution, MCP, Docker, OpenCode, MLflow, ThingsBoard, writeback, alarmes,
détection d'anomalies, API et OpenAPI **de fonctionnalité**, migrations de base
et activation en production.

### 1.3 Constat I-06 — conservé tel quel

Les deux échecs historiques suivants sont **HISTORICAL / OUT_OF_SCOPE_W118 /
RESERVED_FOR_W119**. Ils n'ont été ni corrigés, ni marqués, ni exclus :

- `tests/integration/test_migration_002_analytics_schema.py::test_002_qualifies_analytics_objects_statically`
- `tests/test_service_account_artifacts.py::test_sql_contains_default_revocations_and_read_only_grafana`

---

## 2. I-04 — une seule source pour le contrôle orthographique

### 2.1 Constat chiffré (avant mutation)

| Invocation | Signalements sur les fichiers suivis |
|---|---|
| `typos` (découverte automatique de `typos.toml`) | **1605** (dont 900 sur `docs/`) |
| `typos --config pyproject.toml` (commande du hook et de la CI) | **0** |

Le glossaire français (124 entrées) vivait dans `pyproject.toml`, alors que le
fichier découvert automatiquement par l'outil — `typos.toml` — ne contenait
qu'une unique ignorance. Deux conséquences :

1. toute invocation qui n'explicite pas `--config pyproject.toml` ignore les
   124 entrées et produit plus de 1600 faux positifs sur la documentation
   française ;
2. `typos.toml` se décrivait comme « détecté automatiquement par le hook »,
   ce qui était faux — le hook visait `pyproject.toml`.

### 2.2 Correctif

| Fichier | Modification |
|---|---|
| `pyproject.toml` | −149 lignes : suppression de `[tool.typos.default.extend-words]` et `[tool.typos.default]`. `lignes 1-117` (dont `[tool.bandit]`) et l'override mypy `trendx.main` sont conservés octet pour octet. |
| `typos.toml` | +166 lignes : `[default]` (`extend-ignore-identifiers-re` ancrés + `extend-ignore-words-re` « dahsboard ») puis `[default.extend-words]` (124 entrées, commentaires d'origine conservés : 15/15). En-tête expliquant le rôle de fichier source unique. |
| `.pre-commit-config.yaml` | `args: ["--format", "brief", "--config", "pyproject.toml"]` → `... "typos.toml"` |
| `.gitea/workflows/ci.yaml` | `run: typos --config pyproject.toml` → `run: typos --config typos.toml` |

### 2.3 Preuves TEST-TYPO-01 → 08

Exécution : `bash /tmp/opencode/w118/run_typos_tests.sh` — binaire `typos-cli
1.21.0`, mêmes args que le hook et la CI.

| ID | Objet mesuré | Résultat |
|---|---|---|
| TEST-TYPO-01 | source unique : 0 section `[tool.typos]` suivie, 1 fichier `typos.toml` à la racine, hook + CI pointent ce fichier | **PASS** |
| TEST-TYPO-02 | invocation nue `typos` sur les 515 fichiers suivis | **PASS** — 0 signalement (avant : 1605) |
| TEST-TYPO-03 | commande exacte du hook et de la CI (`--config typos.toml`) | **PASS** — 0 signalement |
| TEST-TYPO-04a | parité dépôt : nu == `--config typos.toml` | **PASS** — rc=0 / 0 ligne des deux côtés |
| TEST-TYPO-04b | parité corpus (fichiers + `typos.toml` dans le même dossier) | **PASS** — sorties strictement identiques |
| TEST-TYPO-04c | `--dump-config -` : glossaire actif par défaut | **PASS** — 124 entrées présentes après, 0 avant |
| TEST-TYPO-05 | environnement propre (dossier vide + `typos.toml` copié) == dépôt | **PASS** — sorties identiques |
| TEST-TYPO-06 | fautes françaises détectées avec la config, 0 règle `exclude`/`check-file` | **PASS** — les 5 fautes de frappe de référence (sur « certification », « documentation », « configuration », « performance », « security ») sont toutes signalées |
| TEST-TYPO-07 | anglais réel toujours détecté avec la config | **PASS** — les 4 fautes anglaises de référence sont toutes signalées (4/4) |
| TEST-TYPO-08 | pointeurs résiduels + glossaire présent dans exactement 1 fichier suivi | **PASS** — 0 invocation `--config pyproject.toml`, glossaire dans `typos.toml` seul, 1605 → 0 |

Portée de ces chiffres : les fichiers **suivis**, qui sont ceux que le hook,
la CI (exécution sur checkout) et la porte de qualité finale examinent. Un
balayage de l'arbre de travail au complet revisite aussi 3 fichiers de travail
non suivis préexistants (`docs/audit-phase2-agent-infrastructure.md`,
`docs/scheduler-b1-production-readiness-design.md`,
`remediation-report.md`) : leur contenu, déjà présent au relevé de départ
(`baseline/git-status.txt`), porte des signalements antérieurs à W118. Ces
fichiers ne font pas partie du périmètre autorisé de la phase et restent
inchangés ; ils ne sont donc ni corrigés ni comptés dans les résultats ci-dessus.

### 2.4 Limites assumées de l'outil (non masquées)

`typos-cli 1.21.0` **détecte** les fautes de frappe classiques sur
« certification », « documentation », « configuration », « performance » et
« security » (la liste exacte des formes reconnues est dans
`/tmp/opencode/w118/evidence/typos-tests.txt`), mais **ne détecte pas**
`securite`, `reproductiblite`, `authentfication` : le dictionnaire de l'outil ne
les reconnaît pas comme fautes anglaises. Le contrôle négatif français
(TEST-TYPO-06, §4.C) s'appuie donc sur les formes réellement détectées, et
cette limite est déclarée ici plutôt que d'être présentée comme une couverture
complète. Aucune couverture n'est annoncée sans test (règle 10).

C'est aussi pour cela que ce document ne recopie pas les formes
détectées telles quelles : elles sont fausses volontairement, et le contrôle
orthographique les signalerait dans la documentation qu'elles décrivent. Les
échantillons vivent dans les fichiers de preuve, hors dépôt.

Comportement outil connu et sans incidence : avec `--config typos.toml`,
`--dump-config` double les tableaux du fichier déjà découvert. La parité des
sorties est vérifiée par TEST-TYPO-04a/04b.

---

## 3. I-05 — Global Closure Gate versionnée

### 3.1 Emplacement et version

- fichier : `tests/integration/test_w118_global_closure_gate.py` (2232 lignes)
- `GLOBAL_CLOSURE_GATE_VERSION = "1"`
- `GATE_BASELINE = b614d4434cd9c37d5b161583b24d17c3b539d0a3` (commit de début de phase)
- exécution : `pytest -q tests/integration/test_w118_global_closure_gate.py` → **27 tests**
  (26 contrôles `TEST-CLOSURE-01…26` + le test d'intégrité du rapport)

La gate remplace le harness jetable exécuté depuis `/tmp/opencode/w117`
(I-05) : la certification vit désormais dans le dépôt, versionnée avec le code
qu'elle certifie. Le harness W117 reste exécuté en preuve de non-régression
(46 tests, §5).

### 3.2 Mécanisme

| Élément | Rôle |
|---|---|
| `GateFailureError` | échec explicite : `phase`, `invariant`, `attendu`, `observé` — jamais un échec silencieux |
| `@gate(check_id, section)` | registre des 26 contrôles ; toute section inconnue lève `ValueError` au chargement |
| `evaluate_phase()` | statut `PASS`/`FAIL`/`N/A` d'une phase : `git cat-file`, `git log -1 --format=%s`, `git merge-base --is-ancestor`, `git show --name-only` — **lecture seule** |
| `build_report()` | rapport structuré : `W102` est toujours `N/A` ; `RESULT` ne vaut `PASS` que si **toutes** les phases et les 13 sections valent `PASS` ou `N/A` |
| `run_gate(skip=…)` | exécute chaque contrôle ; les contrôles déclarant `tmp_path` tournent dans un répertoire `tempfile` jetable ; une exception inattendue est comptée `GATE ERROR` et force `FAIL` |
| `business_tree_digest()` | SHA-256 de `src/` + `migrations/` : la gate observe, elle n'écrit jamais dans les artefacts métier |

**Rapport structuré (36 clés)** : `GATE`, `VERSION`, `BASELINE`, les 19 phases
`W98…W116`, les 13 sections, `RESULT`.

### 3.3 Sections et contrôles

13 sections : `contract`, `api`, `security`, `tenant`, `durability`,
`lifecycle`, `recovery`, `integrity`, `analytics`, `reporting`, `health`,
`documentation`, `production-safety`.

| ID | Section | Contrôle principal |
|---|---|---|
| TEST-CLOSURE-01 | contract | gate versionnée, schéma de rapport stable, chaque section au moins un contrôle |
| TEST-CLOSURE-02 | contract | `W102` reste `N/A` (phase sans implémentation) |
| TEST-CLOSURE-03 | contract | 18 phases vérifiables présentes |
| TEST-CLOSURE-04 | contract | aucun commit attendu ne disparaît |
| TEST-CLOSURE-05 | api | 21 routes de la chaîne, `operationId` et chemins figés |
| TEST-CLOSURE-06 | security | chaque route de la chaîne est sécurisée |
| TEST-CLOSURE-07 | security | aucune route de la chaîne en `security: []` OpenAPI |
| TEST-CLOSURE-08 | contract | versions des 11 artefacts certifiés = `"1"` |
| TEST-CLOSURE-09 | tenant | isolation tenant (requête, projections disjointes, export, restauration interdite) |
| TEST-CLOSURE-10 | lifecycle | W107 et W112 sur des contrats séparés |
| TEST-CLOSURE-11 | durability | archive, restauration et intégrité cohérentes |
| TEST-CLOSURE-12 | documentation | documents de phase présents et référencés, liens résolus |
| TEST-CLOSURE-13 | documentation | aucune route fictive ni promesse de latence garantie |
| TEST-CLOSURE-14 | production-safety | 6 drapeaux de production à `false` dans le contexte de la gate |
| TEST-CLOSURE-15 | production-safety | la gate ne modifie aucun artefact métier |
| TEST-CLOSURE-16 | recovery | invariants de récupération |
| TEST-CLOSURE-17 | integrity | intégrité et contrat d'événement d'audit |
| TEST-CLOSURE-18 | analytics | cohérence du contrat analytique |
| TEST-CLOSURE-19 | reporting | contrat de reporting |
| TEST-CLOSURE-20 | health | health / capacity / readiness |
| TEST-CLOSURE-21 | durability | opérations de lecture réellement en lecture seule |
| TEST-CLOSURE-22 | contract | le contrat global n'a pas divergé (enums, codes d'erreur, dimensions) |
| TEST-CLOSURE-23 | api | la forme documentée de l'API correspond à l'OpenAPI vivant |
| TEST-CLOSURE-24 | lifecycle | purge en échec fermé (`fail-closed`) |
| TEST-CLOSURE-25 | production-safety | aucun déclencheur automatique dans le cycle de vie d'audit |
| TEST-CLOSURE-26 | contract | un résultat global ne masque jamais une section en échec |
| rapport | — | `test_global_closure_gate_report_is_pass` : 36 clés, `RESULT = PASS` |

### 3.4 Rapport produit

```text
GATE = global-closure
VERSION = 1
BASELINE = b614d4434cd9c37d5b161583b24d17c3b539d0a3
W98 = PASS
W99 = PASS
W100 = PASS
W101 = PASS
W102 = N/A
W103 = PASS
W104 = PASS
W105 = PASS
W106 = PASS
W107 = PASS
W108 = PASS
W109 = PASS
W110 = PASS
W111 = PASS
W112 = PASS
W113 = PASS
W114 = PASS
W115 = PASS
W116 = PASS
CONTRACT = PASS
API_ROUTES = PASS
OPENAPI_SECURITY = PASS
TENANT_ISOLATION = PASS
DURABILITY = PASS
LIFECYCLE = PASS
RECOVERY = PASS
INTEGRITY = PASS
ANALYTICS = PASS
REPORTING = PASS
HEALTH = PASS
DOCUMENTATION = PASS
PRODUCTION_SAFETY = PASS
RESULT = PASS
```

### 3.5 Volontairement non figé

timestamps, `statvfs`, tailles de filesystem, mémoire/CPU, latences, PID,
ports, liaisons réseau, chemins temporaires, environnement de l'opérateur. Seuls
des invariants de structure et de contrat sont gelés : c'est ce qui permet à la
gate de rester déterministe d'un clone à l'autre.

---

## 4. Contrôle négatif (clone hors dépôt)

Le contrôle est exécuté dans **un clone de `/root/Trendx`**
(`/tmp/opencode/w118/negative/repo`), jamais dans le dépôt de travail : script
`/tmp/opencode/w118/negative_control.sh`, journaux dans
`/tmp/opencode/w118/negative/`.

| Cas | Alteration du clone | Résultat observé |
|---|---|---|
| A — route fictive | `GET /api/v1/forecast/executions/quantum-entanglement` ajouté à `docs/execution-audit-chain.md` | `3 failed` — `invariant=no_fictional_route_in_documentation` (TEST-CLOSURE-13) + rapports en cascade |
| B — document de phase retiré | `w113-execution-audit-operational-health.md` déplacé hors du clone | `4 failed` — `invariant=phase_document_exists` (W113), `invariant=verified_phases_count` (TEST-CLOSURE-03), rapports en cascade |
| C — glossaire amputé + faute | entrée `branche` retirée de `typos.toml`, une faute de frappe sur « certification » injectée | `exit=2` — message `… should be …` de `typos` sur la faute injectée : elle reste détectée même sans l'entrée du glossaire |
| D — restauration | altérations annulées | `27 passed`, `exit=0` |
| E — dépôt réel | — | `HEAD` inchangé (`b614d44`), 26 entrées de statut avant **et** après, `git diff --stat` inchangé |

La gate échoue donc avec un diagnostic précis (phase + invariant + attendu +
observé) et repasse une fois l'altération corrigée, sans jamais toucher au dépôt
réel.

---

## 5. Baselines avant / après

Invocation commune : `.venv/bin/pytest -q --no-cov` (même méthodologie que
W117).

| Lot | Périmètre | Avant | Après |
|---|---|---|---|
| Suite complète | tout le dépôt | `2 failed, 1437 passed, 266 skipped, 7 xfailed` (1099 s) | `2 failed, **1464 passed**, 266 skipped, 7 xfailed` (1090 s) |
| W98–W113 | 23 fichiers | `500 passed` | `500 passed` |
| W114 | 2 fichiers | `39 passed` | `39 passed` |
| Harness W117 | `/tmp/opencode/w117` | `46 passed` | `46 passed` |
| Contrôle orthographique (fichiers suivis) | `typos` nu | 1605 signalements | 0 |
| Échecs | identiques | les 2 échecs historiques I-06 | les **mêmes** 2 échecs, aucun nouveau |

`1464 = 1437 + 27` : aucun test existant n'a disparu ni n'a régressé, la seule
variation est l'ajout des 27 tests de la gate.

---

## 6. Portes de qualité

| Porte | Commande | Résultat |
|---|---|---|
| ruff | `ruff check .` | **OK** — 0 diagnostic |
| format | `ruff format --check .` | **OK** — 242 fichiers déjà formatés |
| typage | `mypy src` | **OK** — 0 problème sur 85 fichiers |
| compilation | `python -m compileall -q src tests` | **OK** |
| SAST | `bandit -r src -c pyproject.toml -f txt` | **OK** — 0 issue (low/medium/high) |
| secrets | `gitleaks detect --source . --no-banner --redact --exit-code 1` | **OK** — 269 commits scannés, 0 leak |
| orthographe | `typos` (nu et `--config typos.toml`, fichiers suivis) | **OK** — 0 signalement, 10/10 preuves |
| tests | 4 lots de non-régression (§5) | **OK** — 500 / 39 / 46 / 1464 |

---

## 7. Sécurité de production

Aucune écriture vers ThingsBoard, aucun drapeau d'exécution activé. Le contrôle
`TEST-CLOSURE-14` relance un sous-processus avec, pour chacun des six
drapeaux, la valeur explicite `false` et vérifie que les attributs de
configuration résolus sont tous `False` :

`TRENDX_SCHEDULER_FORECAST_ENABLED`, `TRENDX_INGEST_ENABLED`,
`TRENDX_WORKER_INGESTION_ENABLED`, `TB_WRITEBACK_ENABLED`,
`TB_ALARMS_ENABLED`, `ANOMALY_DETECTION_ENABLED`.

`TEST-CLOSURE-25` vérifie en outre qu'aucun service de cycle de vie d'audit ne
contient de déclencheur automatique (`APScheduler`, `asyncio`, `cron`,
`while True`, `sleep(`) et que la politique de rétention reste en `dry_run`.
`TEST-CLOSURE-14` conclut par le relevé des six attributs à `False`.

---

## 8. Preuves

| Artefact | Contenu |
|---|---|
| `/tmp/opencode/w118/baseline/` | manifeste git (HEAD, statut, diff, log), 4 baselines, diagnostic typos avant |
| `/tmp/opencode/w118/evidence/typos-tests.txt` | TEST-TYPO-01…08 (10 lignes PASS/FAIL) |
| `/tmp/opencode/w118/evidence/gate-run-2.txt` | `27 passed` de la gate |
| `/tmp/opencode/w118/evidence/gate-report.txt` | rapport structuré §3.4 |
| `/tmp/opencode/w118/evidence/q-*.txt` | ruff, ruff format, mypy, compileall, gitleaks, bandit |
| `/tmp/opencode/w118/post/` | 4 baselines après mutation |
| `/tmp/opencode/w118/negative/` | contrôle négatif A/B/C/D/E |
| `/tmp/opencode/w118/lot_a_migrate.py` | migration de la configuration typos, 16 contrôles d'intégrité |

---

## 9. Méthodologie et limites

- arête de départ vérifiée et figée : `b614d4434cd9c37d5b161583b24d17c3b539d0a3`,
  diff vide, 21 fichiers de travail non suivis intacts avant et après ;
- chaque mutation a été précédée de sa baseline et suivie de sa
  non-régression ; toute divergence aurait arrêté la phase (`FAIL`) ;
- la gate est **déterministe** : aucun serveur HTTP, aucun port, aucune donnée
  hors `tmp_path`, aucune connexion à ThingsBoard ou à PostgreSQL ;
- limites : la couverture orthographique est celle de `typos-cli 1.21.0`
  (§2.4) et la gate ne certifie que ce qu'elle déclare — les contrôles non
  listés au §3.3 restent hors de sa garantie.

---

## 10. Verdict

**`PASS-W118-CERTIFICATION-INFRASTRUCTURE-HARDENING`**

- I-04 soldé : source unique `typos.toml`, hook et CI alignés, 1605 → 0
  signalements, 10/10 preuves ;
- I-05 soldé : Global Closure Gate versionnée (`1`) et ancrée sur `b614d44`,
  27 tests verts, rapport structuré `RESULT = PASS`, contrôle négatif concluant ;
- I-06 conservé : `HISTORICAL` / `OUT_OF_SCOPE_W118` / `RESERVED_FOR_W119` ;
- 0 régression : 500 / 39 / 46 inchangés, `1437 + 27 = 1464` tests verts,
  mêmes 2 échecs historiques ;
- 6 drapeaux de production à `false` pendant toute la phase.
