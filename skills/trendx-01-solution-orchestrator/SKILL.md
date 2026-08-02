---
name: "trendx-01-solution-orchestrator"
description: "Orchestre la mise en œuvre Trendx de bout en bout. Invoke en phase d'installation initiale, MVP, ajout de modèle, diagnostic ou reprise."
---

# trendx 01 solution orchestrator

## 1. Objectif

Orchestre la mise en œuvre Trendx de bout en bout. Invoke en phase d'installation initiale, MVP, ajout de modèle, diagnostic ou reprise.

## 2. Quand utiliser cette skill

- Phase d'installation initiale, MVP, ou ajout de modèle
- Avant toute modification d'infrastructure ou exécution de DAG
- Lorsque l'utilisateur demande explicitement la fonction associée

## 3. Quand ne pas l'utiliser

- Pour des tâches ne touchant pas au périmètre Trendx
- Sans avoir validé les prérequis au préalable (skill 03 / 05)

## 4. Préconditions

- Fichier .env renseigné et droits 600
- Accès réseau aux cibles (ThingsBoard, Docker, PostgreSQL)
- Variables d'environnement du bloc de la skill exportées ou lues

## 5. Entrées obligatoires

- TB_BASE_URL, TB_USERNAME, TB_PASSWORD, TB_DEVICE_ID, TB_METRIC_NAME
- ANALYTICS_DB_*, AIRFLOW_*, MLFLOW_*, GRAFANA_* selon le cas
- Horizons / fréquences depuis FORECAST_* / ANOMALY_*

## 6. Outils et dépendances

- Outils filesystem Trae (Read / Write / Glob / Grep / LS)
- RunCommand bash + docker compose + python3 + jq + sshpass
- MCPs recommandés : mcp_Docker, mcp_Postgrest, mcp_ssh, mcp_thingsboard
- Pre-commit workflow : On branch master
Changes not staged for commit:
  (use "git add/rm <file>..." to update what will be committed)
  (use "git restore <file>..." to discard changes in working directory)
	modified:   .gitignore
	modified:   MCPs et SKILLS.md
	deleted:    agent.md
	modified:   docs/mvp-scope.md

Untracked files:
  (use "git add <file>..." to include in what will be committed)
	.env.example
	.pre-commit-config.yaml
	AGENTS.md
	Makefile
	README.md
	constraints.txt
	dags/
	docker-compose.yml
	docker/
	grafana/
	migrations/
	pyproject.toml
	scripts/
	skills/
	src/
	tests/

no changes added to commit (use "git add" and/or "git commit -a") puis  avant commit

## 7. Procédure pas à pas

1. Diagnostiquer l'état actuel (prérequis / santé services)
2. Déterminer la sous-commande / phase à exécuter
3. Appliquer les modifications de manière idempotente
4. Inspecter logs / résultats
5. Mettre à jour watermark / métadonnées si besoin
6. Produire un rapport de succès ou d'erreur

## 8. Commandes ou scripts autorisés

- → Interpréteur Python : python3
→ Création de l'environnement virtuel...
The virtual environment was not created successfully because ensurepip is not
available.  On Debian/Ubuntu systems, you need to install the python3-venv
package using the following command.

    apt install python3.12-venv

You may need to use sudo with that command.  After installing the python3-venv
package, recreate your virtual environment.

Failing command: /home/mobilis/Trendx/.venv/bin/python3
- [1m=== Trendx — Validation stack Skills/MCPs[0m (projet=Trendx)

[1m1/7 Arborescence attendue[0m
  [1;32m✓[0m fichier présent : README.md
  [1;32m✓[0m fichier présent : AGENTS.md
  [1;32m✓[0m fichier présent : .gitignore
  [1;32m✓[0m fichier présent : .env.example
  [1;32m✓[0m fichier présent : pyproject.toml
  [1;32m✓[0m fichier présent : docker-compose.yml
  [1;32m✓[0m fichier présent : Makefile
  [1;32m✓[0m fichier présent : MCPs et SKILLS.md
  [1;32m✓[0m fichier présent : Procédure de mise en œuvre Trendx.md
  [1;32m✓[0m fichier présent : skills de mise en oeuvre.md
  [1;32m✓[0m dossier présent : docs/
  [1;32m✓[0m dossier présent : docker/
  [1;32m✓[0m dossier présent : migrations/
  [1;32m✓[0m dossier présent : dags/
  [1;32m✓[0m dossier présent : src/
  [1;32m✓[0m dossier présent : grafana/
  [1;32m✓[0m dossier présent : scripts/
  [1;32m✓[0m dossier présent : tests/
  [1;32m✓[0m dossier présent : config/

[1m2/7 Fichier .env et variables attendues[0m
  [1;32m✓[0m .env présent
  [1;32m✓[0m droits .env = 600
  [1;32m✓[0m TRENDX_ENV = development
  [1;32m✓[0m TRENDX_LOG_LEVEL = INFO
  [1;32m✓[0m TRENDX_TIMEZONE = UTC
  [1;32m✓[0m TB_BASE_URL = https://10.0.0.1:8081
  [1;32m✓[0m TB_USERNAME = tenant@mobilis.dz
  [1;32m✓[0m TB_PASSWORD = ***
  [1;32m✓[0m TB_DEVICE_ID = f3ce3c10-9b9f-11f0-8762-cdf08b46b3be
  [1;32m✓[0m TB_METRIC_NAME = batterylevel
  [1;32m✓[0m ANALYTICS_DB_HOST = timescaledb
  [1;32m✓[0m ANALYTICS_DB_PORT = 5432
  [1;32m✓[0m ANALYTICS_DB_NAME = trendx_analytics
  [1;32m✓[0m ANALYTICS_DB_USER = trendx_app
  [1;32m✓[0m ANALYTICS_DB_PASSWORD = ***
  [1;32m✓[0m AIRFLOW_DB_NAME = trendx_airflow
  [1;32m✓[0m AIRFLOW_DB_USER = trendx_airflow
  [1;32m✓[0m AIRFLOW_DB_PASSWORD = ***
  [1;32m✓[0m AIRFLOW_HOST_PORT = 8082
  [1;32m✓[0m MLFLOW_TRACKING_URI = http://mlflow:5000
  [1;32m✓[0m MLFLOW_HOST_PORT = 5000
  [1;32m✓[0m GRAFANA_HOST_PORT = 3000
  [1;32m✓[0m GRAFANA_ADMIN_USER = admin
  [1;32m✓[0m GRAFANA_ADMIN_PASSWORD = ***
  [1;32m✓[0m FORECAST_FREQUENCY = 1h
  [1;32m✓[0m FORECAST_HORIZON = 24
  [1;32m✓[0m TRAINING_LOOKBACK_DAYS = 90
  [1;32m✓[0m ANOMALY_DETECTION_ENABLED = false
  [1;32m✓[0m ANOMALY_CONTAMINATION = 0.01
  [1;32m✓[0m ANOMALY_WINDOW_SIZE = 24
  [1;32m✓[0m FORECAST_MIN_VALUE = 0
  [1;32m✓[0m FORECAST_MAX_VALUE = 100

[1m3/7 Prérequis système / CLI[0m
  [1;32m✓[0m CLI disponible : python3
  [1;33m⚠[0m CLI absent : pip3 (installer avant `make install`)
  [1;32m✓[0m CLI disponible : docker
  [1;33m⚠[0m CLI absent : docker-compose (installer avant `make install`)
  [1;32m✓[0m CLI disponible : sshpass
  [1;32m✓[0m CLI disponible : jq
  [1;32m✓[0m Docker daemon joignable

[1m4/7 MCPs installés dans ~/.trae/mcps[0m
  [1;32m✓[0m racine MCPs : solo_agent/
  [1;32m✓[0m MCP installé : integrated_browser (17 outils)
  [1;32m✓[0m MCP installé : mcp_Docker (4 outils)
  [1;32m✓[0m MCP installé : mcp_Fetch (1 outils)
  [1;32m✓[0m MCP installé : mcp_Postgrest (2 outils)
  [1;32m✓[0m MCP installé : mcp_Sequential_Thinking (1 outils)
  [1;32m✓[0m MCP installé : mcp_context7 (2 outils)

[1m5/7 MCPs attendus projet (AGENTS.md §18)[0m
  [1;32m✓[0m SSH  →  scripts/tools/mcp_ssh (présent)
  [1;32m✓[0m Filesystem  →  implicite: Read/Grep/Glob/LS/Write (outils Trae natifs)
  [1;32m✓[0m Docker  →  mcp_Docker (natif Trae)
  [1;32m✓[0m Git  →  scripts/tools/mcp_git (présent)
  [1;32m✓[0m PostgreSQL / TimescaleDB  →  mcp_Postgrest (natif Trae)
  [1;32m✓[0m Airflow  →  scripts/tools/mcp_airflow (présent)
  [1;32m✓[0m MLflow  →  scripts/tools/mcp_mlflow (présent)
  [1;32m✓[0m Grafana  →  scripts/tools/mcp_grafana (présent)
  [1;32m✓[0m ThingsBoard  →  scripts/tools/mcp_thingsboard (présent)
  [1;32m✓[0m Coffre de secrets  →  scripts/tools/mcp_secrets (présent)
  [1;32m✓[0m Observabilité & notifications  →  scripts/tools/mcp_observability (présent)

[1m6/7 Skills natifs Trae[0m
  [1;32m✓[0m Skill natif : skill-creator
  [1;31m✗[0m SKILL NATIF MANQUANT : web-dev
  [1;32m✓[0m Skill natif : TRAE-generate-mini-app
  [1;32m✓[0m config skills : skill-config.json
  [1;32m✓[0m TRAE-dynamic-ui désactivé (attendu Trendx)
  [1;33m⚠[0m ? skill(s) natif(s) marqué(s) désactivés

[1m7/7 Skills métier Trendx — LOT 1 MVP[0m
  [1;32m✓[0m Skill LOT1 : trendx-01-solution-orchestrator  (0
0 sections détectées dans SKILL.md)
  [1;33m⚠[0m Skill LOT1 À CRÉER : trendx-03-environment-diagnostics  (utiliser `skill-creator`)
  [1;33m⚠[0m Skill LOT1 À CRÉER : trendx-04-docker-compose-operations  (utiliser `skill-creator`)
  [1;33m⚠[0m Skill LOT1 À CRÉER : trendx-05-secrets-and-configuration  (utiliser `skill-creator`)
  [1;33m⚠[0m Skill LOT1 À CRÉER : trendx-06-timescaledb-engineering  (utiliser `skill-creator`)
  [1;33m⚠[0m Skill LOT1 À CRÉER : trendx-07-thingsboard-api-integration  (utiliser `skill-creator`)
  [1;33m⚠[0m Skill LOT1 À CRÉER : trendx-08-incremental-telemetry-ingestion  (utiliser `skill-creator`)
  [1;33m⚠[0m Skill LOT1 À CRÉER : trendx-09-data-quality-and-governance  (utiliser `skill-creator`)
  [1;33m⚠[0m Skill LOT1 À CRÉER : trendx-10-time-series-preprocessing  (utiliser `skill-creator`)
  [1;33m⚠[0m Skill LOT1 À CRÉER : trendx-15-prophet-forecasting  (utiliser `skill-creator`)
  [1;33m⚠[0m Skill LOT1 À CRÉER : trendx-19-airflow-pipeline-engineering  (utiliser `skill-creator`)
  [1;33m⚠[0m Skill LOT1 À CRÉER : trendx-20-thingsboard-forecast-writeback-and-alarms  (utiliser `skill-creator`)
  [1;33m⚠[0m Skill LOT1 À CRÉER : trendx-21-grafana-observability-dashboards  (utiliser `skill-creator`)
  [1;33m⚠[0m Skill LOT1 À CRÉER : trendx-22-production-readiness  (utiliser `skill-creator`)

[1m--- Résumé ---[0m
  [1;32mPASS : 79[0m   [1;33mWARN : 16[0m   [1;31mFAIL : 1[0m
  [1;31m1 écart(s) critique(s) — appliquer scripts/deploy-mcps.sh + skill-creator.[0m

Spécifications : 
  • MCPs et SKILLS.md   (cartographie MCPs + skills)
  • skills de mise en oeuvre.md   (22 skills métier)
  • AGENTS.md §18   (11 MCPs attendus), [1m[0;36m========================================[0m
[1m[0;36m Trendx MCP - Script de déploiement     [0m
[1m[0;36m========================================[0m

[1mÉtape 1/3: Vérification de la structure des dossiers...[0m

  [0;32m[OK][0m mcp_ssh -> 4 outil(s)
  [0;32m[OK][0m mcp_git -> 6 outil(s)
  [0;32m[OK][0m mcp_airflow -> 4 outil(s)
  [0;32m[OK][0m mcp_mlflow -> 5 outil(s)
  [0;32m[OK][0m mcp_grafana -> 4 outil(s)
  [0;32m[OK][0m mcp_thingsboard -> 9 outil(s)
  [0;32m[OK][0m mcp_secrets -> 3 outil(s)
  [0;32m[OK][0m mcp_observability -> 5 outil(s)

[1mÉtape 2/3: Validation des fichiers JSON avec python3 json.tool...[0m

    [0;32m[OK][0m SERVER_METADATA.json (mcp_ssh)
    [0;32m[OK][0m tools/list_hosts.json (mcp_ssh)
    [0;32m[OK][0m tools/add_host.json (mcp_ssh)
    [0;32m[OK][0m tools/test.json (mcp_ssh)
    [0;32m[OK][0m tools/exec_command.json (mcp_ssh)
    [0;32m[OK][0m SERVER_METADATA.json (mcp_git)
    [0;32m[OK][0m tools/commit.json (mcp_git)
    [0;32m[OK][0m tools/pull.json (mcp_git)
    [0;32m[OK][0m tools/log.json (mcp_git)
    [0;32m[OK][0m tools/status.json (mcp_git)
    [0;32m[OK][0m tools/diff.json (mcp_git)
    [0;32m[OK][0m tools/push.json (mcp_git)
    [0;32m[OK][0m SERVER_METADATA.json (mcp_airflow)
    [0;32m[OK][0m tools/task_status.json (mcp_airflow)
    [0;32m[OK][0m tools/dag_run.json (mcp_airflow)
    [0;32m[OK][0m tools/list_dag_runs.json (mcp_airflow)
    [0;32m[OK][0m tools/list_dags.json (mcp_airflow)
    [0;32m[OK][0m SERVER_METADATA.json (mcp_mlflow)
    [0;32m[OK][0m tools/log_param.json (mcp_mlflow)
    [0;32m[OK][0m tools/get_run.json (mcp_mlflow)
    [0;32m[OK][0m tools/log_metric.json (mcp_mlflow)
    [0;32m[OK][0m tools/search_runs.json (mcp_mlflow)
    [0;32m[OK][0m tools/list_experiments.json (mcp_mlflow)
    [0;32m[OK][0m SERVER_METADATA.json (mcp_grafana)
    [0;32m[OK][0m tools/list_dashboards.json (mcp_grafana)
    [0;32m[OK][0m tools/list_alerts.json (mcp_grafana)
    [0;32m[OK][0m tools/datasource.json (mcp_grafana)
    [0;32m[OK][0m tools/get_dashboard.json (mcp_grafana)
    [0;32m[OK][0m SERVER_METADATA.json (mcp_thingsboard)
    [0;32m[OK][0m tools/get_device.json (mcp_thingsboard)
    [0;32m[OK][0m tools/list_timeseries.json (mcp_thingsboard)
    [0;32m[OK][0m tools/create_alarm.json (mcp_thingsboard)
    [0;32m[OK][0m tools/post_telemetry.json (mcp_thingsboard)
    [0;32m[OK][0m tools/clear_alarm.json (mcp_thingsboard)
    [0;32m[OK][0m tools/latest.json (mcp_thingsboard)
    [0;32m[OK][0m tools/get_timeseries.json (mcp_thingsboard)
    [0;32m[OK][0m tools/get_alarms.json (mcp_thingsboard)
    [0;32m[OK][0m tools/login.json (mcp_thingsboard)
    [0;32m[OK][0m SERVER_METADATA.json (mcp_secrets)
    [0;32m[OK][0m tools/put_secret.json (mcp_secrets)
    [0;32m[OK][0m tools/get_secret.json (mcp_secrets)
    [0;32m[OK][0m tools/list_secrets.json (mcp_secrets)
    [0;32m[OK][0m SERVER_METADATA.json (mcp_observability)
    [0;32m[OK][0m tools/email_send.json (mcp_observability)
    [0;32m[OK][0m tools/rules.json (mcp_observability)
    [0;32m[OK][0m tools/slack_post.json (mcp_observability)
    [0;32m[OK][0m tools/list.json (mcp_observability)
    [0;32m[OK][0m tools/prom_query.json (mcp_observability)

[1mÉtape 3/3: Tableau de bord des MCPs Trendx[0m

[1mNOM DU MCP                NB OUTILS            ÉTAT                    [0m
----------------------------------------------------------------------
mcp_ssh                   4                    [0;32mOK                       [0m
mcp_git                   6                    [0;32mOK                       [0m
mcp_airflow               4                    [0;32mOK                       [0m
mcp_mlflow                5                    [0;32mOK                       [0m
mcp_grafana               4                    [0;32mOK                       [0m
mcp_thingsboard           9                    [0;32mOK                       [0m
mcp_secrets               3                    [0;32mOK                       [0m
mcp_observability         5                    [0;32mOK                       [0m
----------------------------------------------------------------------

[1mRÉSUMÉ:[0m
  MCPs déclarés:       8
  MCPs valides:        [0;32m8[0m/8
  Outils totaux:       40
  Erreurs détectées:   [0;31m0[0m

[0;32m[1m✓ Déploiement MCP Trendx réussi ![0m
- >>> [1;33m[pre-commit] Exécution de 'git status'...[0m
 M .gitignore
 M "MCPs et SKILLS.md"
 D agent.md
 M docs/mvp-scope.md
?? .env.example
?? .pre-commit-config.yaml
?? AGENTS.md
?? Makefile
?? README.md
?? constraints.txt
?? dags/
?? docker-compose.yml
?? docker/
?? grafana/
?? migrations/
?? pyproject.toml
?? scripts/
?? skills/
?? src/
?? tests/

>>> [1;33m[pre-commit] Exécution de 'git diff --cached'...[0m
[0;31mERREUR : Aucun fichier n'est indexé (staged). Utilisez 'git add'.[0m (activation hook)
- Usage:  docker compose [OPTIONS] COMMAND

Define and run multi-container applications with Docker

Options:
      --all-resources              Include all resources, even those not
                                   used by services
      --ansi string                Control when to print ANSI control
                                   characters ("never"|"always"|"auto")
                                   (default "auto")
      --compatibility              Run compose in backward compatibility mode
      --dry-run                    Execute command in dry run mode
      --env-file stringArray       Specify an alternate environment file
  -f, --file stringArray           Compose configuration files
      --parallel int               Control max parallelism, -1 for
                                   unlimited (default -1)
      --profile stringArray        Specify a profile to enable
      --progress string            Set type of progress output (auto,
                                   tty, plain, json, quiet)
      --project-directory string   Specify an alternate working directory
                                   (default: the path of the, first
                                   specified, Compose file)
  -p, --project-name string        Project name

Management Commands:
  bridge      Convert compose files into another model

Commands:
  attach      Attach local standard input, output, and error streams to a service's running container
  build       Build or rebuild services
  commit      Create a new image from a service container's changes
  config      Parse, resolve and render compose file in canonical format
  cp          Copy files/folders between a service container and the local filesystem
  create      Creates containers for a service
  down        Stop and remove containers, networks
  events      Receive real time events from containers
  exec        Execute a command in a running container
  export      Export a service container's filesystem as a tar archive
  images      List images used by the created containers
  kill        Force stop service containers
  logs        View output from containers
  ls          List running compose projects
  pause       Pause services
  port        Print the public port for a port binding
  ps          List containers
  publish     Publish compose application
  pull        Pull service images
  push        Push service images
  restart     Restart service containers
  rm          Removes stopped service containers
  run         Run a one-off command on a service
  scale       Scale services 
  start       Start services
  stats       Display a live stream of container(s) resource usage statistics
  stop        Stop services
  top         Display the running processes
  unpause     Unpause services
  up          Create and start containers
  version     Show the Docker Compose version information
  volumes     List volumes
  wait        Block until containers of all (or specified) services stop.
  watch       Watch build context for service and rebuild/refresh containers when files are updated

Run 'docker compose COMMAND --help' for more information on a command.
- 

## 9. Contrôles avant modification

- Healthcheck Docker / Postgres / Airflow / MLflow / Grafana
- On branch master
Changes not staged for commit:
  (use "git add/rm <file>..." to update what will be committed)
  (use "git restore <file>..." to discard changes in working directory)
	modified:   .gitignore
	modified:   MCPs et SKILLS.md
	deleted:    agent.md
	modified:   docs/mvp-scope.md

Untracked files:
  (use "git add <file>..." to include in what will be committed)
	.env.example
	.pre-commit-config.yaml
	AGENTS.md
	Makefile
	README.md
	constraints.txt
	dags/
	docker-compose.yml
	docker/
	grafana/
	migrations/
	pyproject.toml
	scripts/
	skills/
	src/
	tests/

no changes added to commit (use "git add" and/or "git commit -a") + 
- permissions .env 600, absence de secret dans la sortie
- Variables attendues toutes renseignées dans .env

## 10. Critères de succès

- Toutes les étapes PASS (aucun FAIL bloquant)
- Absence d'erreurs fatales dans les logs
- Idempotence : réexécuter donne le même état
- Watermark / métadonnées à jour si applicables

## 11. Gestion des erreurs

- WARNING : journaliser et continuer si non bloquant
- BLOCKING : interrompre immédiatement, proposer correction
- Timeout API HTTP : retry exponentiel + max 3 tentatives
- Erreurs SQL : rollback + log + avertissement

## 12. Rollback

- docker compose down pour la stack Docker
- Restaurer backup SQL TimescaleDB
- Remettre dernier watermark valide
- Recharger dernier modèle MLflow validé

## 13. Garde-fous de sécurité

- Ne jamais afficher .env en clair ; masquer PASSWORD/SECRET/TOKEN
- Ne jamais commit .env / secrets (vérifié via .gitignore)
- Writeback ThingsBoard et alarmes : validation explicite en production
- Exposer Grafana/Airflow/MLflow en HTTPS + authentification forte

## 14. Format de sortie

Rapport structuré avec :
- Statut PASS/WARNING/BLOCKING
- Liste des actions effectuées
- Métriques (nb points, nb rows, durée, erreurs)
- Prochaines étapes recommandées

## 15. Tests de validation

- Smoke tests de chaque outil CLI
- Tests unitaires Python (pytest src/trendx tests/)
- Tests d'intégration Airflow DAG parse
- End-to-end : TB → Timescale → forecast → _EPD_ writeback
