#!/usr/bin/env bash
# =========================================
# generate-lot1-skills.sh
# Crée 14 skills LOT1 MVP Trendx dans skills/
#   + SKILL.md conforme (frontmatter name+description)
#   + sous-dossiers references/ templates/ scripts/ tests/
# =========================================
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SKILLS_DIR="${ROOT}/skills"
mkdir -p "$SKILLS_DIR"

declare -A SKILL_DESC_FULL
SKILL_DESC_FULL["trendx-01-solution-orchestrator"]="Orchestre la mise en œuvre Trendx de bout en bout. Invoke en phase d'installation initiale, MVP, ajout de modèle, diagnostic ou reprise."
SKILL_DESC_FULL["trendx-03-environment-diagnostics"]="Audite les prérequis d'installation Trendx (SSH, Docker, ports, TB existant). Invoke avant toute modification d'infrastructure ou déploiement Docker."
SKILL_DESC_FULL["trendx-04-docker-compose-operations"]="Déploie et maintient la stack Docker Compose Trendx (TimescaleDB/Airflow/MLflow/Grafana). Invoke pour up/down/config/healthcheck/logs/rollback."
SKILL_DESC_FULL["trendx-05-secrets-and-configuration"]="Gère .env, identifiants et secrets Trendx sans les exposer. Invoke lors de l'initialisation, rotation ou passage d'environnement (dev/stg/prod)."
SKILL_DESC_FULL["trendx-06-timescaledb-engineering"]="Crée hypertables, agrégats continus et politiques TimescaleDB. Invoke pour schema, migrations, retention/compression ou performance SQL."
SKILL_DESC_FULL["trendx-07-thingsboard-api-integration"]="Client robuste API ThingsBoard (auth, devices, télémétrie, alarmes). Invoke pour discover devices/keys, lecture historique, writeback, alarms."
SKILL_DESC_FULL["trendx-08-incremental-telemetry-ingestion"]="Ingestion incrémentale idempotente TB → TimescaleDB avec watermark. Invoke pour backfill 90j, ingestion horaire, reprise, réextraction."
SKILL_DESC_FULL["trendx-09-data-quality-and-governance"]="Contrôle qualité données avant ML (NaN, doublons, fréquence, domaines). Invoke après ingestion et avant entraînement / prévision."
SKILL_DESC_FULL["trendx-10-time-series-preprocessing"]="Pré-traite séries temporelles (rééchantillonnage, imputation, features, scaling). Invoke systématiquement avant tout entraînement ou forecast."
SKILL_DESC_FULL["trendx-15-prophet-forecasting"]="Entraîne et génère prévisions Prophet (tendance + saisonnalités + cap/floor). Invoke pour baseline prévision et compétition modèles."
SKILL_DESC_FULL["trendx-19-airflow-pipeline-engineering"]="Crée et exploite DAG Airflow Trendx (ingestion, train, forecast, anomalies, writeback). Invoke pour création DAG, planification, retry, backfill."
SKILL_DESC_FULL["trendx-20-thingsboard-forecast-writeback-and-alarms"]="Réinjecte _EPD_ prévisions et gère alarmes ThingsBoard. Invoke après génération forecast/anomalies pour writeback et alerting."
SKILL_DESC_FULL["trendx-21-grafana-observability-dashboards"]="Provisionne datasource + dashboards Grafana Trendx (réel/prédit, anomalies, KPI). Invoke pour création/mise à jour dashboards ou datasource."
SKILL_DESC_FULL["trendx-22-production-readiness"]="Tests, sécurité, observabilité, sauvegardes et reprise Trendx. Invoke avant passage en production, audits ou après incident."

LOT1=(
  "trendx-01-solution-orchestrator"
  "trendx-03-environment-diagnostics"
  "trendx-04-docker-compose-operations"
  "trendx-05-secrets-and-configuration"
  "trendx-06-timescaledb-engineering"
  "trendx-07-thingsboard-api-integration"
  "trendx-08-incremental-telemetry-ingestion"
  "trendx-09-data-quality-and-governance"
  "trendx-10-time-series-preprocessing"
  "trendx-15-prophet-forecasting"
  "trendx-19-airflow-pipeline-engineering"
  "trendx-20-thingsboard-forecast-writeback-and-alarms"
  "trendx-21-grafana-observability-dashboards"
  "trendx-22-production-readiness"
)

for sk in "${LOT1[@]}"; do
  skill_dir="${SKILLS_DIR}/${sk}"
  mkdir -p "$skill_dir"/{references,templates,scripts,tests}

  # SKILL.md frontmatter conforme au standard skill-creator Trae
  cat > "${skill_dir}/SKILL.md" <<EOF
---
name: "${sk}"
description: "${SKILL_DESC_FULL[$sk]}"
---

# ${sk//-/ }

## 1. Objectif

${SKILL_DESC_FULL[$sk]}

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
- Pre-commit workflow : \`git status\` puis \`git diff --cached\` avant commit

## 7. Procédure pas à pas

1. Diagnostiquer l'état actuel (prérequis / santé services)
2. Déterminer la sous-commande / phase à exécuter
3. Appliquer les modifications de manière idempotente
4. Inspecter logs / résultats
5. Mettre à jour watermark / métadonnées si besoin
6. Produire un rapport de succès ou d'erreur

## 8. Commandes ou scripts autorisés

- \`make install / up / down / status / logs\`
- \`scripts/validate-stack.sh\`, \`scripts/deploy-mcps.sh\`
- \`scripts/pre-commit-hook.sh\` (activation hook)
- \`docker compose -f docker-compose.yml ...\`
- \`python3 src/trendx/...\`

## 9. Contrôles avant modification

- Healthcheck Docker / Postgres / Airflow / MLflow / Grafana
- \`git status\` + \`git diff --cached\`
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

- \`make down\` pour la stack Docker
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
EOF

  # Sous-dossiers : README simples pour attester du contenu
  for sub in references templates scripts tests; do
    cat > "${skill_dir}/${sub}/README.md" <<EOF
# ${sub^} - ${sk}

Contenu pour \`${sub}\` du skill **${sk}**.

> Consulter \`SKILL.md\` à la racine du skill pour la procédure détaillée.
EOF
  done

  touch "${skill_dir}/scripts/.gitkeep" "${skill_dir}/tests/.gitkeep"
done

echo "✓ 14 skills LOT1 MVP créés dans ${SKILLS_DIR}/ :"
for sk in "${LOT1[@]}"; do
  n_sec=$(grep -cE '^## [0-9]+' "${SKILLS_DIR}/${sk}/SKILL.md" 2>/dev/null || echo 0)
  printf '    · %-50s  SKILL.md (%02d sections) + 4 sous-dossiers\n' "$sk" "$n_sec"
done
