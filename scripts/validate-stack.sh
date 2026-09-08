#!/usr/bin/env bash
# ========================================
# Trendx - validate-stack.sh
# Validation complète des skills et MCPs Trae
# Usage : scripts/validate-stack.sh
# ========================================
set -u
set -o pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
ENV_FILE="${ROOT}/.env"
SPEC_FILE="${ROOT}/MCPs et SKILLS.md"
SKILLS_SPEC="${ROOT}/skills de mise en oeuvre.md"

RED=$'\033[1;31m'; GRN=$'\033[1;32m'; YLW=$'\033[1;33m'; BLD=$'\033[1m'; RST=$'\033[0m'
PASS=0; WARN=0; FAIL=0

pass() { PASS=$((PASS+1)); printf "  ${GRN}✓${RST} %s\n" "$*"; }
warn() { WARN=$((WARN+1)); printf "  ${YLW}⚠${RST} %s\n" "$*"; }
fail() { FAIL=$((FAIL+1)); printf "  ${RED}✗${RST} %s\n" "$*"; }

echo "${BLD}=== Trendx — Validation stack Skills/MCPs${RST} (projet=$(basename "$ROOT"))"
echo

# --- 1. Fichiers et dossiers racine ---
echo "${BLD}1/7 Arborescence attendue${RST}"
for f in "README.md" "AGENTS.md" ".gitignore" ".env.example" \
         "pyproject.toml" "docker-compose.yml" "Makefile" \
         "MCPs et SKILLS.md" "Procédure de mise en œuvre Trendx.md" \
         "skills de mise en oeuvre.md"; do
  if [ -f "${ROOT}/$f" ]; then
    pass "fichier présent : $f"
  else
    fail "FICHIER MANQUANT : $f"
  fi
done
for d in docs docker migrations dags src grafana scripts tests config; do
  if [ -d "${ROOT}/$d" ]; then
    pass "dossier présent : $d/"
  else
    fail "DOSSIER MANQUANT : $d/"
  fi
done
echo

# --- 2. .env & secrets ---
echo "${BLD}2/7 Fichier .env et variables attendues${RST}"
if [ ! -f "$ENV_FILE" ]; then
  fail "Fichier .env absent (copiez .env.example puis renseigner les secrets)"
else
  pass ".env présent"
  perms=$(stat -c '%a' "$ENV_FILE" 2>/dev/null || echo "")
  if [ "$perms" = "600" ]; then
    pass "droits .env = 600"
  else
    warn "droits .env = $perms (attendu 600)"
  fi
  for k in TRENDX_ENV TRENDX_LOG_LEVEL TRENDX_TIMEZONE \
           TB_BASE_URL TB_USERNAME TB_PASSWORD TB_DEVICE_ID TB_METRIC_NAME \
           ANALYTICS_DB_HOST ANALYTICS_DB_PORT ANALYTICS_DB_NAME ANALYTICS_DB_USER ANALYTICS_DB_PASSWORD \
           AIRFLOW_DB_NAME AIRFLOW_DB_USER AIRFLOW_DB_PASSWORD AIRFLOW_HOST_PORT \
           MLFLOW_TRACKING_URI MLFLOW_HOST_PORT \
           GRAFANA_HOST_PORT GRAFANA_ADMIN_USER GRAFANA_ADMIN_PASSWORD \
           FORECAST_FREQUENCY FORECAST_HORIZON TRAINING_LOOKBACK_DAYS \
           ANOMALY_DETECTION_ENABLED ANOMALY_CONTAMINATION ANOMALY_WINDOW_SIZE \
           FORECAST_MIN_VALUE FORECAST_MAX_VALUE; do
    v=$(grep -E "^${k}=" "$ENV_FILE" 2>/dev/null | head -1 | cut -d= -f2-)
    if [ -n "${v:-}" ]; then
      case "$k" in *PASSWORD|*SECRET|*TOKEN) pass "$k = ***" ;; *) pass "$k = $v" ;; esac
    else
      warn "VARIABLE ABSENTE/VIDE dans .env : $k"
    fi
  done
fi
echo

# --- 3. Prérequis système / CLI ---
echo "${BLD}3/7 Prérequis système / CLI${RST}"
for t in python3 pip3 docker docker-compose sshpass jq; do
  available=false
  display="$t"
  if command -v "$t" >/dev/null 2>&1; then
    available=true
  elif [ "$t" = "pip3" ] && python3 -m pip --version >/dev/null 2>&1; then
    available=true
    display="python3 -m pip"
  elif [ "$t" = "docker-compose" ] && docker compose version >/dev/null 2>&1; then
    available=true
    display="docker compose"
  fi

  if [ "$available" = true ]; then
    pass "CLI disponible : $display"
  else
    case "$t" in
      jq|sshpass) warn "CLI absent : $t (fortement recommandé)" ;;
      *)           warn "CLI absent : $t (installer avant \`make install\`)" ;;
    esac
  fi
done
if command -v docker >/dev/null 2>&1; then
  if docker info >/dev/null 2>&1; then pass "Docker daemon joignable"
  else warn "Docker daemon non joignable par l'utilisateur courant"; fi
fi
echo

# --- 4. MCPs installés dans ~/.trae/mcps ---
echo "${BLD}4/7 MCPs installés dans ~/.trae/mcps${RST}"
MCP_HOME="${HOME}/.trae/mcps/s_Trendx-144e020c/solo_agent"
if [ -d "$MCP_HOME" ]; then
  pass "racine MCPs : $(basename "$MCP_HOME")/"
else
  fail "DOSSIER MCPs RACINE MANQUANT"
fi
for mcp in integrated_browser mcp_Docker mcp_Fetch mcp_Postgrest mcp_Sequential_Thinking mcp_context7; do
  tools_dir="${MCP_HOME}/${mcp}/tools"
  if [ -d "$tools_dir" ]; then
    shopt -s nullglob; files=("$tools_dir"/*.json); shopt -u nullglob
    n=${#files[@]}
    if [ "$n" -gt 0 ]; then pass "MCP installé : $mcp ($n outils)"
    else fail "MCP SANS OUTILS : $mcp"; fi
  else
    fail "MCP INCOMPLET : $mcp"
  fi
done
echo

# --- 5. MCPs attendus AGENTS.md §18 ---
echo "${BLD}5/7 MCPs attendus projet (AGENTS.md §18)${RST}"
MCP_CIBLES=(
  "SSH|scripts/tools/mcp_ssh"
  "Filesystem|implicite: Read/Grep/Glob/LS/Write (outils Trae natifs)"
  "Docker|mcp_Docker:native"
  "Git|scripts/tools/mcp_git"
  "PostgreSQL / TimescaleDB|mcp_Postgrest:native"
  "Airflow|scripts/tools/mcp_airflow"
  "MLflow|scripts/tools/mcp_mlflow"
  "Grafana|scripts/tools/mcp_grafana"
  "ThingsBoard|scripts/tools/mcp_thingsboard"
  "Coffre de secrets|scripts/tools/mcp_secrets"
  "Observabilité & notifications|scripts/tools/mcp_observability"
)
for row in "${MCP_CIBLES[@]}"; do
  cible="${row%%|*}"
  chemin="${row#*|}"
  case "$chemin" in
    *":native")
      mcname="${chemin%:native}"
      if [ -d "${MCP_HOME}/${mcname}" ]; then
        pass "$cible  →  $mcname (natif Trae)"
      else
        fail "$cible  →  $mcname INTROUVABLE dans ~/.trae/mcps"
      fi
      ;;
    "implicite:"*)
      pass "$cible  →  $chemin"
      ;;
    *)
      if [ -d "${ROOT}/${chemin}" ]; then
        shopt -s nullglob; f=("${ROOT}/${chemin}"/*/SERVER_METADATA.json "${ROOT}/${chemin}/SERVER_METADATA.json"); shopt -u nullglob
        pass "$cible  →  ${chemin} (présent)"
      else
        fail "$cible  →  ${chemin}  (À CRÉER)"
      fi
      ;;
  esac
done
echo

# --- 6. Skills natifs Trae + skill-config.json ---
echo "${BLD}6/7 Skills natifs Trae${RST}"
SKILL_HOME="${HOME}/.trae/builtin/global/skills"
SKILL_HOME_2="${HOME}/.trae/builtin_skills"
for sk in skill-creator web-dev; do
  if [ -f "${SKILL_HOME}/${sk}/SKILL.md" ]; then
    pass "Skill natif : $sk"
  else
    fail "SKILL NATIF MANQUANT : $sk"
  fi
done
if [ -f "${SKILL_HOME_2}/TRAE-generate-mini-app/SKILL.md" ]; then
  pass "Skill natif : TRAE-generate-mini-app"
else
  fail "SKILL NATIF MANQUANT : TRAE-generate-mini-app"
fi
SC="${HOME}/.trae/skill-config.json"
if [ -f "$SC" ]; then
  pass "config skills : $(basename "$SC")"
  dyn=$(python3 -c "import json; print(json.load(open('$SC')).get('builtinSkillStatus',{}).get('TRAE-dynamic-ui','?'))" 2>/dev/null || echo "?")
  case "$dyn" in False|false) pass "TRAE-dynamic-ui désactivé (attendu Trendx)" ;; *) warn "TRAE-dynamic-ui=$dyn (valeur attendue : false)" ;; esac
   n_dis=$(python3 -c "import json; print(len(json.load(open('$SC')).get('disabledSkills',[])))" 2>/dev/null || echo "?")
  if [ "$n_dis" = "0" ]; then
    pass "Aucun skill natif désactivé"
  else
    warn "$n_dis skill(s) natif(s) marqué(s) désactivés"
  fi
else
  warn "fichier skill-config.json absent — valeurs par défaut appliquées"
fi
echo

# --- 7. 14 Skills métier Trendx LOT 1 MVP ---
echo "${BLD}7/7 Skills métier Trendx — LOT 1 MVP${RST}"
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
SKILLS_ROOT="${ROOT}/skills"
for sk in "${LOT1[@]}"; do
  SKMD="${SKILLS_ROOT}/${sk}/SKILL.md"
  if [ -f "$SKMD" ]; then
    n_sec=$(grep -cE '^## [0-9]' "$SKMD" 2>/dev/null || echo 0)
    pass "Skill LOT1 : $sk  ($n_sec sections détectées dans SKILL.md)"
  elif [ -d "${HOME}/.trae/skills/${sk}" ]; then
    pass "Skill LOT1 (Trae managed) : $sk"
  else
    warn "Skill LOT1 À CRÉER : $sk  (utiliser \`skill-creator\`)"
  fi
done
echo

# --- Résumé ---
echo "${BLD}--- Résumé ---${RST}"
printf "  ${GRN}PASS : %d${RST}   ${YLW}WARN : %d${RST}   ${RED}FAIL : %d${RST}\n" "$PASS" "$WARN" "$FAIL"
if [ "$FAIL" -eq 0 ] && [ "$WARN" -eq 0 ]; then
  echo "  ${GRN}Stack 100 % conforme aux spécifications MCPs et SKILLS.md.${RST}"
elif [ "$FAIL" -eq 0 ]; then
  echo "  ${YLW}Stack globalement conforme — ${WARN} avertissement(s), appliquer si possible.${RST}"
else
  echo "  ${RED}${FAIL} écart(s) critique(s) — appliquer scripts/deploy-mcps.sh + skill-creator.${RST}"
fi
echo
echo  "Spécifications : "
echo  "  • $(basename "$SPEC_FILE")   (cartographie MCPs + skills)"
echo  "  • $(basename "$SKILLS_SPEC")   (22 skills métier)"
echo  "  • AGENTS.md §18   (11 MCPs attendus)"
exit $FAIL
