#!/bin/bash
# deploy-mcps.sh - Déploiement et validation des serveurs MCP Trendx

set -u

BASE_DIR="$(cd "$(dirname "$0")" && pwd)"
TOOLS_DIR="${BASE_DIR}/tools"

MCP_LIST=(
  "mcp_ssh"
  "mcp_git"
  "mcp_airflow"
  "mcp_mlflow"
  "mcp_grafana"
  "mcp_thingsboard"
  "mcp_secrets"
  "mcp_observability"
)

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

echo -e "${BOLD}${CYAN}========================================${NC}"
echo -e "${BOLD}${CYAN} Trendx MCP - Script de déploiement     ${NC}"
echo -e "${BOLD}${CYAN}========================================${NC}"
echo ""

ERRORS=0
declare -a MCP_STATUS=()

echo -e "${BOLD}Étape 1/3: Vérification de la structure des dossiers...${NC}"
echo ""
for mcp in "${MCP_LIST[@]}"; do
  mcp_path="${TOOLS_DIR}/${mcp}"
  tools_path="${mcp_path}/tools"
  metadata_path="${mcp_path}/SERVER_METADATA.json"

  if [ ! -d "${mcp_path}" ]; then
    echo -e "  ${RED}[KO]${NC} Dossier manquant: ${mcp_path}"
    MCP_STATUS+=("${mcp}|0|KO (dossier absent)")
    ERRORS=$((ERRORS + 1))
    continue
  fi

  if [ ! -d "${tools_path}" ]; then
    echo -e "  ${RED}[KO]${NC} Dossier tools/ manquant: ${tools_path}"
    MCP_STATUS+=("${mcp}|0|KO (tools/ absent)")
    ERRORS=$((ERRORS + 1))
    continue
  fi

  if [ ! -f "${metadata_path}" ]; then
    echo -e "  ${RED}[KO]${NC} Fichier manquant: ${metadata_path}"
    MCP_STATUS+=("${mcp}|0|KO (metadata absent)")
    ERRORS=$((ERRORS + 1))
    continue
  fi

  tool_count=$(find "${tools_path}" -maxdepth 1 -type f -name "*.json" | wc -l)
  echo -e "  ${GREEN}[OK]${NC} ${mcp} -> ${tool_count} outil(s)"
  MCP_STATUS+=("${mcp}|${tool_count}|OK")
done
echo ""

echo -e "${BOLD}Étape 2/3: Validation des fichiers JSON avec python3 json.tool...${NC}"
echo ""
for entry in "${MCP_STATUS[@]}"; do
  IFS='|' read -r mcp tool_count status <<< "${entry}"
  mcp_path="${TOOLS_DIR}/${mcp}"
  metadata_path="${mcp_path}/SERVER_METADATA.json"

  if [[ "${status}" != "OK"* ]]; then
    echo -e "  ${YELLOW}[SKIP]${NC} ${mcp} (état précédent: ${status})"
    continue
  fi

  valid=true

  if python3 -m json.tool "${metadata_path}" >/dev/null 2>&1; then
    echo -e "    ${GREEN}[OK]${NC} SERVER_METADATA.json (${mcp})"
  else
    echo -e "    ${RED}[ERR]${NC} JSON invalide: SERVER_METADATA.json (${mcp})"
    valid=false
    ERRORS=$((ERRORS + 1))
  fi

  tools_path="${mcp_path}/tools"
  while IFS= read -r -d '' tool_file; do
    fname=$(basename "${tool_file}")
    if python3 -m json.tool "${tool_file}" >/dev/null 2>&1; then
      echo -e "    ${GREEN}[OK]${NC} tools/${fname} (${mcp})"
    else
      echo -e "    ${RED}[ERR]${NC} JSON invalide: tools/${fname} (${mcp})"
      valid=false
      ERRORS=$((ERRORS + 1))
    fi
  done < <(find "${tools_path}" -maxdepth 1 -type f -name "*.json" -print0)

  if [ "${valid}" = false ]; then
    for i in "${!MCP_STATUS[@]}"; do
      if [[ "${MCP_STATUS[$i]}" == "${mcp}|"* ]]; then
        MCP_STATUS[i]="${mcp}|${tool_count}|JSON_INVALIDE"
        break
      fi
    done
  fi
done
echo ""

echo -e "${BOLD}Étape 3/3: Tableau de bord des MCPs Trendx${NC}"
echo ""

printf "${BOLD}%-25s %-20s %-25s${NC}\n" "NOM DU MCP" "NB OUTILS" "ÉTAT"
printf "%0.s-" {1..70} ; echo ""
for entry in "${MCP_STATUS[@]}"; do
  IFS='|' read -r mcp tool_count status <<< "${entry}"
  if [[ "${status}" == "OK" ]]; then
    col="${GREEN}"
  elif [[ "${status}" == "JSON_INVALIDE" ]]; then
    col="${YELLOW}"
  else
    col="${RED}"
  fi
  printf "%-25s %-20s ${col}%-25s${NC}\n" "${mcp}" "${tool_count}" "${status}"
done
printf "%0.s-" {1..70} ; echo ""

TOTAL_TOOLS=0
TOTAL_OK=0
for entry in "${MCP_STATUS[@]}"; do
  IFS='|' read -r mcp tool_count status <<< "${entry}"
  TOTAL_TOOLS=$((TOTAL_TOOLS + tool_count))
  if [[ "${status}" == "OK" ]]; then
    TOTAL_OK=$((TOTAL_OK + 1))
  fi
done

echo ""
echo -e "${BOLD}RÉSUMÉ:${NC}"
echo -e "  MCPs déclarés:       ${#MCP_LIST[@]}"
echo -e "  MCPs valides:        ${GREEN}${TOTAL_OK}${NC}/${#MCP_LIST[@]}"
echo -e "  Outils totaux:       ${TOTAL_TOOLS}"
echo -e "  Erreurs détectées:   ${RED}${ERRORS}${NC}"
echo ""

if [ "${ERRORS}" -eq 0 ] && [ "${TOTAL_OK}" -eq "${#MCP_LIST[@]}" ]; then
  echo -e "${GREEN}${BOLD}✓ Déploiement MCP Trendx réussi !${NC}"
  exit 0
else
  echo -e "${RED}${BOLD}✗ Déploiement MCP Trendx terminé avec erreurs.${NC}"
  exit 1
fi
