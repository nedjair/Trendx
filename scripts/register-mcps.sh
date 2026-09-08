#!/bin/bash
# register-mcps.sh - Enregistrement des MCPs Trendx dans ~/.trae/mcps
set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PROJECT_MCP_DIR="${ROOT}/scripts/tools"
TRAE_MCP_BASE="${HOME}/.trae/mcps/s_Trendx-144e020c"
SOLO_AGENT_DIR="${TRAE_MCP_BASE}/solo_agent"
GENERAL_DIR="${TRAE_MCP_BASE}/general_purpose_task"

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
echo -e "${BOLD}${CYAN} Trendx - Enregistrement MCPs Trae      ${NC}"
echo -e "${BOLD}${CYAN}========================================${NC}"
echo ""

# Vérifier les répertoires cibles
for dir in "$SOLO_AGENT_DIR" "$GENERAL_DIR"; do
  if [ ! -d "$dir" ]; then
    echo -e "  ${YELLOW}[INFO]${NC} Création du répertoire: $dir"
    mkdir -p "$dir"
  fi
done

# Fonction de conversion du format MCP projet → format Trae
convert_mcp() {
  local src_dir="$1"
  local dst_dir="$2"
  local mcp_name="$3"

  echo -e "  ${CYAN}[MCP]${NC} Enregistrement de ${mcp_name}..."

  # Créer le répertoire de destination
  mkdir -p "${dst_dir}/${mcp_name}/tools"

  # Convertir SERVER_METADATA.json
  if [ -f "${src_dir}/SERVER_METADATA.json" ]; then
    if python3 -c "
import json, sys
with open('${src_dir}/SERVER_METADATA.json') as f:
    data = json.load(f)
# Conversion au format Trae
trae_meta = {
    'server_name': data.get('name', '${mcp_name}'),
    'description': None
}
with open('${dst_dir}/${mcp_name}/SERVER_METADATA.json', 'w') as f:
    json.dump(trae_meta, f, indent=2)
    f.write('\n')
" 2>/dev/null; then
      echo -e "    ${GREEN}[OK]${NC} SERVER_METADATA.json converti"
    else
      echo -e "    ${RED}[ERR]${NC} Échec conversion SERVER_METADATA.json"
      return 1
    fi
  else
    echo -e "    ${RED}[ERR]${NC} SERVER_METADATA.json manquant dans ${src_dir}"
    return 1
  fi

  # Convertir les outils
  if [ -d "${src_dir}/tools" ]; then
    local tool_count=0
    for tool_file in "${src_dir}/tools"/*.json; do
      [ -f "$tool_file" ] || continue
      local fname
      fname=$(basename "$tool_file")
      if python3 -c "
import json, sys
with open('${tool_file}') as f:
    data = json.load(f)
# Conversion inputSchema → arguments
if 'inputSchema' in data:
    data['arguments'] = data.pop('inputSchema')
with open('${dst_dir}/${mcp_name}/tools/${fname}', 'w') as f:
    json.dump(data, f, indent=2)
    f.write('\n')
" 2>/dev/null; then
        tool_count=$((tool_count + 1))
      else
        echo -e "    ${RED}[ERR]${NC} Échec conversion ${fname}"
      fi
    done
    echo -e "    ${GREEN}[OK]${NC} ${tool_count} outil(s) converti(s)"
  else
    echo -e "    ${YELLOW}[WARN]${NC} Aucun outil dans ${src_dir}/tools"
  fi

  return 0
}

ERRORS=0

# Enregistrer chaque MCP dans solo_agent et general_purpose_task
for mcp in "${MCP_LIST[@]}"; do
  src_dir="${PROJECT_MCP_DIR}/${mcp}"

  if [ ! -d "$src_dir" ]; then
    echo -e "  ${RED}[KO]${NC} Dossier source manquant: ${src_dir}"
    ERRORS=$((ERRORS + 1))
    continue
  fi

  # Enregistrer dans solo_agent
  if ! convert_mcp "$src_dir" "$SOLO_AGENT_DIR" "$mcp"; then
    ERRORS=$((ERRORS + 1))
  fi

  # Enregistrer dans general_purpose_task
  if ! convert_mcp "$src_dir" "$GENERAL_DIR" "$mcp"; then
    ERRORS=$((ERRORS + 1))
  fi

  echo ""
done

# Vérification finale
echo -e "${BOLD}Étape 2/2: Vérification de l'enregistrement${NC}"
echo ""

TOTAL_OK=0
for mcp in "${MCP_LIST[@]}"; do
  for dir in "$SOLO_AGENT_DIR" "$GENERAL_DIR"; do
    if [ -d "${dir}/${mcp}/tools" ]; then
      tool_count=$(find "${dir}/${mcp}/tools" -maxdepth 1 -type f -name "*.json" | wc -l)
      if [ "$tool_count" -gt 0 ]; then
        echo -e "  ${GREEN}[OK]${NC} ${mcp} → $(basename "$dir") (${tool_count} outils)"
        TOTAL_OK=$((TOTAL_OK + 1))
      else
        echo -e "  ${RED}[KO]${NC} ${mcp} → $(basename "$dir") (aucun outil)"
        ERRORS=$((ERRORS + 1))
      fi
    else
      echo -e "  ${RED}[KO]${NC} ${mcp} → $(basename "$dir") (répertoire tools manquant)"
      ERRORS=$((ERRORS + 1))
    fi
  done
done

echo ""
echo -e "${BOLD}RÉSUMÉ:${NC}"
echo -e "  MCPs enregistrés:       ${#MCP_LIST[@]}"
echo -e "  Emplacements:           ${GREEN}2${NC} (solo_agent + general_purpose_task)"
echo -e "  Total attendu:          $(( ${#MCP_LIST[@]} * 2 ))"
echo -e "  Enregistrements OK:     ${GREEN}${TOTAL_OK}${NC}"
echo -e "  Erreurs détectées:      ${RED}${ERRORS}${NC}"
echo ""

if [ "$ERRORS" -eq 0 ] && [ "$TOTAL_OK" -eq $(( ${#MCP_LIST[@]} * 2 )) ]; then
  echo -e "${GREEN}${BOLD}✓ Enregistrement MCP Trendx réussi !${NC}"
  exit 0
else
  echo -e "${RED}${BOLD}✗ Enregistrement MCP Trendx terminé avec erreurs.${NC}"
  exit 1
fi
