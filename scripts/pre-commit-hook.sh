#!/usr/bin/env bash
# ========================================
# Pre-commit hook : vérifications Git
# Vérifie explicitement git status et
# git diff --cached avant chaque commit
# ========================================
set -euo pipefail

RED='\033[0;31m'
YELLOW='\033[1;33m'
GREEN='\033[0;32m'
NC='\033[0m'

echo -e ">>> ${YELLOW}[pre-commit] Exécution de 'git status'...${NC}"
if ! git status --short ; then
    echo -e "${RED}ERREUR : Échec de 'git status'${NC}"
    exit 1
fi

echo ""
echo -e ">>> ${YELLOW}[pre-commit] Exécution de 'git diff --cached'...${NC}"
if ! git diff --cached --stat ; then
    echo -e "${RED}ERREUR : Échec de 'git diff --cached'${NC}"
    exit 1
fi

STAGED_COUNT=$(git diff --cached --name-only | wc -l | tr -d ' ')
if [ "$STAGED_COUNT" -eq 0 ]; then
    echo -e "${RED}ERREUR : Aucun fichier n'est indexé (staged). Utilisez 'git add'.${NC}"
    exit 1
fi

# Détection préventive de secrets
if git diff --cached | grep -E "(password\s*=|secret\s*=|api_key\s*=|token\s*=)" --ignore-case ; then
    echo -e "${RED}ATTENTION : détection de potentiels secrets dans le commit. Vérifiez avec 'git diff --cached'.${NC}"
    echo -e "${YELLOW}→ Si ces valeurs sont OK, reprenez avec 'SKIP=secret-check git commit ...'${NC}"
fi

echo ""
echo -e ">>> ${GREEN}Vérifications pre-commit OK${NC} (${STAGED_COUNT} fichiers indexés)"
exit 0
