#!/bin/bash
# Trendx Healthcheck - vérifie l'état de l'environnement
set -euo pipefail

echo "=== Trendx — Environment Readiness Check ==="
echo ""

# Docker
echo -n "Docker daemon : "
if docker info --format '{{.ServerVersion}}' >/dev/null 2>&1; then
    echo "OK (version $(docker info --format '{{.ServerVersion}}'))"
else
    echo "FAIL - Docker not running"
    exit 1
fi

echo -n "Docker Compose : "
if docker compose version >/dev/null 2>&1; then
    echo "OK ($(docker compose version --short 2>/dev/null || docker compose version))"
else
    echo "FAIL - docker compose not available"
    exit 1
fi

# Python
echo -n "Python3 : "
if command -v python3 &>/dev/null; then
    echo "OK ($(python3 --version 2>&1))"
else
    echo "FAIL"
    exit 1
fi

# .env permissions
echo -n ".env permissions : "
if [ -f .env ]; then
    p=$(stat -c '%a' .env 2>/dev/null || stat -f '%A' .env 2>/dev/null)
    if [ "$p" = "600" ] || [ "$p" = "600" ]; then
        echo "OK (600)"
    else
        echo "WARN ($p) - expected 600"
    fi
else
    echo "FAIL - .env not found"
    exit 1
fi

# Ports
echo ""
echo "=== Port Check ==="
for port in 5432 6379 8000 8080 8082 5000 3001; do
    echo -n "  Port $port : "
    if (echo >/dev/tcp/127.0.0.1/$port) 2>/dev/null; then
        echo "OCCUPÉ"
    else
        echo "libre"
    fi
done

echo ""
echo "=== Docker Compose Config ==="
docker compose config --quiet 2>/dev/null && echo "docker-compose.yml : valide" || echo "docker-compose.yml : INVALIDE"

echo ""
echo "=== Existing Containers ==="
docker compose ps --format 'table {{.Name}}\t{{.State}}\t{{.Status}}' 2>/dev/null || echo "Aucun conteneur"

echo ""
echo "=== Environment Variables (masked) ==="
grep -E '^[A-Z]+\w*=' .env | grep -v -i 'password\|secret\|key\|token' | head -20 || true

echo ""
echo "=== Doctor Summary ==="
echo "Environment: $(grep '^TRENDX_ENV=' .env | cut -d= -f2)"
echo "Writeback: $(grep '^TB_WRITEBACK_ENABLED=' .env | cut -d= -f2)"
echo "Alarms: $(grep '^TB_ALARMS_ENABLED=' .env | cut -d= -f2)"
echo "Anomaly: $(grep '^ANOMALY_DETECTION_ENABLED=' .env | cut -d= -f2)"
echo "Confirm Apply: $(grep '^TRENDX_CONFIRM_APPLY=' .env | cut -d= -f2)"
