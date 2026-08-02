#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

echo "=== Vérification Docker ==="
docker info --format '{{.ServerVersion}}' 2>/dev/null || { echo "Docker indisponible"; exit 1; }
echo "Docker OK"

echo ""
echo "=== Build images Docker ==="
docker compose build --parallel 2>&1 | tail -5

echo ""
echo "=== Démarrage infrastructure ==="
docker compose up -d timescaledb redis
echo "Attente TimescaleDB..."
for i in $(seq 1 30); do
    if docker compose exec -T timescaledb pg_isready -h 127.0.0.1 &>/dev/null; then
        echo "TimescaleDB prêt (tentative $i)"
        break
    fi
    echo -n "."
    sleep 2
done
echo ""

echo ""
echo "=== Démarrage services ==="
docker compose up -d mlflow grafana api worker airflow-init airflow-webserver airflow-scheduler ui 2>&1 || true
echo "Attente stabilisation..."
sleep 10

echo ""
echo "=== Health checks ==="
docker compose ps --format 'table {{.Name}}\t{{.State}}\t{{.Status}}'

echo ""
echo "=== URLs ==="
echo "API      : http://127.0.0.1:8000/docs"
echo "UI       : http://127.0.0.1:8080"
echo "Airflow  : http://127.0.0.1:8082"
echo "MLflow   : http://127.0.0.1:5000"
echo "Grafana  : http://127.0.0.1:3001"
echo ""
echo "Pour voir les logs : docker compose logs -f"
echo "Pour arrêter : docker compose down"
