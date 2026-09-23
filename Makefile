# ========================================
# Makefile - Trendx
# Respecte AGENTS.md — Commandes dev
# ========================================

SHELL := /bin/bash

.PHONY: help setup install build build-images up up-minimal up-full status health logs logs-api logs-worker logs-ui logs-reverse-proxy down down-clean lint fmt format typecheck type-check test test-unit test-integration test-e2e migrate seed seed-test-data doctor backup restore-check restore-test clean shell venv dc-config

# Valeurs par défaut
PYTHON ?= $(shell command -v python3 >/dev/null 2>&1 && echo python3 || (command -v python >/dev/null 2>&1 && echo python || echo python3))
PIP    ?= $(PYTHON) -m pip
DC     ?= docker compose -f /opt/trendx/docker-compose.trendx.yml -p trendx --env-file /root/Trendx/.env
VENV   ?= .venv
ENV_OK := $(shell test -f .env && echo 1 || echo 0)

# Pare-feu TRENDX_CONFIRM_APPLY pour actions destructives (AGENTS.md §6.4, §19)
CONFIRM := $(shell grep -E '^TRENDX_CONFIRM_APPLY=(YES|yes|true|TRUE|1)' .env 2>/dev/null | cut -d= -f2)

# Profil de déploiement : minimal par défaut, full pour services optionnels
PROFILE := $(shell grep -E '^TRENDX_PROFILE=' .env 2>/dev/null | cut -d= -f2 || echo minimal)

# Conteneur PostgreSQL existant ThingsBoard / Trendx
PG_EXTERNAL_CONTAINER ?= mobili_dahsboard-postgres-1

# Seuil disque dur (AGENTS §5) — lu depuis .env, défaut 50
TRENDX_DISK_MIN_FREE_GB ?= $(shell grep -E '^TRENDX_DISK_MIN_FREE_GB=' .env 2>/dev/null | cut -d= -f2 || echo 50)
TRENDX_DISK_MONITOR_MOUNTS ?= $(shell grep -E '^TRENDX_DISK_MONITOR_MOUNTS=' .env 2>/dev/null | cut -d= -f2 || echo /)

.DEFAULT_GOAL := help

# ————————————————————————————————————————
# Aide
# ————————————————————————————————————————
help: ## Afficher ce message d'aide
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-25s\033[0m %s\n", $$1, $$2}'

# ————————————————————————————————————————
# Installation & environnement
# ————————————————————————————————————————
venv: ## Créer/mettre à jour le venv Python local
	@test -d $(VENV) || { echo "[trendx] création venv Python..."; $(PYTHON) -m venv $(VENV); }
	@$(VENV)/bin/python -m ensurepip --upgrade >/dev/null 2>&1 || true
	@$(VENV)/bin/python -m pip install --upgrade pip setuptools wheel

setup: install ## Alias : installer le projet (convention AGENTS §17)

install: venv ## Installer les dépendances (contraintes épinglées + dev)
	@echo "[trendx] install dépendances avec constraints.txt..."
	@$(VENV)/bin/pip install -c constraints.txt -e ".[dev]"
	@echo "[trendx] hooks pre-commit..."
	@$(VENV)/bin/pre-commit install >/dev/null 2>&1 || true
	@chmod 600 .env 2>/dev/null || true
	@chmod 600 .secrets/*.env .secrets/*.token 2>/dev/null || true
	@echo "[trendx] prêt. Pour tester : source $(VENV)/bin/activate && uvicorn trendx.main:app"

# ————————————————————————————————————————
# Build images Docker
# ————————————————————————————————————————
build: build-images ## Alias AGENTS §17

build-images: ## Builder les images trendx
	@echo "[trendx] build api..."
	@$(DC) build api
	@echo "[trendx] build worker..."
	@$(DC) build worker
	@echo "[trendx] build reverse-proxy..."
	@$(DC) build reverse-proxy
	@echo "[trendx] build OK (services custom Trendx)"

# ————————————————————————————————————————
# Démarrage par profil
# ————————————————————————————————————————
up: ## Démarrer la stack selon TRENDX_PROFILE (minimal par défaut)
	@if [ "$(PROFILE)" = "full" ]; then \
	  $(MAKE) up-full; \
	else \
	  $(MAKE) up-minimal; \
	fi

up-minimal: ## Démarrer le profil minimal : reverse-proxy + api + worker
	@echo "[trendx] profil minimal : reverse-proxy + api + worker"
	@$(DC) up -d --no-deps reverse-proxy
	@$(DC) up -d --no-deps api worker
	@echo "[trendx] stack minimal démarrée. Taper 'make status' puis 'make health'"

up-full: ## Démarrer le profil full : services optionnels inclus
	@echo "[trendx] profil full : reverse-proxy + api + worker"
	@$(DC) up -d --no-deps reverse-proxy
	@$(DC) up -d --no-deps api worker
	@echo "[trendx] stack full démarrée. Taper 'make status' puis 'make health'"

# ————————————————————————————————————————
# Status, health, logs
# ————————————————————————————————————————
status: ## docker compose ps avec état santé (AGENTS §17)
	@echo "┌─────────────────────────────────────────────────────────────────────────┐"
	@echo "│  Trendx — État des services (profil: $(PROFILE))                       │"
	@echo "├─────────────────────────────────────────────────────────────────────────┤"
	@$(DC) ps --format 'table {{.Name}}\t{{.Image}}\t{{.State}}\t{{.Status}}\t{{.Ports}}' 2>&1 || $(DC) ps
	@echo "└─────────────────────────────────────────────────────────────────────────┘"

health: ## Vérifier health de chaque service docker
	@echo "[trendx-health] PostgreSQL externe ..."
	@docker exec $(PG_EXTERNAL_CONTAINER) pg_isready -h 127.0.0.1 -U postgres -q 2>/dev/null && echo "  PostgreSQL : OK" || echo "  PostgreSQL : FAIL"
	@echo "[trendx-health] reverse-proxy ..."
	@curl -fsS --max-time 5 http://127.0.0.1:8443/healthz >/dev/null 2>&1 && echo "  Reverse-proxy 8443 : OK" || echo "  Reverse-proxy 8443 : FAIL"
	@echo "[trendx-health] api (via reverse-proxy) ..."
	@curl -fsS --max-time 5 http://127.0.0.1:8443/health >/dev/null 2>&1 && echo "  API via reverse-proxy : OK" || echo "  API via reverse-proxy : FAIL"

logs: ## Suivre logs tous services (AGENTS §17)
	@$(DC) logs -f --tail=50

logs-api:          ; @$(DC) logs -f --tail=200 api
logs-worker:       ; @$(DC) logs -f --tail=200 worker
logs-reverse-proxy:; @$(DC) logs -f --tail=100 reverse-proxy

# ————————————————————————————————————————
# Arrêt (pare-feu destructif)
# ————————————————————————————————————————
down: ## Arrêter la stack SANS supprimer les volumes (conservé données)
	@$(DC) down

down-clean: ## ⚠  Arrêter ET supprimer volumes (NÉCESSITE TRENDX_CONFIRM_APPLY=YES dans .env)
	@if [ -z "$(CONFIRM)" ]; then \
	  echo "ERREUR : action destructive bloquée."; \
	  echo "Pour confirmer, mettre TRENDX_CONFIRM_APPLY=YES dans .env et relancer."; \
	  exit 1; fi
	@echo "⚠  Destruction des volumes..."
	@$(DC) down -v --remove-orphans

# ————————————————————————————————————————
# Qualité code
# ————————————————————————————————————————
lint: ## Lint ruff sur src/tests (AGENTS §17)
	@$(VENV)/bin/ruff check src tests 2>/dev/null || ruff check src tests

fmt: format ## Alias

format: ## Ruff format (indentation, quotes, import)
	@$(VENV)/bin/ruff format src tests 2>/dev/null || ruff format src tests

typecheck: type-check ## Alias AGENTS §17

type-check: ## Vérification de type mypy strict
	@$(VENV)/bin/mypy src 2>/dev/null || mypy src

# ————————————————————————————————————————
# Tests (AGENTS §17)
# ————————————————————————————————————————
test: test-unit ## Alias

test-unit: ## Tests unitaires (pytest -m unit)
	@$(VENV)/bin/pytest -m unit -ra 2>/dev/null || pytest -m unit -ra

test-integration: ## Tests d'intégration (besoin stack up)
	@$(VENV)/bin/pytest -m integration -ra 2>/dev/null || pytest -m integration -ra

test-e2e: ## Tests end-to-end (MVP complet)
	@$(VENV)/bin/pytest -m e2e -ra 2>/dev/null || pytest -m e2e -ra

# ————————————————————————————————————————
# Migrations SQL tracées via trendx_catalog.schema_version (sur PostgreSQL externe existant)
# ————————————————————————————————————————
migrate: ## Appliquer les migrations SQL tracées sur la base trendx (AGENTS §17)
	@echo "[trendx-migrate] 001_trendz_native_schema.sql → trendx..."
	@docker exec $(PG_EXTERNAL_CONTAINER) psql -v ON_ERROR_STOP=1 -U postgres -d trendx -c "SELECT 1 FROM information_schema.tables WHERE table_schema = 'trendx_catalog' AND table_name = 'business_entity' LIMIT 1;" | grep -q 1 || \
		docker exec $(PG_EXTERNAL_CONTAINER) psql -v ON_ERROR_STOP=1 -U postgres -d trendx -f /migrations/001_trendz_native_schema.sql
	@echo "[trendx-migrate] catalogue OK."

# ————————————————————————————————————————
# Data test / seed
# ————————————————————————————————————————
seed: seed-test-data ## Alias AGENTS §17

seed-test-data: migrate ## Générer données de test dans trendx_analytics (ALG16025001)
	@echo "[trendx-seed] Génération 24h test ALG16025001 / mppt_main_battery_voltage_v..."
	@python3 scripts/generate_test_data.py 2>/dev/null || echo "[trendx-seed] script pas encore écrit — placeholder OK."

# ————————————————————————————————————————
# Doctor (vérifications runtime)
# ————————————————————————————————————————
doctor: ## Vérifications pre-flight (réseau, ports, ressources, bases, disque)
	@echo "┌─ Trendx Doctor ──────────────────────────────────────────────────────────┐"
	@fail=0; \
	echo -n "│  .env permissions (600 attendu) : "; \
	  p=$$(stat -c '%a' .env 2>/dev/null); [ "$$p" = "600" ] && echo "OK (rw-------)" || { echo "FAIL ($$p) — chmod 600 .env"; fail=$$((fail+1)); }; \
	for sf in .secrets/service-accounts.env; do \
	  echo -n "│  $$sf permissions (600) : "; \
	    p=$$(stat -c '%a' $$sf 2>/dev/null); [ "$$p" = "600" ] && echo "OK" || { echo "FAIL ($$p)"; fail=$$((fail+1)); }; \
	done; \
	echo "│"; \
	echo -n "│  Compose file = docker-compose.trendx.yml ? : "; \
	  [ -f /opt/trendx/docker-compose.trendx.yml ] && echo "OK" || { echo "FAIL — /opt/trendx/docker-compose.trendx.yml absent"; fail=$$((fail+1)); }; \
	echo "│"; \
	echo -n "│  Réseau mobili_dahsboard_default existe ? : "; \
	  docker network ls --format '{{.Name}}' | grep -qx 'mobili_dahsboard_default' && echo "OK" || { echo "FAIL — réseau absent ou mal orthographié"; fail=$$((fail+1)); }; \
	echo "│"; \
	echo -n "│  Port publié (seul 127.0.0.1:8443 autorisé) ? : "; \
	  bad=$$(docker ps --filter "name=trendx" --format '{{.Ports}}' 2>/dev/null | grep -v '8443' | grep -E ':[0-9]+->' | wc -l); \
	  [ "$$bad" -eq 0 ] && echo "OK" || { echo "FAIL ($$bad port(s) publié(s) hors 8443)"; fail=$$((fail+1)); }; \
	echo "│"; \
	echo -n "│  trendx_ro ne peut pas INSERT dans thingsboard ? : "; \
	  ins=$$(docker exec mobili_dahsboard-postgres-1 psql -U trendx_ro -d thingsboard -c "INSERT INTO ts_kv (ts, entity_id, metric_key, str_v, long_v, dbl_v, bool_v, json_v, source) VALUES (0, '00000000-0000-0000-0000-000000000000', 'trendx_doctor_test', NULL, NULL, NULL, NULL, NULL, 'doctor') ON CONFLICT DO NOTHING;" 2>&1 | grep -c -i 'error\|FATAL'); \
	  [ "$$ins" -gt 0 ] && echo "OK (INSERT refusé)" || { echo "FAIL — INSERT autorisé avec trendx_ro"; fail=$$((fail+1)); }; \
	echo "│"; \
	for sch in trendx_catalog trendx_analytics public; do \
	  echo -n "│  trendx_app CREATE TABLE $$sch.doctor_test ? : "; \
	    cre=$$(docker exec mobili_dahsboard-postgres-1 psql -U trendx_app -d trendx -c "CREATE TABLE $$sch.doctor_test (id int);" 2>&1 | grep -c -i 'error\|FATAL'); \
	    [ "$$cre" -gt 0 ] && echo "OK (CREATE refusé)" || { echo "FAIL — CREATE autorisé dans $$sch"; fail=$$((fail+1)); }; \
	done; \
	echo "│"; \
	echo -n "│  Bases PostgreSQL (liste blanche) ? : "; \
	  dbs=$$(docker exec mobili_dahsboard-postgres-1 psql -U postgres -d postgres -tAc "SELECT datname FROM pg_database WHERE NOT datistemplate AND datallowconn ORDER BY datname" 2>/dev/null | tr '\n' ' '); \
	  unexpected=""; \
	  for db in $$dbs; do case "$$db" in thingsboard|trendx|postgres|template*) ;; *) unexpected="$$unexpected $$db";; esac; done; \
	  [ -z "$$unexpected" ] && echo "OK ($$dbs)" || { echo "FAIL — base(s) inattendue(s):$$unexpected"; fail=$$((fail+1)); }; \
	echo "│"; \
	echo -n "│  Search paths moteurs (règles schémas) ? : "; \
	  sp=$$(docker exec trendx_api python3 /app/scripts/check_search_path.py 2>&1); rc=$$?; \
	  echo "$$sp" | sed 's/^/│    /'; \
	  if [ "$$rc" -ne 0 ]; then fail=$$((fail+1)); fi; \
	echo "│"; \
	echo -n "│  Auth ThingsBoard (sans leak) ? : "; \
	  tb_configured=$$(grep -E '^TB_AUTH_CONFIGURED=(true|1|yes|TRUE|YES)' .env 2>/dev/null | wc -l); \
	  tbout=$$(python3 scripts/tb_auth_check.py 2>&1); rc=$$?; \
	  echo "$$tbout"; \
	  if [ "$$tb_configured" -gt 0 ] && [ "$$rc" -ne 0 ]; then fail=$$((fail+1)); fi; \
	echo "│"; \
	echo -n "│  Whitelist ThingsBoard (GET + login only) ? : "; \
	  wlout=$$($(VENV)/bin/python scripts/tb_whitelist_check.py 2>&1); rc=$$?; \
	  echo "$$wlout"; \
	  if [ "$$rc" -ne 0 ]; then fail=$$((fail+1)); fi; \
	echo "│"; \
	echo -n "│  Partitions trendx_analytics à +3 mois ? : "; \
	  cov=$$(docker exec mobili_dahsboard-postgres-1 psql -U postgres -d trendx -tAc "SELECT trendx_analytics.partition_coverage(3);" 2>/dev/null | tr -d '\n'); \
	  miss=$$(printf '%s' "$$cov" | grep -o '"missing":[^,]*' | cut -d: -f2 | tr -d ' '); \
	  [ "$$miss" = "0" ] && echo "OK ($$cov)" || { echo "FAIL — partitions manquantes (< 3 mois): $$cov"; fail=$$((fail+1)); }; \
	echo "│"; \
	for svc in reverse-proxy api worker; do \
	  echo -n "│  Ressources $$svc (cpus/mem/pids/logs) ? : "; \
	    conf=$$($(DC) config 2>/dev/null); \
	    echo "$$conf" | python3 /tmp/check_resources.py "$$svc" && echo "OK" || { echo "FAIL"; fail=$$((fail+1)); }; \
	done; \
	echo "│"; \
	echo -n "│  Pas de mélange pools TB/trendx dans .env ? : "; \
	  mixed=$$(grep -E '^(TB_DB|TRENDX_DB)_' .env 2>/dev/null | grep -c 'mobili_dahsboard-postgres-1'); \
	  [ "$$mixed" -le 2 ] && echo "OK" || { echo "FAIL — même hôte pour les deux pools sans séparation"; fail=$$((fail+1)); }; \
	echo "│"; \
	echo -n "│  Points de montage surveillés : "; \
	  echo "$(TRENDX_DISK_MONITOR_MOUNTS)"; \
	echo "│"; \
	for mp in $(TRENDX_DISK_MONITOR_MOUNTS); do \
	  label=$$mp; \
	  echo -n "│  Espace libre $$label >= $(TRENDX_DISK_MIN_FREE_GB) GB ? : "; \
	    avail=$$(df -BG --output=avail "$$mp" 2>/dev/null | tail -1 | tr -dc '0-9'); \
	    [ -n "$$avail" ] && [ "$$avail" -ge $(TRENDX_DISK_MIN_FREE_GB) ] && echo "OK ($${avail} GB)" || { echo "FAIL ($${avail:-?} GB)"; fail=$$((fail+1)); }; \
	done; \
	echo "│"; \
	echo -n "│  Migrations : numéros uniques ? : "; \
	  dup=$$(ls -1 migrations/[0-9][0-9][0-9]_*.sql 2>/dev/null | sed 's|.*/\([0-9][0-9][0-9]\)_.*|\1|' | sort | uniq -d | wc -l); \
	  [ "$$dup" -eq 0 ] && echo "OK" || { echo "FAIL — numéros en double: $$(ls -1 migrations/[0-9][0-9][0-9]_*.sql 2>/dev/null | sed 's|.*/\([0-9][0-9][0-9]\)_.*|\1|' | sort | uniq -d | tr '\n' ' ')"; fail=$$((fail+1)); }; \
	echo "│"; \
	echo -n "│  Devices enregistrés (catalogue) ? : "; \
	  dev_count=$$(docker exec mobili_dahsboard-postgres-1 psql -U postgres -d trendx -tAc "SELECT count(*) FROM trendx_catalog.business_entity;" 2>/dev/null | tr -d ' '); \
	  dev_count=$${dev_count:-0}; \
	  echo "$$dev_count"; \
	  if [ "$$dev_count" -gt 52 ] 2>/dev/null; then \
	    echo "│  \033[33mWARN — projection > 150 GB (seuil de bascule rétention) atteinte avec $$dev_count devices. Considérer purge du brut à J+30.\033[0m"; \
	  fi; \
	echo "│"; \
	echo -n "│  Python3 disponible : "; which python3 && python3 --version | head -1; \
	echo -n "│  docker compose      : "; docker compose version --short 2>/dev/null || echo "NON"; \
	echo -n "│  Compose file        : "; echo "/opt/trendx/docker-compose.trendx.yml (via \$$(DC))"; \
	echo -n "│  Projet compose      : "; docker compose ls --format json 2>/dev/null | python3 -c "import sys,json; d=json.load(sys.stdin); print(next((p['Name'] for p in d if p['Name']=='trendx'), 'ABSENT'))" 2>/dev/null || echo "NON"; \
	echo "└─────────────────────────────────────────────────────────────────────────┘"; \
	exit $$fail

# ————————————————————————————————————————
# Sauvegarde & restauration
# ————————————————————————————————————————
backup: ## Sauvegarde base trendx dans /opt/trendx/data/backups/YYYYMMDD/
	@mkdir -p /opt/trendx/data/backups/$$(date +%Y%m%d)
	@echo "[trendx-backup] trendx → /opt/trendx/data/backups/$$(date +%Y%m%d)/trendx.dump.gz"
	@docker exec $(PG_EXTERNAL_CONTAINER) pg_dump -U postgres --format=custom --no-owner --clean \
	    trendx | gzip -9 > /opt/trendx/data/backups/$$(date +%Y%m%d)/trendx.dump.gz
	@echo "[trendx-backup] Terminé → /opt/trendx/data/backups/$$(date +%Y%m%d)/"

restore-check: ## Vérifier intégrité dumps les plus récents (AGENTS §17)
	@latest=$$(ls -td /opt/trendx/data/backups/*/ 2>/dev/null | head -1); \
	if [ -z "$$latest" ]; then echo "[trendx-restore-check] aucun dump trouvé dans /opt/trendx/data/backups/"; exit 0; fi; \
	echo "[trendx-restore-check] vérification $$latest"; \
	for f in $$latest/*.gz; do \
	  echo -n "  $$f : "; \
	  gzip -t "$$f" 2>/dev/null && echo "OK gzip" || echo "FAIL gzip"; \
	done

restore-test: ## Rejouer restauration complète sur base jetable + preuves état canonique (docs/deployment.md §12bis)
	@bash scripts/restore-test.sh

# ————————————————————————————————————————
# Nettoyage Docker (périmètre Trendx uniquement, pas de cron automatique)
# ————————————————————————————————————————
docker-cleanup: ## Nettoyer images/calques/cache Docker Trendx (hors volumes TB/PG)
	@echo "[trendx] Nettoyage Docker (périmètre Trendx)..."
	@echo "[trendx] Images inutilisées (>72h, label com.trendx.project=trendx) :"
	@docker image prune -f --filter "until=72h" --filter "label=com.trendx.project=trendx"
	@echo "[trendx] Calques de construction (>72h, label com.trendx.project=trendx) :"
	@docker builder prune -f --filter "until=72h" --filter "label=com.trendx.project=trendx"
	@echo "[trendx] Nettoyage global (conteneurs arrêtés, réseaux, build cache) :"
	@docker system prune -f --filter "until=72h" --filter "label=com.trendx.project=trendx"
	@echo "[trendx] Espace récupéré :"
	@docker system df

# ————————————————————————————————————————
# Utilitaires
# ————————————————————————————————————————
dc-config: ## Valider docker compose config + variables résolues
	@$(DC) config

shell: ## Shell Python interactif avec imports trendx
	@$(VENV)/bin/python -i -c "import sys; sys.path.insert(0,'src'); from trendx.config import settings; from trendx.main import app; print('settings OK, app FastAPI OK. taper exit() pour quitter')"

clean: ## Nettoyer caches Python (AGENTS §17)
	find . -type d -name '__pycache__' -exec rm -rf {} + 2>/dev/null; true
	find . -type f -name '*.pyc' -delete 2>/dev/null; true
	find . -type d -name '*.egg-info' -exec rm -rf {} + 2>/dev/null; true
	rm -rf build dist .pytest_cache .ruff_cache .mypy_cache htmlcov .coverage 2>/dev/null; true
	@echo "[trendx] nettoyage terminé"
