# Plan de migration Trendx — Serveur actuel vers 10.0.0.1

**Objectif** : Migrer l'intégralité du projet Trendx d'un serveur saturé vers le nouveau serveur cible `10.0.0.1` (co-localisé avec ThingsBoard) en minimisant l'interruption de service.

**Références** : `docker-compose.yml`, `AGENTS.md`, `docs/rapport_phase2_infrastructure.md`

---

## État des lieux pré-migration

| Élément | Valeur observée |
|---|---|
| IP serveur actuel (Trendx) | `10.0.0.1` (`.env TRENDX_HOST`) |
| IP cible | `10.0.0.1` (co-localisé ThingsBoard `:8081`) |
| Stack Docker | 10 services (timescaledb, redis, api, worker, ui, airflow-init, airflow-webserver, airflow-scheduler, mlflow, grafana) |
| Bases PostgreSQL | `trendx` (~9 MB), `trendx_analytics` (~9 MB), `trendx_airflow` (~11 MB), `trendx_mlflow` (~9 MB) |
| Volumes nommés | `trendx_timescaledb_data`, `trendx_redis_data`, `trendx_mlflow_artifacts`, `trendx_grafana_data`, `trendx_airflow_logs` |
| Docker images | ~35 GB (24 GB reclaimable) |
| Espace disque source | 98 GB total, 74 GB utilisés, 20 GB libres (79%) |
| Secrets | `.env` (chmod 600), `.secrets/service-accounts.env` (chmod 600) |
| Ports publiés | 8000 (API), 8080 (UI), 8082 (Airflow), 5000 (MLflow), 3001 (Grafana), 5432 (TSDB localhost), 6379 (Redis localhost) |

---

## Décisions préalables à valider

### D-1 — Accès au serveur cible `10.0.0.1`
**Recommandation** : Utiliser SSH avec authentification par clé (`ssh mobilis@10.0.0.1`). Si le port 22 n'est pas joignable depuis le serveur source, utiliser un bastion ou un transfert hors-bande (clé USB, scp via rebond). Confirmer l'utilisateur (`root` ou `mobilis` avec sudo).

### D-2 — Politique de ports sur `10.0.0.1`
**Recommandation** : Maintenir les mêmes ports publiés (`8000`, `8080`, `8082`, `5000`, `3001`) car ils sont libres d'après l'audit Phase 2. Vérifier avant coup que `10.0.0.1` n'a pas de service sur ces ports. Si Gitea est sur `3000` (confirmé dans `.env`), `3001` reste disponible pour Grafana.

### D-3 — Docker sur le serveur cible
**Recommandation** : Installer Docker Engine 24+ et Docker Compose v2 sur `10.0.0.1` avant transfert. Vérifier avec `docker info` et `docker compose version`.

### D-4 — Gestion de l'interruption de service (cutover)
**Recommandation** : Accepter une fenêtre d'interruption planifiée de 30-60 minutes pendant la bascule finale (Phase 4). Toutes les phases amont (préparation, transfert, validation) se font à chaud sur le serveur source.

### D-5 — Conservation des données ThingsBoard
**Recommandation** : Aucune donnée ThingsBoard n'est migrée (read-only canal 2 non activé en Phase 2). Le writeback ThingsBoard (`TB_WRITEBACK_ENABLED=false`) est désactivé. Aucun risque de corruption TB.

---

## Plan d'exécution détaillé

### Phase 0 — Préparation (à chaud sur le serveur source)

**0.1** Installer les dépendances système sur le serveur source si absentes : `ssh`, `rsync`, `pg_dump` (client PostgreSQL), `gzip`.

**0.2** Vérifier l'intégrité de la stack source :
```bash
cd /home/mobilis/Trendx
make doctor
make health
docker compose ps
```

**0.3** Exécuter une sauvegarde complète **avant toute manipulation** :
```bash
make backup
# → génère backups/YYYYMMDD/{trendx,trendx_analytics,trendx_airflow,trendx_mlflow}.dump.gz
```

**0.4** Vérifier la taille des volumes Docker :
```bash
docker system df
docker volume ls --filter "name=trendx"
```

**0.5** Générer un manifeste des images Docker utilisées :
```bash
docker compose -f docker-compose.yml images --format "{{.Repository}}:{{.Tag}} {{.ID}}" > /tmp/trendx-image-manifest.txt
```

**0.6** Geler les écritures applicatives (si production) :
- Mettre `TRENDX_CONFIRM_APPLY=NO` (déjà par défaut)
- Arrêter temporairement les DAG Airflow via l'UI (`/home/mobilis/Trendx` → `make down` arrête toute la stack, donc préférer pause des DAGs via UI Airflow si la stack doit rester up)

**0.7** Exporter la liste des volumes avec leurs tailles :
```bash
docker volume ls --filter "name=trendx" --format "{{.Name}} {{.Driver}}"
```

**Résultat attendu** : Sauvegarde locale vérifiée, stack source saine, manifeste prêt.

---

### Phase 1 — Préparation du serveur cible `10.0.0.1`

**1.1** Vérifier l'accès SSH :
```bash
ssh mobilis@10.0.0.1 "hostname && df -h / && free -h && docker --version && docker compose version"
```
Si échec, résoudre l'accès réseau/clé SSH avant de continuer.

**1.2** Vérifier l'espace disque sur la cible :
- Besoin minimal estimé : 50 GB (images Docker 35 GB + volumes 1 GB + OS + marge TB).
- Si `< 60 GB libres`, nettoyer d'abord (`docker system prune -a --volumes` sur la cible, **avec confirmation explicite**).

**1.3** Créer l'arborescence du projet sur la cible :
```bash
ssh mobilis@10.0.0.1 "mkdir -p /opt/trendx && chown mobilis:mobilis /opt/trendx"
```

**1.4** Transférer le code source (hors secrets et volumes) :
```bash
rsync -avz --delete \
  --exclude='.env' \
  --exclude='.secrets/' \
  --exclude='backups/' \
  --exclude='.venv/' \
  --exclude='__pycache__/' \
  --exclude='*.pyc' \
  /home/mobilis/Trendx/ mobilis@10.0.0.1:/opt/trendx/
```

**1.5** Transférer les secrets **de manière sécurisée** (hors bande rsync) :
```bash
# Option A : scp avec droits restrictifs
scp /home/mobilis/Trendx/.env mobilis@10.0.0.1:/opt/trendx/.env
scp -r /home/mobilis/Trendx/.secrets/ mobilis@10.0.0.1:/opt/trendx/.secrets/
ssh mobilis@10.0.0.1 "chmod 600 /opt/trendx/.env /opt/trendx/.secrets/*.env /opt/trendx/.secrets/*.token 2>/dev/null; true"
```
Vérifier sur la cible : `stat -c '%a' /opt/trendx/.env` → doit retourner `600`.

**1.6** Adapter `.env` pour le nouveau contexte :
- `TRENDX_HOST=10.0.0.1`
- `PG_ADMIN_HOST=timescaledb` (interne docker-compose)
- Vérifier que tous les ports publiés sont libres sur `10.0.0.1`.

**1.7** Installer les dépendances Docker sur la cible (si nécessaire) :
```bash
ssh mobilis@10.0.0.1 "curl -fsSL https://get.docker.com | sh && sudo systemctl enable --now docker"
```

**Résultat attendu** : Code et secrets présents sur `10.0.0.1`, Docker opérationnel, `.env` adapté.

---

### Phase 2 — Transfert des données

**2.1** Sauvegarder les bases de données source (dumps compressés) :
```bash
cd /home/mobilis/Trendx
docker compose exec -T timescaledb pg_dump -U postgres --format=custom --no-owner --clean trendx | gzip -9 > /tmp/trendx.dump.gz
docker compose exec -T timescaledb pg_dump -U postgres --format=custom --no-owner --clean trendx_analytics | gzip -9 > /tmp/trendx_analytics.dump.gz
docker compose exec -T timescaledb pg_dump -U postgres --format=custom --no-owner --clean trendx_airflow | gzip -9 > /tmp/trendx_airflow.dump.gz
docker compose exec -T timescaledb pg_dump -U postgres --format=custom --no-owner --clean trendx_mlflow | gzip -9 > /tmp/trendx_mlflow.dump.gz
```

**2.2** Transférer les dumps vers la cible :
```bash
scp /tmp/trendx_*.dump.gz mobilis@10.0.0.1:/tmp/
```

**2.3** Transférer les images Docker (option 1 : export/import, recommandé si réseau rapide) :
```bash
docker save -o /tmp/trendx-images.tar \
  $(docker compose -f docker-compose.yml images --format "{{.Repository}}:{{.Tag}}" | grep -v '^$')
scp /tmp/trendx-images.tar mobilis@10.0.0.1:/tmp/
```
**Alternative (option 2) :** Rebuild sur la cible si la bande passante est limitée et les sources sont à jour :
```bash
ssh mobilis@10.0.0.1 "cd /opt/trendx && docker compose build --parallel"
```

**2.4** Transférer les volumes Docker (si nécessaire) :
- Pour `trendx_timescaledb_data` : préférer la restauration par dump SQL (2.1-2.2) plutôt que copie brute de volume (plus propre, versionné).
- Pour `trendx_grafana_data`, `trendx_airflow_logs`, `trendx_mlflow_artifacts`, `trendx_redis_data` :
```bash
# Sur la source (arrêt de la stack nécessaire pour cohérence)
docker compose down
docker run --rm -v trendx_grafana_data:/data -v /tmp/backup/grafana:/backup alpine tar czf /backup/grafana_data.tgz -C /data .
docker run --rm -v trendx_airflow_logs:/data -v /tmp/backup/airflow:/backup alpine tar czf /backup/airflow_logs.tgz -C /data .
# ... répéter pour chaque volume
```
Puis transférer les `.tgz` vers la cible et extraire dans des volumes nommés identiques.

**⚠ Sécurité** : Les volumes contiennent des données sensibles (logs, configs). Ne pas exposer les archives. Supprimer les `.tgz` après extraction vérifiée.

**Résultat attendu** : Dumps SQL sur la cible, images Docker prêtes, volumes transférés.

---

### Phase 3 — Configuration et validation sur la cible

**3.1** Démarrer l'infrastructure seule (TimescaleDB + Redis) :
```bash
ssh mobilis@10.0.0.1 "cd /opt/trendx && docker compose up -d --no-deps timescaledb redis"
```

**3.2** Attendre que TimescaleDB soit healthy :
```bash
ssh mobilis@10.0.0.1 "cd /opt/trendx && docker compose wait timescaledb --timeout 180 || { for i in \$(seq 1 36); do docker compose exec -T timescaledb pg_isready -h 127.0.0.1 -q && break; sleep 5; done; }"
```

**3.3** Restaurer les dumps SQL **dans l'ordre** :
```bash
ssh mobilis@10.0.0.1 "cd /opt/trendx && \
  gunzip -c /tmp/trendx.dump.gz | docker compose exec -T timescaledb pg_restore -U postgres --dbname=trendx --no-owner --clean && \
  gunzip -c /tmp/trendx_analytics.dump.gz | docker compose exec -T timescaledb pg_restore -U postgres --dbname=trendx_analytics --no-owner --clean && \
  gunzip -c /tmp/trendx_airflow.dump.gz | docker compose exec -T timescaledb pg_restore -U postgres --dbname=trendx_airflow --no-owner --clean && \
  gunzip -c /tmp/trendx_mlflow.dump.gz | docker compose exec -T timescaledb pg_restore -U postgres --dbname=trendx_mlflow --no-owner --clean"
```

**3.4** Réappliquer les migrations SQL idempotentes (sécurité) :
```bash
ssh mobilis@10.0.0.1 "cd /opt/trendx && make migrate-catalog migrate-analytics"
```
Cela garantit que les schémas sont à jour même si les dumps sont anciens.

**3.5** Vérifier la taille et l'intégrité des bases restaurées :
```bash
ssh mobilis@10.0.0.1 "docker compose exec -T timescaledb psql -U postgres -c \"SELECT datname, pg_size_pretty(pg_database_size(datname)) FROM pg_database WHERE datname LIKE 'trendx_%' ORDER BY datname;\""
```

**3.6** Démarrer les services core et vérifier les health checks :
```bash
ssh mobilis@10.0.0.1 "cd /opt/trendx && make up-core"
```
Puis :
```bash
ssh mobilis@10.0.0.1 "cd /opt/trendx && sleep 15 && make health"
```

**3.7** Vérifier la connectivité entre services :
```bash
# API health
curl -fsS http://10.0.0.1:8000/health
# UI
curl -fsS http://10.0.0.1:8080/
# Airflow
curl -fsS http://10.0.0.1:8082/health
# MLflow
curl -fsS http://10.0.0.1:5000/
# Grafana
curl -fsS http://10.0.0.1:3001/api/health
# Redis
ssh mobilis@10.0.0.1 "docker compose exec -T redis redis-cli ping"
```

**3.8** Vérifier les bases de données depuis la cible :
```bash
ssh mobilis@10.0.0.1 "docker compose exec -T timescaledb psql -U trendx_app -d trendx_analytics -c 'SELECT count(*) FROM ts_kv;'"
```

**Résultat attendu** : Stack complète opérationnelle sur `10.0.0.1`, toutes les bases restaurées, health checks OK.

---

### Phase 4 — Cutover final (basculer la production)

**4.1** Planifier la fenêtre de coupure (ex. : créneau de maintenance de 30 min).

**4.2** Arrêter la stack source :
```bash
# Sur le serveur source
cd /home/mobilis/Trendx
docker compose down
```

**4.3** Synchroniser les données delta éventuelles (si la coupure n'a pas été totale) :
- Si la stack source a été stoppée en Phase 0, aucun delta.
- Sinon, capturer le dernier checkpoint/watermark et rejouer l'ingestion depuis la cible via Airflow backfill.

**4.4** Mettre à jour la configuration réseau :
- Si un reverse-proxy, DNS ou load-balancer pointe vers l'ancienne IP, le rediriger vers `10.0.0.1`.
- Vérifier que les URLs de service (API, UI, Airflow, MLflow, Grafana) sont accessibles depuis les clients.

**4.5** Démarrer la stack cible en mode production :
```bash
ssh mobilis@10.0.0.1 "cd /opt/trendx && docker compose up -d"
```

**4.6** Valider l'accès depuis l'extérieur :
```bash
curl -fsS http://10.0.0.1:8000/health
curl -fsS http://10.0.0.1:8080/
curl -fsS http://10.0.0.1:8082/health
```

**4.7** Démarrer les DAGs Airflow préalablement mis en pause :
- Vérifier dans l'UI Airflow que les DAGs `trendx_*` sont unpaused et scheduled.

**Résultat attendu** : Production basculée sur `10.0.0.1`, services accessibles, données cohérentes.

---

### Phase 5 — Post-migration et nettoyage

**5.1** Surveiller la stack cible pendant 2-4 heures :
```bash
ssh mobilis@10.0.0.1 "cd /opt/trendx && make logs && make health"
```

**5.2** Exécuter les tests de non-régression :
```bash
ssh mobilis@10.0.0.1 "cd /opt/trendx && make test-unit"
# Si stack up et DB accessible :
ssh mobilis@10.0.0.1 "cd /opt/trendx && make test-integration"
```

**5.3** Vérifier les logs d'erreur spécifiques :
```bash
ssh mobilis@10.0.0.1 "docker compose logs --tail=200 api worker | grep -iE 'error|exception|traceback|fail' || echo 'Aucune erreur critique'"
```

**5.4** Nettoyer le serveur source (après validation de 24h de stabilité) :
```bash
# Sur le serveur source — action DESTRUCTIVE, nécessite TRENDX_CONFIRM_APPLY=YES
cd /home/mobilis/Trendx
docker compose down -v --remove-orphans
docker system prune -a --volumes -f
```
**Conserver** les sauvegardes dans `backups/` et le répertoire `/home/mobilis/Trendx` pendant 7 jours.

**5.5** Mettre à jour la documentation :
- `docs/rapport_phase2_infrastructure.md` : ajouter la nouvelle IP `10.0.0.1`.
- `README.md` : vérifier les URLs d'accès.

**5.6** Planifier le nettoyage des Docker images obsolètes sur la cible :
```bash
ssh mobilis@10.0.0.1 "docker image prune -a --filter 'until=720h'"
```

---

## Tests de validation obligatoires

| # | Test | Commande | Critère de succès |
|---|---|---|---|
| V-1 | Health checks tous services | `make health` sur cible | Tous `OK` |
| V-2 | Bases accessibles | `psql -U trendx_app -d trendx_analytics -c 'SELECT 1'` | Sortie `1` |
| V-3 | Taille des bases cohérente | `pg_database_size` | Comparable à la source |
| V-4 | API répond | `curl -fsS http://10.0.0.1:8000/health` | HTTP 200 |
| V-5 | UI répond | `curl -fsS http://10.0.0.1:8080/` | HTTP 200 |
| V-6 | Airflow répond | `curl -fsS http://10.0.0.1:8082/health` | HTTP 200 |
| V-7 | MLflow répond | `curl -fsS http://10.0.0.1:5000/` | HTTP 200 |
| V-8 | Grafana répond | `curl -fsS http://10.0.0.1:3001/api/health` | HTTP 200 |
| V-9 | Redis répond | `redis-cli ping` | `PONG` |
| V-10 | Secrets protégés | `stat -c '%a' .env .secrets/*` | `600` partout |
| V-11 | Aucun appel ThingsBoard non autorisé | `grep -r ':8081\|:8090' src/ docker-compose.yml | grep -v '^#' | grep -v 'TB_BASE_URL'` | Vide |
| V-12 | Tests unitaires passent | `make test-unit` | 0 échec |
| V-13 | Permissions fichiers | `ls -la /opt/trendx/.env /opt/trendx/.secrets/` | `-rw-------` |

---

## Points d'attention spécifiques

### Co-localisation avec ThingsBoard (`10.0.0.1`)
- **CPU/RAM** : ThingsBoard + Trendx sur la même machine. Vérifier que `10.0.0.1` a suffisamment de RAM (≥ 8 GB recommandé) et que les cgroups Docker ne sont pas en contention.
- **Ports** : `5432` est publié en `127.0.0.1:5432` uniquement (localhost). Si le serveur cible a déjà PostgreSQL sur `5432`, modifier le mapping ou désactiver l'exposition.
- **Disque** : Le répertoire `/var/lib/docker` et les volumes Docker sont sur le même filesystem que ThingsBoard. Surveiller l'usage après migration.

### Sécurité des secrets
- Ne jamais transférer `.env` ou `.secrets/` en clair par email/chat.
- Supprimer les `.tgz` de volumes après extraction.
- Vérifier `chmod 600` systématiquement sur la cible.

### Gestion des images Docker
- Si la bande passante source→cible est limitée, préférer le rebuild sur la cible (`make build-images`).
- Si les images sont volumineuses et la bande passante suffisante, utiliser `docker save/load`.
- Vérifier que les tags `latest` ne sont pas utilisés en production (conformité AGENTS §6.2).

### Rollback
En cas d'échec critique sur la cible :
1. Redémarrer la stack source si elle a été stoppée : `docker compose up -d`
2. Revenir à l'ancienne IP dans les DNS/reverse-proxy.
3. Conserver les dumps sources pendant 7 jours.

---

## Prochaine étape

Une fois ce plan validé, l'implémentation nécessite un agent capable d'exécuter des commandes SSH, `rsync`, `docker compose`, `pg_dump`/`pg_restore` et `make`.

**Décisions bloquantes à confirmer avec l'utilisateur** :
1. Accès SSH à `10.0.0.1` : utilisateur, méthode d'authentification, port SSH.
2. Taille disque disponible sur `10.0.0.1` (besoin ≥ 60 GB libres).
3. Acceptation d'une fenêtre de coupure de 30-60 min pour le cutover.
4. Gestion des Docker images : sauvegarde/transfert ou rebuild sur cible ?
