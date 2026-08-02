# Plan d'abandon — Phase 2 Trendx

**Version :** 1.0  
**Date :** 2026-08-02  
**Portée :** Arrêt propre de la stack Trendx Phase 2, retour à l'état initial.

---

## 1. Principe

Ce runbook décrit la procédure d'abandon complet de Trendx Phase 2 **sans toucher à ThingsBoard**.  
Aucune commande ne s'applique aux conteneurs ou volumes ThingsBoard/PostgreSQL existants.

**Seuil d'activation :** arrêt manuel ou échec bloquant non récupérable.

---

## 2. Pré-requis

- Accès SSH root@10.0.0.1
- Accès au dépôt /root/Trendx
- Accès au conteneur PostgreSQL ThingsBoard (lecture seule trendx_ro suffit)
- Fichier Compose : /opt/trendx/docker-compose.trendx.yml (ou /root/Trendx/docker-compose.trendx.yml)

---

## 3. Séquence d'abandon

### Étape 1 — Arrêt de la stack Trendx

```bash
cd /root/Trendx
docker compose -f /opt/trendx/docker-compose.trendx.yml -p trendx down
```

**Vérification :**
```bash
docker ps --filter "name=trendx" --format '{{.Names}}\t{{.Status}}'
# Doit retourner vide
```

### Étape 2 — Nettoyage des volumes Trendx

```bash
# Lister les volumes Trendx
docker volume ls --filter "name=trendx" --format '{{.Name}}'

# Supprimer UNIQUEMENT les volumes Trendx
docker volume rm trendx_data 2>/dev/null || true
# (ajouter d'autres volumes Trendx si créés ultérieurement)
```

### Étape 3 — Suppression de la base trendx et des rôles

```bash
# Se connecter au conteneur PostgreSQL ThingsBoard
docker exec -it mobili_dahsboard-postgres-1 psql -U postgres -c "
  SELECT pg_terminate_backend(pid)
  FROM pg_stat_activity
  WHERE datname = 'trendx'
    AND pid <> pg_backend_pid();
"

docker exec -it mobili_dahsboard-postgres-1 psql -U postgres -c "DROP DATABASE IF EXISTS trendx;"
docker exec -it mobili_dahsboard-postgres-1 psql -U postgres -c "DROP ROLE IF EXISTS trendx_app;"
docker exec -it mobili_dahsboard-postgres-1 psql -U postgres -c "DROP ROLE IF EXISTS trendx_ro;"
docker exec -it mobili_dahsboard-postgres-1 psql -U postgres -c "DROP ROLE IF EXISTS trendx_migration;"
```

### Étape 4 — Suppression des réseaux Trendx

```bash
# Supprimer UNIQUEMENT les réseaux Trendx (jamais mobili_dahsboard_default)
docker network rm trendx_internal 2>/dev/null || true
docker network rm trendx_edge 2>/dev/null || true
```

### Étape 5 — Suppression des images Trendx

```bash
docker image rm trendx/reverse-proxy:latest 2>/dev/null || true
docker image rm trendx/api:latest 2>/dev/null || true
docker image rm trendx/worker:latest 2>/dev/null || true
```

### Étape 6 — Nettoyage du système Docker (hors volumes ThingsBoard)

```bash
# Supprimer les conteneurs arrêtés Trendx
docker container prune -f --filter "name=trendx"

# Supprimer les images dangling
docker image prune -f

# NE PAS toucher aux volumes ThingsBoard (tb-data, tb-postgres-data, etc.)
```

---

## 4. Vérification finale — ThingsBoard intact

### 4.1 Conteneurs ThingsBoard

```bash
docker ps --filter "name=thingsboard" --format '{{.Names}}\t{{.Status}}\t{{.CreatedAt}}'
```

**Critère de succès :** Les conteneurs ThingsBoard sont toujours running, avec un `CreatedAt` inchangé.

### 4.2 Base ThingsBoard

```bash
docker exec mobili_dahsboard-postgres-1 psql -U postgres -d thingsboard -c "\dt"
```

**Critère de succès :** Aucune table Trendx (trendx_catalog, trendx_analytics) dans la base thingsboard.

### 4.3 Rôles PostgreSQL

```bash
docker exec mobili_dahsboard-postgres-1 psql -U postgres -c "\du"
```

**Critère de succès :** Les rôles trendx_ro, trendx_app, trendx_migration n'existent plus.

### 4.4 Espace disque

```bash
df -h /dev/sdb2
```

**Critère de succès :** Espace libre >= état initial (376 GB).

---

## 5. Restauration du dépôt (si besoin)

```bash
cd /root/Trendx
git checkout -- .
git clean -fd
```

**Attention :** Cette commande supprime tous les fichiers non suivis (y compris les modifications de ce runbook).

---

## 6. Contacts et escalade

- Incident infrastructure : ops@mobilis.dz
- Base de données ThingsBoard : DBA interne
- Stack Trendx : équipe Trendx

---

## 7. Historique des exécutions

| Date | Résultat | Commentaire |
|---|---|---|
| 2026-08-02 | ⏳ Plan rédigé, non exécuté | — |
