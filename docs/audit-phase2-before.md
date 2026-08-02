# Audit Phase 2 — État initial

**Date :** 2026-08-02  
**Opérateur :** Kilo  
**Séquence :** Étape 1 — snapshot avant modifications

---

## 1. État disque (avant)

```
Sys. de fichiers Taille Utilisé Dispo Uti% Monté sur
/dev/sdb2          1,1T    668G  376G  65% /
```

**Espace libre :** 376 GB  
**Seuil minimal (TRENDX_DISK_MIN_FREE_GB) :** 50 GB  
**Marge :** 326 GB

**Montages concernés :**
- `/` (/dev/sdb2) : 376 GB libres
- `/var/lib/docker` (inclus dans /dev/sdb2) : inclus ci-dessus
- `/opt` (inclus dans /dev/sdb2) : inclus ci-dessus

---

## 2. Conteneurs Docker (avant)

| Conteneur | Image | État | Créé le |
|---|---|---|---|
| thingsboard_thingsboard-ce_1 | thingsboard/tb-node:4.3.1.1-mobilis | running | 2026-06-15 16:27:16 |
| mobili_dahsboard-postgres-1 | postgres:16 | running | 2026-06-01 12:05:40 |
| thingsboard_kafka_1 | confluentinc/cp-kafka:7.5.0 | running | 2026-06-15 16:27:17 |
| thingsboard_zookeeper_1 | confluentinc/cp-zookeeper:7.5.0 | running | 2026-06-15 16:27:16 |
| thingsboard_tb-adminer_1 | tb-adminer-custom | running | 2026-06-15 16:27:16 |

**Conteneurs Trendx :** aucun (0)

---

## 3. Base de données ThingsBoard (avant)

| Base | Propriétaire | Tables |
|---|---|---|
| thingsboard | postgres | Tables ThingsBoard CE |
| trendx | postgres | 58 tables Trendz (déjà créées) |
| trendx_airflow | trendx_airflow | (existe) |
| trendx_analytics | trendx_migration | (existe) |
| trendx_mlflow | trendx_mlflow | (existe) |

**Rôles PostgreSQL :**
- postgres (superuser)
- trendx_airflow
- trendx_app
- trendx_migration
- trendx_mlflow
- trendx_ro : **N'EXISTE PAS ENCORE**

**max_connections :** 100  
**Connexions actives :** 22

---

## 4. Réseau Docker (avant)

| Réseau | État |
|---|---|
| mobili_dahsboard_default | existe |
| trendx_internal | n'existe pas encore |
| trendx_edge | n'existe pas encore |

---

## 5. Compose utilisé

**Fichier :** /opt/trendx/docker-compose.trendx.yml  
**Projet :** trendx

---

## 6. Points d'attention

1. La base `trendx` existe déjà avec 58 tables (schéma Trendz déjà appliqué)
2. Les bases `trendx_airflow`, `trendx_analytics`, `trendx_mlflow` existent (hors périmètre Phase 2)
3. Le rôle `trendx_ro` n'existe pas encore (à créer étape 6)
4. Les conteneurs Trendx ne tournent pas encore
5. Espace disque suffisant (376 GB libres)

---

## 7. Vérifications doctor (avant)

```
│  .env permissions (600 attendu) : OK
│  Compose file = docker-compose.trendx.yml ? : OK
│  Réseau mobili_dahsboard_default existe ? : OK
│  Port publié (seul 127.0.0.1:8443 autorisé) ? : OK
│  trendx_ro ne peut pas INSERT dans thingsboard ? : OK (INSERT refusé)
│  Ressources reverse-proxy/api/worker : OK
│  Espace libre / >= 50 GB ? : OK (376 GB)
│  Espace libre /var/lib/docker >= 50 GB ? : OK (376 GB)
```

**Tous les contrôles sont verts.**

---

*Fin du snapshot initial.*
