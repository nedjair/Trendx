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

| Conteneur | Image | État | Créé le (UTC) | StartedAt (UTC) | RestartCount |
|---|---|---|---|---|---|
| thingsboard_thingsboard-ce_1 | thingsboard/tb-node:4.3.1.1-mobilis | running | 2026-06-15 15:27:16 | 2026-08-02 07:47:09 | 1 |
| mobili_dahsboard-postgres-1 | postgres:16 | running | 2026-06-01 11:05:40 | 2026-08-02 07:43:09 | 0 |
| thingsboard_kafka_1 | confluentinc/cp-kafka:7.5.0 | running | 2026-06-15 15:27:17 | 2026-08-02 07:43:09 | 0 |
| thingsboard_zookeeper_1 | confluentinc/cp-zookeeper:7.5.0 | running | 2026-06-15 15:27:16 | 2026-08-02 07:43:09 | 0 |
| thingsboard_tb-adminer_1 | tb-adminer-custom | running | 2026-06-15 15:27:16 | 2026-08-02 07:43:09 | 0 |

**Conteneurs Trendx :** aucun (0)

> **Note (correctif PREUVE) :** le snapshot initial n'avait capturé que la colonne
> `Created`. Les colonnes `StartedAt` et `RestartCount` sont désormais enregistrées
> pour rendre la non-régression démontrable (AGENTS). Les valeurs ci-dessus sont
> les valeurs de référence après l'incident du matin (voir §7bis et
> `audit-phase2-after.md` §10).

---

## 2bis. Incident infrastructure du matin 2026-08-02 (PREUVE redémarrage ThingsBoard)

**Événement :** redémarrage de l'hôte et du démon Docker, AVANT toute présence Trendx.

```text
$ uptime
 15:43:43 up  7:01,  3 users,  load average: 2,40, 2,18, 1,77

$ last reboot
 reboot   system boot  7.0.0-28-generic Sun Aug  2 08:42   still running
 reboot   system boot  7.0.0-28-generic Wed Jul 29 13:08 - 20:40 (1+07:31)

$ journalctl -u docker --since "2026-08-02 07:00" --until "2026-08-02 09:00" --no-pager
 août 02 08:42:47 iotserver systemd[1]: Starting docker.service - Docker Application Container Engine...
 août 02 08:43:05 iotserver dockerd: Restoring containers: start.
 août 02 08:43:11 iotserver dockerd: Loading containers: done.
```

**Chronologie (UTC) :**
| Instant (UTC) | Événement |
|---|---|
| 07:42:47 | Boot hôte (08:42:47 local, +01:00) — `systemd[1]: Starting docker.service` |
| 07:43:05–07:43:11 | dockerd restaure les conteneurs existants |
| 07:43:09 | PG, Kafka, Zookeeper, adminer redémarrés (`StartedAt`) |
| 07:47:09 | `thingsboard_thingsboard-ce_1` redémarré (`StartedAt`, `RestartCount=1`) |
| 13:51:38 | Premier conteneur Trendx créé (API/worker/reverse-proxy) |

**Conclusion :** le `RestartCount=1` et le `StartedAt` du 07:47:09Z du conteneur
ThingsBoard s'expliquent intégralement par le redémarrage de l'hôte à 07:42 UTC,
soit plus de 6 heures avant la création des conteneurs Trendx (13:51 UTC).
Aucun lien avec Trendx. Événement à signaler à l'exploitant (ops@mobilis.dz).

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
