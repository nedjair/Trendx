# Budget disque Trendx

## Principe

Toute écriture Trendx (ingestion, agrégats, modèles, sauvegardes) consomme de
l'espace sur l'hôte unique `10.0.0.1`. Ce document consigne la consommation de
référence mesurée et l'estimation d'ingestion dérivée du flux réel ThingsBoard,
afin de distinguer le coût métier du bruit système.

## Sonde disque (application)

La sonde disque côté applicatif (`check_disk_min_free`) lit la variable
`TRENDX_DISK_MONITOR_MOUNTS` (liste séparée par des virgules).

Point de montage surveillé par défaut : `/opt/trendx/data` (volume Docker
`trendx_data`, bindé depuis l'hôte).

### Vérifications au démarrage (worker)

Pour chaque point de montage déclaré, le worker loggue au démarrage :
- chemin
- `st_dev`
- taille TOTALE du système de fichiers
- espace libre
- type de système de fichiers (`fstype` lu depuis `/proc/mounts`)

Si le type est `tmpfs`, le worker échoue au démarrage :
```
[disk] FAIL : montage /opt/trendx/data est un tmpfs (dev=..., fstype=tmpfs, total=... GB).
Utiliser un disque persistant. Vérifier df -h /dev/sdb2 sur l'hôte.
```

Si la taille totale mesurée est < 10 GB, un avertissement est émis pour
détecter un montage manifestement trop petit pour un disque hôte.

### Vérification d'identité avec le disque hôte

Depuis l'hôte :
```bash
df -h /dev/sdb2
```

Depuis le conteneur Trendx (worker au démarrage) :
```
[disk] mount /opt/trendx/data = XXX GB libres / YYY GB total (dev=ZZZ, fstype=ext4)
```

Les deux commandes doivent pointer vers le même périphérique (même `st_dev` /
même périphérique sous-jacent). Si `fstype=tmpfs`, la sonde échoue.

## Consommation de référence (mesurée 2026-08-03)

### Disque hôte

| Période | Observation | Source |
|---|---|---|
| T0 (2026-08-03 11:45) | `/dev/sdb2` 1.1 TB total, 693 GB utilisés, 350 GB libres (67%) | `df -h /dev/sdb2` depuis l'hôte |

### PostgreSQL (conteneur `mobili_dahsboard-postgres-1`, montage `/dev/sdb2`)

| Répertoire | Taille | Rôle |
|---|---|---|
| `/var/lib/postgresql/data/pg_wal` | 81 MB | WAL actif (recyclé en continu) |
| `/var/lib/postgresql/data/base` | 33 GB | Données ThingsBoard + Trendx |
| `/var/lib/postgresql/data` (total) | 33 GB | |

Base ThingsBoard : **26 GB**  
Base Trendx : **16 MB** (catalogue + analytics, phase initiale)

Flux WAL permanent ThingsBoard : **~81 MB on-disk** (recyclé, pas de croissance
linéaire). Seul chiffre permanent à long terme.

### Docker (hôte)

| Type | Total | Reclaimable | Nature |
|---|---|---|---|
| Images | 43.43 GB | 22.36 GB | Volumes cumulés (historique des rebuilds) |
| Build cache | 26.75 GB | 4.27 GB | Volumes cumulés |
| Conteneurs | 477.5 MB | 422.9 MB | |
| Volumes | 53.66 GB | 3.98 GB | |

**Note** : les 43 GB d'images et 26 GB de build cache sont des volumes
cumulés, pas une consommation horodatée. Ils résultent de l'historique des
rebuilds d'images et ne se reproduiront pas à ce rythme en régime stabilisé.

## Budget ingestion (dérivé de mesures réelles)

### Mesure brute ThingsBoard `ts_kv` (7 derniers jours)

| Métrique | Valeur mesurée |
|---|---|
| Lignes totales `ts_kv` | 177 920 470 |
| Taille totale `ts_kv` (avec index BRIN) | ~27.4 GB |
| Octets par ligne (partition août 2026) | 159.7 octets/ligne |
| Lignes 7 derniers jours | 23 370 394 |
| Lignes 1 jour | 2 506 325 |
| Devices actifs | 13 |

### Formules de dérivation

```
octets_par_ligne = pg_total_relation_size(partition) / nombre_lignes_partition
lignes_par_device_par_jour = lignes_1_jour / nb_devices
go_par_device_par_jour = (lignes_par_device_par_jour * octets_par_ligne) / 1024^3
go_par_jour_100_devices = go_par_device_par_jour * 100
```

### Tableau de sensibilité (basé sur les mesures ci-dessus)

| Devices | Lignes / jour | Octets / ligne | GB / jour |
|---|---|---|---|
| 3 | 578 270 | 160 | **0,09** |
| 13 (actuel) | 2 506 325 | 160 | **0,40** |
| 50 | 9 625 134 | 160 | **1,54** |
| 100 | 19 250 269 | 160 | **3,08** |

**Estimation réaliste pour 100 devices : ~3 GB / jour** (hors agrégats,
modèles, et index secondaires).

### Marge de sécurité

- Marge appliquée : 2× la référence mesurée
- `TRENDX_DISK_MIN_FREE_GB` (seuil d'arrêt automatique ingestion) : 50 GB

### Seuil de bascule rétention

| Paramètre | Valeur |
|---|---|
| Marge disponible | 300 GB |
| Seuil de bascule (50% marge) | **150 GB** |
| Débit estimé (100 devices) | ~2,87 GB/jour |
| Jours avant seuil (100 devices) | **~52 jours** |

| Devices | GB/jour | Jours avant seuil |
|---|---|---|
| 10 | 0,29 | 523 |
| 25 | 0,72 | 209 |
| 50 | 1,43 | 105 |
| 75 | 2,15 | 70 |
| 100 | 2,87 | 52 |

**Règle :** Tant que la projection de consommation agrégée reste sous 150 GB, la rétention brute 180 jours est conservée. Dès que la projection dépasse 150 GB, bascule automatique vers l'option 2 : purge du brut à J+30, conservation des agrégats horaires 180 jours, journaliers 2 ans, hebdomadaires 5 ans.

Cette bascule est déclenchée par l'opérateur après constatation du franchissement du seuil, sans attendre la date limite.

## Nettoyage périodique

### Images Docker intermédiaires

**Périmètre Trendx uniquement** : les commandes ci-dessous ciblent les images
et calques portant le label `com.trendx.project=trendx`.

```bash
# Supprimer les images Trendx non utilisées de plus de 72 h
docker image prune -f --filter "until=72h" --filter "label=com.trendx.project=trendx"

# Supprimer les calques de construction Trendx de plus de 72 h
docker builder prune -f --filter "until=72h" --filter "label=com.trendx.project=trendx"

# Nettoyage complet (conteneurs arrêtés, réseaux non utilisés, volumes dangling)
docker system prune -f --filter "until=72h" --filter "label=com.trendx.project=trendx"
```

**Aucun cron automatique.** L'opérateur lance `make docker-cleanup` manuellement
selon son propre planning.

### Journaux Docker

Limiter la taille des journaux dans `docker-compose.trendx.yml` :
```yaml
logging:
  driver: json-file
  options:
    max-size: "10m"
    max-file: "3"
```

### WAL PostgreSQL

Le WAL est géré par PostgreSQL (checkpoint, archive). Si `archive_mode = on`,
vérifier que l'archive fonctionne et que les WAL sont recyclés.

## Vérification avant ingestion

```bash
# Espace libre sur le point de montage surveillé
df -h /opt/trendx/data

# Vérification d'identité depuis l'hôte
df -h /dev/sdb2

# Consommation Docker
docker system df

# Taille de la base trendx
docker exec mobili_dahsboard-postgres-1 psql -U postgres -d trendx -c \
  "SELECT pg_size_pretty(pg_database_size('trendx'));"

# Mesure temps réel ts_kv ThingsBoard (7 jours)
docker exec mobili_dahsboard-postgres-1 psql -U postgres -d thingsboard -c "
  SELECT
    count(*) FILTER (WHERE ts >= EXTRACT(EPOCH FROM NOW() - INTERVAL '7 days')::bigint * 1000) AS rows_7d,
    count(*) FILTER (WHERE ts >= EXTRACT(EPOCH FROM NOW() - INTERVAL '1 day')::bigint * 1000) AS rows_1d,
    count(DISTINCT entity_id) FILTER (WHERE ts >= EXTRACT(EPOCH FROM NOW() - INTERVAL '7 days')::bigint * 1000) AS devices_7d
  FROM ts_kv;
"

# Octets par ligne sur la partition la plus récente
docker exec mobili_dahsboard-postgres-1 psql -U postgres -d thingsboard -c "
  SELECT
    pg_total_relation_size('ts_kv_2026_08') / NULLIF((SELECT count(*) FROM ts_kv_2026_08), 0) AS bytes_per_row
  ;
"
```

## Contrainte de rétention (3,1 GB/jour à 100 devices)

### Calcul de seuil

| Paramètre | Valeur |
|---|---|
| Débit brut estimé (100 devices) | ~3,1 GB/jour |
| Rétention brute configurée | 180 jours (`TRENDX_RETENTION_DAYS=180`) |
| Consommation 180 jours à 100 devices | **~558 GB** |
| Marge disponible (hors TB) | ~300 GB |
| **Écart** | **~258 GB au-dessus du seuil** |

### Date d'atteinte du seuil par palier

| Devices | GB/jour | Jours avant 300 GB | Date limite (à partir du 2026-08-03) |
|---|---|---|---|
| 10 | 0,31 | 967 | ~2029-04-18 |
| 25 | 0,78 | 385 | ~2027-08-13 |
| 50 | 1,54 | 195 | ~2027-02-14 |
| 75 | 2,31 | 130 | ~2026-12-11 |
| 100 | 3,08 | 97 | ~2026-11-09 |

### Options chiffrées

**Option 1 — Rétention brute réduite**
- Ajuster `TRENDX_RETENTION_DAYS` à 97 jours pour 100 devices
- Coût : perte de 83 jours de données brutes
- Bénéfice : pas de modification du pipeline d'agrégation
- Marge restante : 0 GB (à la limite)

**Option 2 — Agrégation précoce + purge du brut**
- Conserver le brut 30 jours, puis purge automatique
- Agrégats horaires conservés 180 jours, journaliers 2 ans, hebdomadaires 5 ans
- Coût : complexité supplémentaire (job de purge + dépendance à l'agrégation)
- Bénéfice : données brutes disponibles pour re-entraînement court terme
- Marge restante : ~210 GB pour 100 devices

**Recommandation :** Option 2 à partir de 50 devices, avec purge brut à J+30 et conservation agrégats 180j. Avant 50 devices, l'Option 1 suffit.

## Alerte

Si l'ingestion consomme plus de **2× la référence** (actuellement ~0,8 GB / jour
pour 13 devices) sans augmentation du volume de données métier, déclencher un
audit : compression, purge des tables temporaires, vérification des logs
d'ingestion.
