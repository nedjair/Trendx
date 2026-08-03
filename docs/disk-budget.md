# Budget disque Trendx

## Principe

Toute écriture Trendx (ingestion, agrégats, modèles, sauvegardes) consomme de
l'espace sur l'hôte unique `10.0.0.1`. Ce document consigne la consommation de
référence (bruit de fond) et le budget alloué à l'ingestion, afin de distinguer
le coût métier du bruit système.

## Points de montage surveillés

| Point de montage | Rôle | Résolution |
|---|---|---|
| `/var/lib/docker/volumes/tb-postgres-data/_data` | Données PostgreSQL (ThingsBoard + Trendx) | `docker volume inspect tb-postgres-data --format '{{.Mountpoint}}'` |
| `/var/lib/docker` | Docker Root Dir (volumes, images, calques, conteneurs) | `docker info --format '{{.DockerRootDir}}'` |

La garde disque côté applicatif (`check_disk_min_free`) lit la variable
`TRENDX_DISK_MONITOR_MOUNTS` (liste séparée par des virgules). Le doctor résout
dynamiquement les deux chemins ci-dessus et les affiche.

## Consommation de référence (2026-08-03)

| Période | Observation | Cause probable |
|---|---|---|
| 2026-08-02 15:00 → 2026-08-03 10:00 (~19 h) | 361 GB → 357 GB = **4 GB consommés** sans ingestion | Reconstruction d'images Docker, journaux de conteneurs, WAL PostgreSQL, couches de calques |

Cette valeur de **4 GB / jour** est la consommation de référence hors ingestion.

## Budget ingestion

| Poste | Budget |
|---|---|
| Bruit de fond (référence) | 4 GB / jour |
| Ingestion 72 h, 3 devices, métriques principales | ~0,5 GB / jour (estimé) |
| Marge de sécurité | 2× la référence |
| `TRENDX_DISK_MIN_FREE_GB` (seuil d'arrêt) | 50 GB |

## Nettoyage périodique

### Images Docker intermédiaires

```bash
# Supprimer les images dangling (non taguées, non utilisées)
docker image prune -f

# Supprimer les images non utilisées (tous les calques morts)
docker image prune -a -f

# Nettoyage complet (conteneurs arrêtés, réseaux non utilisés, volumes dangling)
docker system prune -f
```

Planification recommandée : cron quotidien sur l'hôte.

```cron
0 3 * * * root /usr/bin/docker image prune -a -f --filter "until=72h" >> /var/log/docker-cleanup.log 2>&1
```

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
# Espace libre sur les points de montage surveillés
df -BG /var/lib/docker/volumes/tb-postgres-data/_data /var/lib/docker

# Consommation Docker
docker system df

# Taille de la base trendx
docker exec mobili_dahsboard-postgres-1 psql -U postgres -d trendx -c \
  "SELECT pg_size_pretty(pg_database_size('trendx'));"
```

## Alerte

Si l'ingestion consomme plus de **2× la référence** (8 GB / jour) sans
augmentation du volume de données, déclencher un audit : compression, purge des
tables temporaires, vérification des logs d'ingestion.
