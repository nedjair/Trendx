# Budget disque détaillé — Trendx sur 10.0.0.1

**Version :** 1.2 — Phase 2 (post-context réel)  
**Date :** 2026-08-02  
**Hôte :** iotserver (10.0.0.1)  
**Système de fichiers :** /dev/sdb2 — 1,1 TB ext4  
**Décisions applicables :** B5 (pas de TimescaleDB), B2 (pas d'exposition 5433), B6 (pas de ports ouverts), Correction B (partage volume PostgreSQL TB)

---

## 1. État actuel (vérifié 2026-08-02)

**Constat :** `/root`, `/opt` et `/var/lib/docker` sont sur le même montage `/dev/sdb2`.  
Il n'y a donc **qu'un seul système de fichiers** à surveiller pour l'espace libre.

| Chemin | Montage | Taille | Utilisé | Libre | % utilisé |
|---|---|---|---|---|---|
| `/root/Trendx` (code Git) | /dev/sdb2 | 1,1 TB | 668 GB | 376 GB | 65 % |
| `/opt/trendx` (données runtime) | /dev/sdb2 | (inclus ci-dessus) | — | — | — |
| `/var/lib/docker` | /dev/sdb2 | (inclus ci-dessus) | ~90 GB | (inclus) | — |

**Seuil minimal (TRENDX_DISK_MIN_FREE_GB) :** 50 GB  
**Espace restant après seuil :** 326 GB

---

## 2. Budget par montage

### 2.1 Montage unique : /dev/sdb2

Toutes les écritures Trendx (code, données, volumes, sauvegardes, images, logs) se font sur `/dev/sdb2`.

| Composant | Emplacement | Estimation basse | Estimation haute | Justification |
|---|---|---|---|---|
| Code Git | `/root/Trendx` | < 1 GB | < 2 GB |源代码、配置、文档 |
| Images Docker Trendx | `/var/lib/docker` | 2 GB | 4 GB | reverse-proxy + api + worker (sans ML, cible < 1 Go par image) |
| Volumes Trendx | `/opt/trendx/data` | 2 GB | 8 GB | Données analytiques, logs applicatifs |
| Sauvegardes Trendx | `/opt/trendx/data/backups` | 0 GB | 10 GB | pg_dump -Fc -d trendx, rotation |
| Schéma `trendx_catalog` | base trendx (dans volume PG partagé) | 500 MB | 1 GB | Métadonnées, modèles, tables Trendz reproduites |
| Schéma `trendx_analytics` | base trendx (dans volume PG partagé) | 0 GB | 8 GB | Partitionné déclaratif + BRIN, 180j × devices MVP |
| **TOTAL** | | **4,5 GB** | **31 GB** | — |

**Note :** Les images et volumes Trendx s'ajoutent à l'existant Docker (~90 GB déjà utilisé sur /dev/sdb2).

---

## 3. Partage du volume PostgreSQL ThingsBoard

**Constat :** Les bases Trendx (`trendx_catalog`, `trendx_analytics`) partagent le volume PostgreSQL existant de ThingsBoard (`tb-postgres-data`), monté sur `/var/lib/docker/volumes/tb-postgres-data/_data` (/dev/sdb2).

**Conséquences acceptées pour Phase 2 :**
- Performance : risque de contention si ThingsBoard est très chargé
- Espace : ts_kv TB (~25–30 GB) + `trendx_analytics.ts_kv` (estimé 2–8 GB) sur même volume
- Sauvegarde : la sauvegarde de `trendx` inclut le volume PostgreSQL partagé
- Pas de séparation physique possible sans migration PostgreSQL complète (reportée)

**Protections obligatoires :**
- Vérification espace libre du montage `/dev/sdb2` AVANT toute migration
- Arrêt automatique ingestion sous `TRENDX_DISK_MIN_FREE_GB` (pas seulement alerte)
- Alerte si < 100 GB
- Purge automatique partitions `trendx_analytics` au-delà de `TRENDX_RETENTION_DAYS`

---

## 4. Budget Trendx (profil minimal)

**Contrainte :** Aucune ingestion historique complète. Fenêtres + checkpoints + rétention + purge + arrêt sous TRENDX_DISK_MIN_FREE_GB.

| Composant | Estimation basse | Estimation haute | Justification |
|---|---|---|---|
| Images Docker Trendx | 2 GB | 4 GB | reverse-proxy + api + worker (sans ML, cible < 1 Go par image) |
| Volume `trendx_data` | 2 GB | 8 GB | Données analytiques, logs applicatifs |
| Sauvegardes | 0 GB | 10 GB | pg_dump -Fc -d trendx uniquement, rotation |
| Schéma `trendx_catalog` | 500 MB | 1 GB | Métadonnées, modèles, tables Trendz reproduites |
| Schéma `trendx_analytics` | 0 GB | 8 GB | Partitionné déclaratif + BRIN + agrégats matérialisés, 180j × devices MVP |
| **TOTAL** | **4,5 GB** | **31 GB** | — |

**Note :** Pas de base `trendx_airflow`, `trendx_mlflow`, `trendx_grafana` en Phase 2.  
**Note 2 :** Les images et volumes Trendx s'ajoutent à l'existant Docker (~90 GB déjà utilisé sur /dev/sdb2).

---

## 5. Budget Trendx (profil full)

Ajoute au profil minimal :

| Composant | Estimation |
|---|---|
| Redis | 500 MB – 2 GB |
| Airflow (DAGs + logs) | 2 – 5 GB |
| MLflow (artefacts complets) | 5 – 15 GB |
| Grafana (dashboards + métriques) | 1 – 3 GB |
| **TOTAL full** | **14 – 40 GB** |

---

## 6. Projection à 12 mois

| Hypothèse | Impact |
|---|---|
| Croissance télémétrie TB : +5 GB/mois | 60 GB/an |
| Croissance Trendx analytics : +2 GB/mois | 24 GB/an |
| Rétention Trendx brut : 180 jours | ~12 GB max analytics |
| Purge automatique partitions + agrégats matérialisés | économie 30–50 % |

**Projection 12 mois :**
- TB : +60 GB
- Trendx analytics : +24 GB
- Total ajouté : ~84 GB
- Libre dans 12 mois : 376 GB - 84 GB - 50 GB (seuil) = **242 GB**

**Verdict :** ✅ Espace suffisant pour 12 mois en profil minimal. Surveillance mensuelle recommandée.

---

## 7. Alertes et seuils

| Seuil | Action |
|---|---|
| Espace libre < 200 GB | Alerte info |
| Espace libre < 100 GB | Alerte warning |
| Espace libre < 50 GB | **STOP ingestion Trendx** + purge données test |
| Espace libre < 20 GB | Intervention manuelle requise |

**Monitoring :**
- Cron quotidien `df -h` + alerte mail/slack si < 100 GB
- Grafana dashboard espace disque (si Grafana activé)
- Purge automatique `trendx_analytics` au-delà de `TRENDX_RETENTION_DAYS=180`
- Arrêt automatique ingestion si `TRENDX_DISK_MIN_FREE_GB=50` atteint

**Surveillance par montage :** Un seul montage `/dev/sdb2` concerne tous les chemins Trendx. Le contrôle d'espace libre porte sur ce montage unique.

---

## 8. Optimisations recommandées (sans TimescaleDB)

**B5 — Pas d'extension TimescaleDB. Utiliser :**

1. **Partitionnement déclaratif PostgreSQL** : partition `ts_kv` par mois sur `ts` (RANGE)
2. **Index BRIN** sur `ts` pour chaque partition : `CREATE INDEX ... USING brin (ts)`
3. **Agrégats matérialisés** : UPSERT incrémental par plage recalculée, jamais REFRESH MATERIALIZED VIEW
4. **Rétention** : 180 jours brut, 2 ans horaire, 5 ans jour/semaine, 1 an prédictions, 90 jours journaux, purge par DROP PARTITION
5. **Logs Docker** : `max-size: 10m`, `max-file: 3` sur chaque service
6. **Images** : utiliser des tags versionnés + digest SHA256 en production
7. **Build cache** : `docker system prune -f` mensuel (hors volumes)

---

## 9. Conclusion

| Profil | Besoin | Espace requis | Espace libre | Marge |
|---|---|---|---|---|
| Minimal | 4,5 – 31 GB | 31 GB max | 376 GB | 345 GB |
| Full | 14 – 40 GB | 40 GB max | 376 GB | 336 GB |
| 12 mois (minimal) | +84 GB | 115 GB | 376 GB | 261 GB |

**Recommandation :** Profil minimal suffisant. Espace disque non bloquant. Aucune ingestion historique complète avant validation opérateur. Partage volume PostgreSQL accepté avec protections automatiques.
