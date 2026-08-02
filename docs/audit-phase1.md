# Rapport d'audit Phase 1 — Serveur 10.0.0.1

**Date :** 2026-07-30  
**Mode :** Lecture seule — aucune écriture de production, aucun conteneur créé, aucun objet créé dans la base ThingsBoard  
**Conformité :** AGENTS.md §1–22, prompt.md §15

---

## 1. État de l'hôte 10.0.0.1

| Caractéristique | Valeur observée |
|---|---|
| **Hostname** | iotserver |
| **OS** | Ubuntu 24.04.4 LTS (Noble Numbat) |
| **CPU** | 16 cœurs physiques |
| **RAM totale** | 62 GB — libre 1,7 GB — disponible 47 GB |
| **Swap** | 8 GB — utilisé 900 KB |
| **Disk total** | 1,1 TB ext4 — utilisé 669 GB — libre **374 GB (65 % utilisé)** |
| **Docker** | Engine 29.3.0 (build 5927d80) — Compose v5.1.0 |
| **Réseau hôte** | 10.0.0.1/24 (interface lo + bridge Docker) |

**Espace disponible pour Trendx :** 374 GB libre. Suffisant pour profil minimal.

---

## 2. Inventaire Docker

### 2.1 Conteneurs actifs

| Conteneur | Image | Statut | Ports publiés |
|---|---|---|---|
| thingsboard_thingsboard-ce_1 | thingsboard/tb-node:4.3.1.1-mobilis | Up 21 h | 0.0.0.0:8081→8080, 0.0.0.0:443→8080, 0.0.0.0:1883→1883, 0.0.0.0:5683-5688→5683-5688/udp, 0.0.0.0:8883→8883 |
| mobili_dahsboard-postgres-1 | postgres:16 | Up 21 h (healthy) | 127.0.0.1:5433→5432 |
| thingsboard_kafka_1 | confluentinc/cp-kafka:7.5.0 | Up 21 h | 9092/tcp |
| thingsboard_zookeeper_1 | confluentinc/cp-zookeeper:7.5.0 | Up 21 h | 2181, 2888, 3888 |
| thingsboard_tb-adminer_1 | tb-adminer-custom | Up 21 h | 127.0.0.1:8082→8080 |
| nodered-thingsboard | nodered/node-red:latest | Up 21 h (healthy) | 0.0.0.0:1880→1880 |
| stupefied_carver | thingsboard/mcp | Up 17 h | — |
| thingsboard-tb-mcp-server-1 | thingsboard/mcp:latest | Up 21 h | 127.0.0.1:8085→8000 |
| solar-topo-api | ai-mobilis_solar-topo-api:latest | Up 21 h (healthy) | 127.0.0.1:5002→5002 |
| battery-soh-api | battery_soh-battery-api | Up 21 h (unhealthy) | 127.0.0.1:5010→5010 |
| solar-meteo-api | open-meteo_solar-meteo-api:latest | Up 21 h (healthy) | 127.0.0.1:8004→8004 |
| mobili-ai-chatbot | ai-ai-chatbot | Up 21 h (healthy) | 127.0.0.1:8003→8003 |

### 2.2 Conteneurs arrêtés (historique TB)

| Conteneur | Image | Statut |
|---|---|---|
| thingsboard | thingsboard/tb-postgres | Exited (137) 8 semaines |
| thingsboard-mcp-edge | thingsboard/mcp | Exited (255) 8 semaines |
| temp-pg | postgres:16 | Exited (1) 3 mois |
| thingsboard-deployment-thingsboard-ce-1 | thingsboard/tb-node:4.3.0.1-mobilis | Exited (143) 2 mois |
| thingsboard-deployment-postgres-1 | postgres:16 | Exited (0) 2 mois |

### 2.3 Réseaux Docker

| Réseau | Driver | Scope |
|---|---|---|
| mobili_dahsboard_default | bridge | local |
| root_thingsboard-network | bridge | local |
| thingsboard-deployment_default | bridge | local |
| ai-mobilis_solar-network | bridge | local |
| ai_mobili-network | bridge | local |
| battery_soh_battery-network | bridge | local |
| open-meteo_meteo-network | bridge | local |
| bridge | bridge | local |
| host | host | local |
| none | null | local |

**Réseau ThingsBoard actuel :** `mobili_dahsboard_default`  
**Passerelle :** 172.21.0.1  
**Conteneur TB CE :** 172.21.0.5 (alias : thingsboard-ce)  
**Conteneur PostgreSQL :** 172.21.0.8 (alias : postgres)

### 2.4 Volumes Docker

| Volume | Taille |
|---|---|
| tb-postgres-data | Volume actif PostgreSQL ThingsBoard |
| tb-data | Volume ThingsBoard |
| tb-logs | Logs ThingsBoard |
| root_tb-data | Backup/ancien volume |
| tb-postgres-data-restored-20260514 | Backup restore |
| tb-postgres-data-v3 | Ancien volume |
| postgres_v4.2_data | Ancien volume |
| kafka_zookeeper_data / _logs | Kafka/ZooKeeper |
| node_red_data / nodered_data | Node-RED |
| backup | Backup général |

**Images Docker :** 18 images — 30,99 GB dont 19,53 GB récupérables (63 %)

---

## 3. Version ThingsBoard

| Élément | Valeur |
|---|---|
| **Image** | thingsboard/tb-node:4.3.1.1-mobilis |
| **Version** | 4.3.1.1 |
| **Branche** | release-4.3 |
| **Commit** | c2a52e4 |
| **Build** | 2026-03-30T17:48:53.381Z |
| **Édition** | Community Edition (CE) — confirmée par image et absence de tables PE-only |
| **Démarrage conteneur** | 2026-07-29T12:13:33Z |
| **API /actuator/info** | `{"git":{"branch":"release-4.3","commit":{"id":"c2a52e4","time":"2026-03-30T13:36:41Z"}},"build":{"artifact":"application","name":"ThingsBoard Server Application","time":"2026-03-30T17:48:53.381Z","version":"4.3.1.1","group":"org.thingsboard"}}` |

**Connectivité API ThingsBoard :**  
- `https://10.0.0.1:8081` répond (TLS auto-signé)  
- Auth JWT : identifiants existants dans `.env` (non divulgués ici)  
- Endpoints disponibles : `/api/auth/login`, `/api/tenant/devices`, `/api/tenant/assets`, `/api/plugins/telemetry/{id}/values/timeseries`

---

## 4. Connectivité réseau depuis conteneur externe

### 4.1 Par nom de conteneur (réseau Docker)

| Service | Nom conteneur | Adresse IP réseau | Port interne | Accessible |
|---|---|---|---|---|
| ThingsBoard CE | thingsboard_thingsboard-ce_1 | 172.21.0.5 | 8080 | ✅ |
| PostgreSQL TB | mobili_dahsboard-postgres-1 | 172.21.0.8 | 5432 | ✅ |

### 4.2 Ports hôte déjà pris

| Port | Service | Conflit Trendx ? |
|---|---|---|
| 8081 | ThingsBoard CE (TLS) | Oui — réservé TB |
| 443 | ThingsBoard CE (TLS alternatif) | Oui — réservé TB |
| 1883 | MQTT TB | Non |
| 5683–5688/udp | LwM2M TB | Non |
| 8883 | MQTT TLS TB | Non |
| 1880 | Node-RED | Non |
| 8082 | Adminer TB (127.0.0.1) | Non (localhost only) |
| 8085 | MCP Server (127.0.0.1) | Non (localhost only) |
| 5002 | solar-topo-api (127.0.0.1) | Non |
| 5010 | battery-soh-api (127.0.0.1) | Non |
| 8003 | mobili-ai-chatbot (127.0.0.1) | Non |
| 8004 | solar-meteo-api (127.0.0.1) | Non |
| 5433 | PostgreSQL TB (127.0.0.1) | **Oui si Trendx veut 5432 public** |

**Ports Trendx recommandés (non conflit) :**
- 8000 : API Trendx
- 8080 : UI Trendx
- 8082 : Airflow (libre sur hôte, adminer sur 127.0.0.1:8082)
- 5000 : MLflow
- 3001 : Grafana (3000 occupé par Gitea)
- 5432 : PostgreSQL Trendx (interne Docker seulement ; port hôte 5433 occupé par TB PG)

---

## 5. Découverte dynamique ThingsBoard

### 5.1 Entités (via SQL)

| Type | Nombre |
|---|---|
| Devices | 38 |
| Assets | 14 |
| Customers | 8 |
| Device Profiles | 7 |
| Entity Views | 0 |

### 5.2 Device Profiles

| Profil |
|---|
| AI Service |
| ALG16025001 |
| default |
| GPIO Controller |
| test |
| thermostat |
| *(doublon 'default' détecté)* |

### 5.3 Customers

| Customer |
|---|
| 016002 |
| Adrar |
| Customer A |
| Customer B |
| Customer C |
| Public |
| site alger n 1 |
| taki |

### 5.4 Relations

| Métrique | Valeur |
|---|---|
| Nombre total | 464 |
| Types | Contains, Manages, etc. |

### 5.5 Clés de télémétrie

| Métrique | Valeur |
|---|---|
| Clés uniques (key_dictionary) | 1 543 |
| Dont clés pour ALG16025001 | 292 |

---

## 6. Schéma de télémétrie observé

### 6.1 Table `ts_kv` (partitionnée RANGE ts)

```
Colonnes :
  entity_id  uuid               NOT NULL
  key        integer            NOT NULL   (FK → key_dictionary.key_id)
  ts         bigint             NOT NULL   (millisecondes UTC)
  bool_v     boolean
  str_v      varchar(10 000 000)
  long_v     bigint
  dbl_v      double precision
  json_v     json

PK : (entity_id, key, ts) — index btree unique
Partitionnement : RANGE(ts) par mois
Nombre de partitions : 14
  ts_kv_2023_12, ts_kv_2025_07, ts_kv_2025_09, ts_kv_2025_10,
  ts_kv_2025_11, ts_kv_2025_12, ts_kv_2026_01, ts_kv_2026_02,
  ts_kv_2026_03, ts_kv_2026_04, ts_kv_2026_05, ts_kv_2026_06,
  ts_kv_2026_07, ts_kv_latest, ts_kv_indefinite
```

### 6.2 Table `key_dictionary`

```
Colonnes :
  key    varchar(255)  NOT NULL
  key_id integer       NOT NULL  (séquence nextval)

PK : key_dictionary_id_pkey (key)
UK  : key_dictionary_key_id_key (key_id)
```

### 6.3 Table `relation`

```
Colonnes :
  from_id             uuid                 NOT NULL
  from_type           varchar(255)         NOT NULL
  to_id               uuid                 NOT NULL
  to_type             varchar(255)         NOT NULL
  relation_type_group varchar(255)         NOT NULL
  relation_type       varchar(255)         NOT NULL
  additional_info     varchar
  version             bigint               DEFAULT 0

PK : (from_id, from_type, relation_type_group, relation_type, to_id, to_type)
Index : idx_relation_from_id (relation_type_group, from_type, from_id)
Index : idx_relation_to_id   (relation_type_group, to_type, to_id)
```

### 6.4 Aucune table Trendz native détectée

La base `thingsboard` ne contient **aucune table Trendz** hormis `cf_debug_event` (table ThingsBoard CE standard pour le debug des champs calculés).  
Aucune table `business_entity`, `prediction_model`, `anomaly`, `view_config`, `metric_definition`, `trendz_task`, etc.  
**Conséquence :** il n'existe pas de schéma Trendz à reproduire. Trendx créera son propre schéma dans une base séparée `trendx` selon les noms consignés dans `docs/schema-mapping.md`.

---

## 7. Mesure d'impact d'une requête ThingsBoard

**Requête testée :** `SELECT count(*) FROM ts_kv WHERE entity_id = '<ALG16025001 UUID>' AND ts > (extract(epoch FROM now()) * 1000) - 86400000;`

**Résultat :**
- Durée : **11 secondes**
- Ligne retournée : 0 point sur les dernières 24h (données plus anciennes ou device inactif récemment)
- Impact observé : requête séquentielle sur partition unique sans index spécifique sur `entity_id` seul

**Conclusion :** Les requêtes sur `ts_kv` sans filtre strict sur `(entity_id, key, ts)` sont coûteuses. Trendx utilisera exclusivement le Canal 2 (PostgreSQL read-only) avec des requêtes bornées `WHERE entity_id = $1 AND key = $2 AND ts BETWEEN $3 AND $4` + `LIMIT` pour ne jamais dégrader ThingsBoard.

---

## 8. Volume disque et budget

### 8.1 Occupation actuelle

| Élément | Taille |
|---|---|
| Disk total (/dev/sdb2) | 1,1 TB |
| Utilisé | 669 GB |
| Libre | **374 GB** |
| % utilisé | 65 % |
| Images Docker | 30,99 GB |
| Volumes Docker | 46,49 GB |
| Build cache | 12,65 GB |

### 8.2 Télémetrie ThingsBoard

| Partition ts_kv | Taille |
|---|---|
| ts_kv_2026_07 | 12 GB |
| ts_kv_2026_06 | 8 523 MB |
| ts_kv_2026_05 | 1 705 MB |
| ts_kv_2026_04 | 1 164 MB |
| ts_kv_2026_02 | 836 MB |
| ts_kv_2026_01 | 702 MB |
| ts_kv_2026_03 | 280 MB |
| ts_kv_2025_09 | 170 MB |
| ts_kv_2025_12 | 109 MB |
| ts_kv_2025_11 | 37 MB |
| ts_kv_2025_10 | 11 MB |
| ts_kv_2025_07 | 1,84 MB |
| ts_kv_latest | 1,69 MB |
| ts_kv_2023_12 | 16 KB |
| ts_kv_indefinite | 16 KB |

**Estimation volume total ts_kv :** ~25–30 GB (partitions principales 2026)

### 8.3 Budget Trendx estimé (profil minimal)

| Composant | Estimation |
|---|---|
| Images Docker Trendx (5 images) | 3–5 GB |
| Volume PostgreSQL trendx_analytics (hypertables 90j × 38 devices) | 5–15 GB |
| Volume PostgreSQL trendx | 500 MB |
| Volume Airflow | 1–2 GB |
| Volume MLflow artefacts | 2–5 GB |
| Volume Grafana | 500 MB |
| Logs Docker (plafonnés 10 MB × 5 services) | 500 MB |
| **Total estimé** | **12–28 GB** |

**Espace libre disponible :** 374 GB  
**Espace après installation Trendx :** ≥ 346 GB  
**Seuil minimal requis (TRENDX_DISK_MIN_FREE_GB) :** 50 GB  
**Verdict :** ✅ Espace suffisant pour profil minimal.

---

## 9. Extensions PostgreSQL

| Extension | Version | Installée |
|---|---|---|
| plpgsql | 1.0 | ✅ |
| timescaledb | — | ❌ |
| postgres_fdw | — | ❌ |
| dblink | — | ❌ |

**Conséquence :** Trendx nécessitera l'activation de `timescaledb` sur la future base `trendx_analytics` (point d'arrêt #9 AGENTS.md).

---

## 10. Risques identifiés

| # | Risque | Niveau | Mitigation |
|---|---|---|---|
| R1 | Port 5433 occupé par TB PG sur l'hôte | Moyen | Trendx PG sur réseau interne uniquement ; pas d'exposition hôte 5432 |
| R2 | 11 s de latence sur requête ts_kv sans index optimisé | Élevé | Canal 2 avec requêtes bornées + index sur (entity_id, key, ts) |
| R3 | Volume ts_kv très important (25–30 GB) | Moyen | Rétention 2 ans + compression TimescaleDB |
| R4 | Aucune base Trendz existante à reproduire | Faible | Utiliser schema-mapping.md comme source de vérité |
| R5 | Identifiants TB en clair dans .env | Moyen | chmod 600 déjà appliqué ; prévoir vault pour prod |
| R6 | Swap 8 GB peu utilisé mais Prophet+MLflow parallèle | Faible | Monitoring + vm.swappiness=1 |

---

## 11. Points nécessitant approbation

| # | Point d'arrêt | Description | Approbation requise |
|---|---|---|---|
| 1 | Création rôle `trendx_ro` | User PostgreSQL read-only sur conteneur TB PG | Oui |
| 2 | Exposition port PostgreSQL | Ouvrir 5432 sur hôte ou tunnel SSH pour Canal 2 | Oui (sinon fallback API Canal 1) |
| 3 | Activation TimescaleDB | Extension sur base trendx_analytics | Oui |
| 4 | Premier writeback `_EPD_` | Écriture télémétrie ThingsBoard device test | Oui |
| 5 | Création alarmes TB | Alarmes ThingsBoard depuis anomalies | Oui |
| 6 | Ouverture ports Trendx sur pare-feu | 8000, 8080, 8082, 5000, 3001 | Oui |

---

## 12. Décisions attendues

1. Autorisez-vous la création du rôle `trendx_ro` sur le conteneur PostgreSQL 10.0.0.1 ?
2. Préférez-vous l'ouverture du port 5432 sur l'hôte ou un tunnel SSH pour le Canal 2 ?
3. Autorisez-vous l'activation de l'extension `timescaledb` sur la future base `trendx_analytics` ?
4. Confirmerez-vous le device test `ALG16025001` pour les premiers writebacks ?
5. Quelle politique de rétention appliquer (proposé : 2 ans ts_kv, 3 ans prédictions) ?
