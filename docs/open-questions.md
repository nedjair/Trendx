# Questions ouvertes et blocages

**Version :** 1.0 — Phase 1  
**Date :** 2026-07-30

---

## 1. Blocages nécessitant approbation explicite

### B1 — Création rôle PostgreSQL `trendx_ro`

**État :** ⏳ En attente  
**Description :** Créer un user read-only sur le conteneur PostgreSQL ThingsBoard pour Canal 2 haute performance.  
**Alternative :** utiliser uniquement Canal 1 API (moins performant, pas de blocage).  
**REQUIS :** commande SQL `CREATE ROLE trendx_ro WITH LOGIN PASSWORD '...' GRANT SELECT ON ALL TABLES IN SCHEMA public TO trendx_ro; ALTER ROLE trendx_ro SET default_transaction_read_only = on;`  
**IMPACT :** sans Canal 2, ingestion massive limitée par API REST ThingsBoard.

### B2 — Exposition port PostgreSQL sur l'hôte

**État :** ⏳ En attente  
**Description :** Le port PostgreSQL ThingsBoard est actuellement mappé sur `127.0.0.1:5433`. Pour Canal 2 depuis conteneurs Trendx sur réseau externe, le port doit être accessible ou un tunnel SSH établi.  
**Options :**
- Ouvrir 5432/5433 sur l'hôte vers 10.0.0.1 uniquement
- Créer un tunnel SSH
- Garder Canal 1 API uniquement

### B3 — Premier writeback télémétrie ThingsBoard

**État :** ⏳ En attente  
**Description :** Écrire `_EPD_mppt_main_battery_voltage_v` et `_EPD_mppt_main_battery_current_a` sur device test `ALG16025001`.  
**REQUIS :** `TB_WRITEBACK_ENABLED=true` + approbation explicite device/customer.  
**IMPACT :** validation writeback avant déploiement multi-devices.

### B4 — Création alarmes ThingsBoard

**État :** ⏳ En attente  
**Description :** Créer des alarmes TB depuis anomalies détectées par Trendx.  
**REQUIS :** `TB_ALARMS_ENABLED=true` + configuration hystérésis + cooldown.  
**IMPACT :** alerte opérateur, nécessite validation seuils.

### B5 — Activation extension TimescaleDB

**État :** ⏳ En attente  
**Description :** Activer `timescaledb` sur la base `trendx_analytics`. Nécessite un point d'arrêt AGENTS.md §9.  
**REQUIS :** `CREATE EXTENSION IF NOT EXISTS timescaledb;` dans `trendx_analytics`.  
**IMPACT :** sans TimescaleDB, pas de hypertables ni CAGGs → performances dégradées sur séries temporelles.

### B6 — Ouverture ports Trendx sur pare-feu hôte

**État :** ⏳ En attente  
**Description :** Ouvrir 8000, 8080, 8082, 5000, 3001 si accès externe requis.  
**Alternative :** accès `127.0.0.1` uniquement (localhost).  
**IMPACT :** accès distant aux services Trendx.

---

## 2. Questions techniques sans blocage immédiat

### Q1 — Authentification ThingsBoard

**Constat :** Les identifiants `tenant@thingsboard.org` / `tenant` standards échouent (401).  
**Hypothèse :** instance personnalisée avec domaines (`mobilis.dz`).  
**Action :** vérifier les identifiants exacts dans `.env` (non divulgués dans ce document).  
**Impact :** Canal 1 API dépend de credentials valides.

### Q2 — Canal 2 PostgreSQL sans port public

**Constat :** PostgreSQL TB est mappé sur `127.0.0.1:5433`. Les conteneurs Trendx sur `mobili_dahsboard_default` peuvent y accéder par nom de conteneur (`mobili_dahsboard-postgres-1:5432`).  
**Question :** Suffisant pour Phase 3 ? ou faut-il ouvrir le port sur l'hôte pour accès direct ?

### Q3 — Volume ts_kv et stratégie d'ingestion

**Constat :** 25–30 GB de ts_kv sur ThingsBoard. ALG16025001 possède 292 clés avec jusqu'à 2,4M points pour certaines clés système.  
**Question :** Ingérer l'historique complet ou seulement les clés métier MVP (`mppt_main_battery_voltage_v`, `mppt_main_battery_current_a`, `battery_state_of_charge_pct`) ?  
**Recommandation :** ingestion ciblée MVP d'abord, puis extension progressive.

### Q4 — Relations métier ThingsBoard vs Business Entities Trendx

**Constat :** 464 relations TB existent (RULE_CHAIN, etc.), mais aucune BE Trendz.  
**Question :** Les relations métier (Building → Apartment → Meter) doivent-elles être créées manuellement dans Trendx ou déduites de la topologie TB ?

### Q5 — Doublons `device_profile` 'default'

**Constat :** 2 profiles nommés `default` dans TB.  
**Question :** comportement attendu Trendx ? Ignorer, fusionner, ou préfixer ?

### Q6 — Stratégie de rétention

**Constat :** Partition TB `ts_kv_2023_12` existe (16 KB).  
**Question :** rétention Trendx proposée à 2 ans pour ts_kv, 3 ans pour prédictions. Accord ?

### Q7 — Swap et Prophet

**Constat :** Swap 8 GB, pression mémoire possible si Prophet + MLflow en parallèle.  
**Question :** ajouter swap fichier 4 GB ? ou limiter workers Prophet à 1 ?

---

## 3. Éléments inconnus (à investiguer en Phase 2)

- Identifiants ThingsBoard exacts pour Canal 1 API (en dehors de `.env`)
- Comportement exact des 29 `calculated_field` existants (sont-ils utilisés ?)
- Modèle `ai_model` existant : est-il un Trendz AI Assistant ou autre ?
- Politique de quotas ThingsBoard (API rate limit)
- Plan de sauvegarde actuel de `thingsboard` (géré par TB ou admin ?)
- **Aucune base Trendz ni schema Trendz sur l'hôte 10.0.0.1** : clause de repli activée (voir `docs/schema-mapping.md`). Si une base Trendz existe ailleurs, elle devra être exportée en lecture seule (`pg_dump --schema-only`) puis comparée au mapping Trendx avant toute modification. Le fichier `docs/proposed-schema.sql` est NON NORMATIF : dump généré sans provenance certifiée.

---

## 4. Décisions attendues de l'opérateur

1. **B1** : Autorisez-vous la création de `trendx_ro` ?
2. **B2** : Port 5432 ouvert sur hôte ou tunnel SSH ?
3. **B5** : Activation `timescaledb` sur `trendx_analytics` ?
4. **B6** : Ouverture ports Trendx sur pare-feu ?
5. **B3** : Device test `ALG16025001` pour writeback ?
6. **Q3** : Ingestion historique complète ou ciblée MVP ?
7. **Q6** : Rétention 2 ans / 3 ans acceptée ?
