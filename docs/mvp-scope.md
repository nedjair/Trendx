# Périmètre du MVP Trendx

## Source
- Instance ThingsBoard : https://10.0.0.1:8081
- Version ThingsBoard : Community Edition 4.3.1.1 (release-4.3 commit c2a52e4, 2026-03-30)
- Device MVP principal : ALG16025001
- Device ID (placeholder, à remplacer par l'UUID réel) : ALG16025001-resolved-by-api
- Clés de télémétrie MVP :
  - `mppt_main_battery_voltage_v` : tension batterie principale (Volts DC) — **~9600 points/30j détectés, valeur stable ~13.11V**
  - `mppt_main_battery_current_a` : courant batterie principale (Ampères DC) — **~9600 points/30j, ~-0.41A**
  - `battery_state_of_charge_pct` (alias ALG16025001-battery) : SoC si disponible
- Fréquence nominale : ~5 min (~288 pts/jour)

## Avertissement sur ancien device
- Device CerboGx (f3ce3c10) et métrique `batterylevel` : **0 POINT SUR 90 JOURS** (appareil inactif)
- ⚠️ Ne PAS utiliser CerboGx comme MVP sans remise en service préalable

## Données
- Historique à extraire : 90 jours (fenêtre minimale, étendre si disponible)
- Agrégation d’entrée configurable : 1 heure (agrégat Moy/Min/Max/Count)
- Fuseau de stockage : UTC (timestamps ms stockés en bigint, dates sans ambigüité)
- Politique valeurs manquantes : forward fill temporel borné → interpolation linéaire → backfill dernier recours
- Valeurs minimales/maximales métier : par métrique, héritées de `metric_definition.how_to_calculate` ou config
  - `mppt_main_battery_voltage_v` : min=0, max=60
  - `mppt_main_battery_current_a` : min=-100, max=+100
  - `battery_state_of_charge_pct` : min=0, max=100

## Prévision
- Premier algorithme : Prophet (propriétés tendance+saisonnalité quotidienne+hebdomadaire)
- Horizon : 24 heures
- Fréquence : 1 heure
- Réentraînement : quotidien (modèle champion conservé si gain < seuil)
- Prévision : toutes les heures
- Préfixe writeback Prévision (EPD = Estimated Predicted Data) : `_EPD_<metric_name>`
- Préfixe writeback Champ Calculé (ECD = Estimated Calculated Data) : `_ECD_<field_name>`

## Validation
- Métrique principale : MAE (robuste aux outliers, interprétable en Volts/Ampères/%)
- Métriques secondaires obligatoires : RMSE, sMAPE, coverage@80%, coverage@95%, mean interval width, bias
- Sélection modèle : **jamais uniquement sur MAPE** (hors biais zéro-proche)
- Critères de promotion champion : métrique principale + stabilité inter-fenêtres + couverture + contraintes métier + temps d'entraînement acceptable

## Accès canal 2 PostgreSQL read-only
- Conteneur ThingsBoard PostgreSQL publie sur **port hôte 32768** (mapping 0.0.0.0:32768→5432 conteneur)
- Schéma natif ThingsBoard CE : `ts_kv` partitionné (ts RANGE ms) + `key_dictionary` key_id↔key
- Fallback canal 2 → Canal 1 API si user PG read-only absent ou port fermé
- **POINT D'ARRÊT #1** : création user PostgreSQL `trendx_ro` sur serveur 10.0.0.1 → nécessite approbation

## Ports Trendx (conflits résolus)
- 8081 = ThingsBoard CE (occupé)
- 8082 = Airflow Webserver (LIBRE)
- 5000 = MLflow Tracking (LIBRE)
- 5432 = TimescaleDB Trendx (LIBRE — TB PG est sur 32768)
- **3001 = Grafana** (3000 occupé par Gitea → décalé +1)
- 8090 = ThingsBoard Mobilis (2ème tenant, 608k pts ts_kv)
- 8888 = Trendz Analytics 1.15.0 (RÉFÉRENCE PARITÉ, déjà installé)

## Environnement
- Premier environnement : développement (même machine physique, réseaux Docker dédiés)
- Writeback TB : désactivé par défaut, opt-in explicite `TB_WRITEBACK_ENABLED=true` + approbation
- Création alarmes TB : désactivée par défaut, opt-in `TB_ALARMS_ENABLED=true` + approbation
- Instance Mobilis (8090) : base télémetrie monitoring TB uniquement, 0 entité business
- 42 tables Trendz natives déjà présentes dans TB PG → référence schéma parité fonctionnelle