# Trendx

Plateforme open source d'analyse IoT, fonctionnellement équivalente aux fonctionnalités publiquement documentées de ThingsBoard Trendz Analytics.

## Règles d'installation

- Installation sur le serveur unique **10.0.0.1**, dans `/opt/trendx`.
- ThingsBoard CE et PostgreSQL existent déjà sur ce serveur ; Trendx s'installe **à côté**, jamais à l'intérieur.
- La base PostgreSQL ThingsBoard est accessible en **lecture seule** via un rôle dédié `trendx_ro`.
- Les bases Trendx sont séparées de la base ThingsBoard : `trendx`, `trendx_analytics`, `trendx_airflow`, `trendx_mlflow`.
- Tout écriture vers ThingsBoard (télémétrie, alarmes, dashboards) est **désactivée par défaut** (`TB_WRITEBACK_ENABLED=false`, `TB_ALARMS_ENABLED=false`).

## Profil de déploiement par défaut

- `TRENDX_PROFILE=minimal`
  - requis   : `trendx-api`, `trendx-ui`, `trendx-worker` (planificateur intégré), `trendx-reverse-proxy`, bases Trendx dans PostgreSQL existant
  - supprimé : conteneur TimescaleDB dédié
  - reporté  : Airflow
  - optionnel: MLflow en mode fichier local, Grafana, Redis

## Prérequis

- Python 3.11+
- Docker & Docker Compose
- Make (optionnel)

## Démarrage rapide

```bash
cp .env.example .env
make setup
```

## Commandes utiles

| Commande | Description |
|----------|-------------|
| `make setup` | Installer les dépendances Python |
| `make up` | Démarrer la stack Docker |
| `make down` | Arrêter la stack |
| `make doctor` | Vérifier la configuration |
| `make lint` | Linter le code |
| `make test` | Lancer les tests |

## Documentation

- `AGENTS.md` : règles permanentes du projet
- `docs/` : documentation détaillée

## Sécurité

- Ne jamais commiter le fichier `.env`.
- Les secrets doivent être stockés dans `.secrets/` avec permissions `600`.
- `.env.example` ne contient que des placeholders.
