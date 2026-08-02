# Comptes de service Trendx

## Principes

`scripts/bootstrap-service-accounts.sh` est en mode `--check` par défaut. Il ne contacte aucun service distant dans ce mode. L'application exige simultanément `--apply` et `TRENDX_CONFIRM_APPLY=YES`. Les mots de passe absents sont générés par OpenSSL et conservés dans `TRENDX_CREDENTIALS_FILE` (par défaut `.secrets/service-accounts.env`, permissions `600`), jamais dans Git ni dans les logs.

Le rôle PostgreSQL `trendx_migration` est un login non privilégié dédié aux migrations. Il reçoit `CONNECT` sur les trois bases et `USAGE, CREATE` sur `public`; il peut donc créer/modifier le schéma sans devenir superuser. Les bases déjà existantes ne changent pas de propriétaire automatiquement. `trendx_app` est le rôle applicatif analytique, `trendx_airflow` et `trendx_mlflow` sont séparés par base, et `trendx_grafana` dispose uniquement de `CONNECT`, `USAGE` et `SELECT` sur l'analytique. Les privilèges publics sont révoqués et les privilèges par défaut sont définis pour les objets créés par la migration.

## Exécution locale contrôlée

```bash
cp .env.example .env
chmod 600 .env
bash scripts/bootstrap-service-accounts.sh --check
TRENDX_CONFIRM_APPLY=YES bash scripts/bootstrap-service-accounts.sh --apply
```

Ne renseignez jamais un mot de passe dans une commande ou un fichier suivi. Les variables `*_PASSWORD` peuvent être exportées depuis un coffre ou un fichier `600`. Le mot de passe administrateur PostgreSQL est lu par `PGPASSWORD` et n'est pas affiché.

## Grafana

Avec `GRAFANA_API_ENABLED=true`, le script exige que le dossier `Trendx` et le datasource existent déjà, puis crée ou réutilise le service account. Cette prévalidation intervient avant toute mutation. Il donne la permission datasource `Query` et la permission dossier `Edit` via `PUT` par défaut (`GRAFANA_FOLDER_PERMISSION_METHOD=POST` pour une ancienne version explicitement validée). Le token est créé une seule fois puis écrit dans `GRAFANA_TOKEN_FILE`, mode `600`; il n'est jamais imprimé. Les permissions d'alertes restent désactivées par défaut.

## ThingsBoard

La création est désactivée par défaut. L'adaptateur contacte ThingsBoard uniquement après confirmation explicite du device, du customer isolé (`TB_CONFIRMED_CUSTOMER_ISOLATED=true`) et du fait que ce customer ne contient qu'un seul device (`TB_CONFIRMED_CUSTOMER_DEVICE_COUNT=1`). Ces confirmations sont des préconditions opérateur, pas une capacité de filtrage ajoutée au `CUSTOMER_USER`. L'URL de création est obligatoire car elle varie selon la version (`TB_SERVICE_USER_CREATE_URL`). Toute discordance arrête le script avant création. Le compte demandé est `CUSTOMER_USER`; aucune alarme ni écriture de télémétrie n'est appelée. Un customer partagé ne peut pas être limité à un device par ce script.

## Compte de service ThingsBoard — Phase 3 (Canal 1 REST)

### Variables attendues (coffre → `.secrets/service-accounts.env`, mode 600)

| Variable | Emplacement | Usage |
|---|---|---|
| `TB_SERVICE_USER_EMAIL` | `.secrets/service-accounts.env` | Email du compte de service ThingsBoard |
| `TB_SERVICE_USER_PASSWORD` | `.secrets/service-accounts.env` | Mot de passe du compte de service |

- L'opérateur renseigne ces deux valeurs depuis le coffre ; elles ne sont JAMAIS versionnées ni journalisées.
- À l'exécution, `settings.tb_login` (config.py) retombe sur ces valeurs si `.env` (`TB_USERNAME`/`TB_PASSWORD`) contient encore des placeholders.
- Contrôle sans fuite : `make doctor` → `scripts/tb_auth_check.py` POSTe `/api/auth/login` et n'affiche que `OK`/`SKIP`/`FAIL` (jamais le mot de passe ni le JWT).

### Points d'accès REST ThingsBoard nécessaires en Phase 3

Phase 3 = découverte de topologie + ingestion incrémentale (lecture seule). Les droits du compte de service doivent couvrir :

| Méthode | Endpoint | Usage Phase 3 |
|---|---|---|
| POST | `/api/auth/login` | Authentification (obtention du JWT) |
| GET | `/api/info` | Version/état du serveur (diagnostic) |
| GET | `/api/tenant/devices?pageSize&page` | Liste paginée des devices du tenant |
| GET | `/api/device/{deviceId}` | Détail d'un device (attributs serveur) |
| GET | `/api/deviceProfiles?pageSize&page` | Profils device (regroupement business entities) |
| GET | `/api/tenant/assets?pageSize&page` | Assets du tenant (topologie) |
| GET | `/api/customers?tenantId&pageSize&page` | Customers (isolation par tenant/customer) |
| GET | `/api/relations?pageSize&page` | Relations entités (découverte topologie) |
| GET | `/api/plugins/telemetry/{entityType}/{entityId}/attributes/{scope}` | Attributs client/shared/server |
| GET | `/api/plugins/telemetry/{entityType}/{entityId}/keys/timeseries` | Liste des clés télémétrie |
| GET | `/api/plugins/telemetry/{entityType}/{entityId}/values/timeseries?keys&startTs&endTs&limit` | **Valeurs télémétrie (chemin d'ingestion principal)** |

Uniquement si le writeback/alarmes est activé explicitement (`TB_WRITEBACK_ENABLED`/`TB_ALARMS_ENABLED`, désactivés par défaut) :
`POST /api/plugins/telemetry/{entityType}/{entityId}/timeseries/ANY`, `POST /api/alarm`,
`GET /api/alarm`, `PUT /api/alarm/{alarmId}/ack`, `PUT /api/alarm/{alarmId}/clear`.

> Note : `GET /api/tenants` (sysadmin) n'est pas requis si le compte est un compte tenant ;
> la découverte se borne à son propre tenant.

## Exécution distante et rollback

Toute exécution distante nécessite une confirmation séparée du propriétaire de l'environnement et `TRENDX_CONFIRM_APPLY=YES`. Utilisez une clé SSH, l'agent SSH ou une invite interactive, sans passer le mot de passe root en argument, dans `ssh`, `sshpass`, l'historique shell ou les logs. Exemple non destructif:

```bash
ssh -o BatchMode=yes admin@hote 'cd /srv/trendx && ./scripts/bootstrap-service-accounts.sh --check'
```

Le rollback PostgreSQL est un modèle à relire: `migrations/000_service_accounts_rollback.sql`. Il ne supprime aucune base ni aucun rôle. Toute commande `REVOKE`, `DROP OWNED`, révocation de token Grafana ou suppression d'utilisateur ThingsBoard doit être revue et confirmée séparément; aucune n'est exécutée automatiquement par ce dépôt.
