# Trendx — Sécurité (secrets, réseau, rôles PostgreSQL)

Source de vérité : règles dures AGENTS.md (§7, §8) et docs/service-accounts.md.

## 1. Principes

- Aucun secret dans le code, un fichier versionné, un log ou une sortie de terminal.
- `.env` et `.secrets/` ne sont jamais commités (`.gitignore`).
- Les mots de passe ne sont jamais passés en argument de ligne de commande (visibles
  dans `ps` / historique shell) : usage de `psql \password` (stdin) ou `PGPASSWORD`.
- Les services lisent les secrets via `env_file` ; aucun secret ne doit figurer dans
  un bloc `environment:` du Compose (visible via `docker inspect`).

## 2. Rôles PostgreSQL (instance partagée ThingsBoard + Trendx)

| Rôle | Base cible | Droits | Connexions |
|---|---|---|---|
| `trendx_app` | trendx | DML uniquement, NOCREATEDB, NOCREATEROLE | LIMIT 24 |
| `trendx_migration` | trendx | DDL/migrations, NOCREATEDB, NOCREATEROLE | illimité |
| `trendx_ro` | thingsboard (lecture seule) | SELECT seul, `default_transaction_read_only=on` | LIMIT 5 |

- Le mot de passe du rôle `postgres` et ceux des rôles ThingsBoard ne sont JAMAIS modifiés.
- La base `thingsboard` reste strictement en lecture seule côté Trendx ; aucune création
  d'objet, aucune extension (postgres_fdw, dblink, timescaledb interdits).

## 3. Risque accepté : pg_hba `trust` sur 127.0.0.1 (constat 2026-08-02)

La configuration `pg_hba.conf` de l'instance partagée autorise `host all all
127.0.0.1/32 trust` (et `::1/128`). Conséquence : tout `docker exec` sur le conteneur
PostgreSQL équivaut à un accès superutilisateur sans mot de passe.

- **Non corrigé volontairement** : modifier `pg_hba.conf` exigerait de recharger la
  configuration du conteneur PostgreSQL géré par ThingsBoard, opération interdite.
- **Mesures compensatoires** :
  - accès à l'hôte (10.0.0.1) strictement limité (SSH root réservé aux opérateurs) ;
  - restriction de l'accès au démon Docker (utilisateurs autorisés uniquement) ;
  - journalisation des accès (`/var/log/auth.log`, `journalctl -u docker`) ;
  - jamais d'exposition du port 5432 sur l'hôte ni sur Internet.

## 4. Procédure de rotation des mots de passe

Périodicité : tous les 90 jours, ou immédiatement après tout incident / suspicion de fuite.

1. Prérequis : vérifier les rôles et connexions actives :
   `SELECT usename, count(*) FROM pg_stat_activity GROUP BY usename;`
2. Générer un secret distinct par rôle (jamais réutilisé) :
   `openssl rand -base64 32` (écrit dans un fichier mode 600 hors dépôt, ex. `/tmp/kilo/rot/`).
3. Appliquer SANS secret en clair (transmet un hachage SCRAM, invisible dans les logs,
   l'historique et `pg_stat_activity`) :
   `export HISTFILE=/dev/null` puis
   `printf '%s\n%s\n' "$PASS" "$PASS" | docker exec -i <PG_CONTAINER> psql -U postgres -d postgres -c '\password <role>'`
4. Persister : mettre à jour `.secrets/service-accounts.env` (chmod 600, dir 700) puis
   `.env` (repo + /opt/trendx, chmod 600). Aucune sortie terminal du secret.
5. Recréer les conteneurs Trendx (`docker compose up -d --no-deps --force-recreate api worker`)
   et vérifier les journaux (aucune erreur d'authentification).
6. Rejouer les tests négatifs :
   - `trendx_ro` : INSERT sur `thingsboard` refusé (read-only) ;
   - `trendx_app` : CREATE TABLE qualifié refusé sur `trendx_catalog`, `trendx_analytics` et `public`.
7. Vérifier que les attributs des rôles n'ont pas été réinitialisés :
   `SELECT rolname, rolconnlimit, rolcreatedb, rolsuper FROM pg_roles WHERE rolname LIKE 'trendx%';`
   (attendu : `trendx_ro`=5, `trendx_app`=24, `trendx_migration` NOCREATEDB, aucun superutilisateur).

Dernière rotation : 2026-08-02 (rôles `trendx_app`, `trendx_migration`, `trendx_ro`).

## 5. TLS du reverse-proxy (état réel — Phase 2)

- Le reverse-proxy Trendx sert en **HTTP simple** sur `127.0.0.1:8443` (aucun TLS dans
  le conteneur nginx, aucun certificat, aucun secret `tls_cert`/`tls_key`).
- Le chiffrement est **délégué au tunnel SSH** pour tout accès distant
  (`ssh -L 8443:127.0.0.1:8443 root@10.0.0.1`).
- **TLS reporté à une phase ultérieure** (certificat auto-signé ou ACME + HSTS).
- **Règle stricte tant que TLS est absent :** le port 8443 n'est JAMAIS publié ailleurs
  que sur `127.0.0.1`. Pas d'exposition sur `0.0.0.0`, pas d'IP LAN, pas de NAT, pas de
  port hôte supplémentaire.
- Ne pas réintroduire `TLS_CERT`/`TLS_KEY`/`tls_cert`/`tls_key` dans le Compose tant que
  le TLS n'est pas réellement implémenté.
- L'authentification de l'API (`/api/v1/*`) reste obligatoire et indépendante du TLS :
  jeton Bearer signé (voir §6).

## 6. Authentification de l'API Trendx

- Tous les endpoints `/api/v1/*` exigent un jeton (`Authorization: Bearer <jeton>`
  ou `X-API-Key: <jeton>`) vérifié en temps constant (`hmac.compare_digest`).
- Absence de jeton → HTTP 401 ; jeton invalide → HTTP 401 (jamais 403 ;
  la réponse ne doit pas révéler que le jeton existe).
- Endpoints publics pour l'infrastructure uniquement : `/health`, `/metrics`, `/`,
  `/docs`, `/redoc`, `/openapi.json` (healthchecks Docker et reverse-proxy).
- `/health` ne divulgue ni version, ni environnement, ni configuration, ni état de
  la base : retour minimal `{"status":"ok","service":"trendx-api"}`.
- `/metrics` ne divulgue pas de secrets (seul `service`, `version`, `uptime_seconds`,
  `memory_bytes_approx`).
- Clé : `TRENDX_API_TOKEN` dans `.env` (mode 600, hors dépôt), générée par
  `openssl rand -hex 32`. Jamais journalisée, jamais transmise dans une URL
  (query string, fragment, referer).
- Politique de rotation : jeton statique partagé, rotation recommandée tous les
  90 jours. L'ancien jeton reste valide pendant une fenêtre de transition de 24 h
  (double acceptation) puis est révoqué.
- Dernière rotation : 2026-08-03 — le jeton précédent a été révoqué après avoir
  été exposé en clair dans une sortie de rapport (incident de fuite).
- Les tokens ThingsBoard (JWT) ne transitent jamais par l'API Trendx publique ;
  ils restent confinés au worker (Canal 1) et au moteur `tb_readonly` (Canal 2).

## 7. Constat : compte tenant ThingsBoard et mot de passe d'usine (2026-08-03)

Lors de l'investigation STOP du 2026-08-03, il a été constaté que le compte
tenant ThingsBoard (compte d'administration du tenant, fonctionnel sur la
production) s'authentifie encore avec le **mot de passe d'usine** de ThingsBoard,
c'est-à-dire le défaut de fabrication du produit.

- **Rotation à la charge de l'opérateur humain** : le mot de passe du compte
  tenant est changé dans ThingsBoard par l'utilisateur lui-même. Ni l'agent ni
  aucun script Trendx ne le modifie. Cette section ne contient volontairement
  aucune valeur (ni intégrale, ni partielle).
- **Purge effectuée** : la seule référence littérale au mot de passe d'usine dans
  le code (une blacklist de placeholders) a été retirée, puis l'historique git a
  été réécrit (2026-08-03) pour purger le commit qui l'introduisait. Aucune
  occurrence ne subsiste dans le dépôt ni dans `/opt/trendx`.
- **Règle de code** : aucune heuristique ne spécialise un mot de passe réel. La
  blacklist des placeholders ne contient que `CHANGE_ME`. La source de vérité de
  l'authentification ThingsBoard est le drapeau `TB_AUTH_CONFIGURED` (défaut
  `false`) : il n'est mis à `true` que lorsque des identifiants vérifiés d'un
  compte de service sont en place (voir `docs/service-accounts.md`).
- **Verrou d'ingestion** : tant que `TB_AUTH_CONFIGURED != true`, toute tentative
  d'ingestion est refusée (HTTP 409) par l'endpoint `/api/v1/ingestion/trigger`.
