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
   - `trendx_app` : CREATE TABLE sur `trendx` refusé (permission denied).
7. Vérifier que les attributs des rôles n'ont pas été réinitialisés :
   `SELECT rolname, rolconnlimit, rolcreatedb, rolsuper FROM pg_roles WHERE rolname LIKE 'trendx%';`
   (attendu : `trendx_ro`=5, `trendx_app`=24, `trendx_migration` NOCREATEDB, aucun superutilisateur).

Dernière rotation : 2026-08-02 (rôles `trendx_app`, `trendx_migration`, `trendx_ro`).
