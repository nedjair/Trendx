#!/usr/bin/env bash
# =============================================================
# restore-test.sh — Rejoue la restauration complète de la base trendx
# sur une base jetable (trendx_restore_test) puis prouve l'état canonique.
#
# Appel : make restore-test   (ou  bash scripts/restore-test.sh)
#
# Périmètre : conteneur PostgreSQL ThingsBoard existant, base trendx NON touchée.
#   - crée une base jetable trendx_restore_test (supprimée en fin de succès) ;
#   - restaure le dernier dump avec --role=trendx_migration (propriété canonique) ;
#   - rétablit l'alignement ACL de la base (CREATE retiré, CONNECT trendx_app) ;
#   - prouve la parité avec la prod (tables/fonctions) et l'état canonique
#     (propriétaire trendx_migration, trendx_app DML seul, pas de CREATE) ;
#   - échoue (exit 1) à la première preuve non vérifiée, en conservant la base
#     jetable pour diagnostic.
#
# Sécurité : ne touche jamais à la base trendx de prod ; aucun DROP sauf sur la
# base jetable. Nécessite l'approbation explicite avant exécution (points d'arrêt
# AGENTS.md : création d'une base dans le conteneur PostgreSQL existant).
# =============================================================

set -euo pipefail

PG="${PG_EXTERNAL_CONTAINER:-mobili_dahsboard-postgres-1}"
BACKUP_ROOT="/opt/trendx/data/backups"
TEST_DB="trendx_restore_test"
TMP_DUMP="/tmp/trendx_restore_test.dump.gz"

fatal() { echo "[restore-test] FATAL : $*" >&2; exit 1; }
step()  { echo "[restore-test] $*"; }
ok()    { echo "[restore-test]   OK : $*"; }

# ── Préconditions ────────────────────────────────────────────
latest="$(ls -td "${BACKUP_ROOT}"/*/ 2>/dev/null | head -1 || true)"
[ -n "${latest}" ] || fatal "aucun dossier de backup dans ${BACKUP_ROOT}"
dump="$(ls "${latest}"trendx.dump.gz 2>/dev/null | head -1 || true)"
[ -n "${dump}" ] || fatal "aucun trendx.dump.gz dans ${latest}"
step "dump utilisé : ${dump}"

docker exec "${PG}" psql -U postgres -tAc "SELECT 1" >/dev/null 2>&1 \
  || fatal "conteneur PostgreSQL ${PG} injoignable"

# ── 1. Rôles prérequis (idempotent) ──────────────────────────
step "1/6 rôles prérequis (idempotent)"
docker exec "${PG}" psql -U postgres -v ON_ERROR_STOP=1 -q -c "
DO \$\$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='trendx_migration') THEN
    CREATE ROLE trendx_migration LOGIN;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='trendx_app') THEN
    CREATE ROLE trendx_app LOGIN;
  END IF;
END \$\$;"
ok "rôles trendx_migration / trendx_app présents"

# ── 2. Base jetable ──────────────────────────────────────────
step "2/6 base jetable ${TEST_DB}"
docker exec "${PG}" psql -U postgres -v ON_ERROR_STOP=1 -q -c \
  "DROP DATABASE IF EXISTS ${TEST_DB} WITH (FORCE);"
docker exec "${PG}" psql -U postgres -v ON_ERROR_STOP=1 -q -c \
  "CREATE DATABASE ${TEST_DB} OWNER postgres;"
docker exec "${PG}" psql -U postgres -v ON_ERROR_STOP=1 -q -c \
  "GRANT CONNECT, CREATE, TEMPORARY ON DATABASE ${TEST_DB} TO trendx_migration;"
ok "base ${TEST_DB} créée, CREATE temporaire accordé à trendx_migration"

# ── 3. Restauration (docker cp : le pg_restore du conteneur ne lit pas stdin) ──
step "3/6 restauration du dump (--role=trendx_migration)"
docker cp "${dump}" "${PG}:${TMP_DUMP}"
if ! docker exec "${PG}" sh -c \
    "gunzip -c ${TMP_DUMP} | pg_restore -U trendx_migration --clean --if-exists --no-owner \
       --role=trendx_migration -d ${TEST_DB}"; then
  docker exec "${PG}" rm -f "${TMP_DUMP}" || true
  fatal "pg_restore a échoué (base ${TEST_DB} conservée pour diagnostic)"
fi
docker exec "${PG}" rm -f "${TMP_DUMP}"
ok "restauration terminée (schémas + ACL créés par le dump)"

# ── 4. Alignement ACL base ───────────────────────────────────
step "4/6 alignement ACL base (retrait du CREATE temporaire)"
docker exec "${PG}" psql -U postgres -v ON_ERROR_STOP=1 -q -d "${TEST_DB}" -c \
  "REVOKE CREATE ON DATABASE ${TEST_DB} FROM trendx_migration;"
docker exec "${PG}" psql -U postgres -v ON_ERROR_STOP=1 -q -d "${TEST_DB}" -c \
  "GRANT CONNECT ON DATABASE ${TEST_DB} TO trendx_app;"
ok "ACL base alignées"

# ── 5. Preuves ───────────────────────────────────────────────
step "5/6 preuves (parité prod + état canonique)"

run_q() { # run_q <db> <sql> → valeur trimée
  docker exec "${PG}" psql -U postgres -tAc "$2" -d "$1" | tr -d '[:space:]'
}

Q_TABLES="SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace \
WHERE n.nspname IN ('trendx_catalog','trendx_analytics') AND c.relkind IN ('r','p');"
Q_FUNCS="SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace \
WHERE n.nspname NOT IN ('pg_catalog','information_schema') AND p.prokind='f';"
Q_SEQS="SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace \
WHERE n.nspname IN ('trendx_catalog','trendx_analytics') AND c.relkind='S';"
Q_OWN_NOT_MIG="SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace \
WHERE n.nspname IN ('trendx_catalog','trendx_analytics') AND c.relkind IN ('r','p','S','v','m') \
AND c.relowner <> (SELECT oid FROM pg_roles WHERE rolname='trendx_migration');"
Q_OWN_FUNC_NOT_MIG="SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace \
WHERE n.nspname IN ('trendx_catalog','trendx_analytics') AND p.prokind='f' \
AND p.proowner <> (SELECT oid FROM pg_roles WHERE rolname='trendx_migration');"
Q_SCHEMA_OWN="SELECT count(*) FROM pg_namespace \
WHERE nspname IN ('trendx_catalog','trendx_analytics') \
AND nspowner <> (SELECT oid FROM pg_roles WHERE rolname='trendx_migration');"
Q_APP_NO_CREATE="SELECT count(*) FROM pg_namespace \
WHERE nspname IN ('trendx_catalog','trendx_analytics') \
AND has_schema_privilege('trendx_app', nspname, 'CREATE');"
Q_APP_USAGE="SELECT count(*) FROM pg_namespace \
WHERE nspname IN ('trendx_catalog','trendx_analytics') \
AND NOT has_schema_privilege('trendx_app', nspname, 'USAGE');"
Q_APP_DML="SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace \
WHERE n.nspname IN ('trendx_catalog','trendx_analytics') AND c.relkind IN ('r','p') \
AND NOT (has_table_privilege('trendx_app', c.oid, 'SELECT') \
     AND has_table_privilege('trendx_app', c.oid, 'INSERT') \
     AND has_table_privilege('trendx_app', c.oid, 'UPDATE') \
     AND has_table_privilege('trendx_app', c.oid, 'DELETE'));"

# 5.1 Parité de structure avec la prod
t_prod="$(run_q trendx "${Q_TABLES}")"
t_test="$(run_q "${TEST_DB}" "${Q_TABLES}")"
f_prod="$(run_q trendx "${Q_FUNCS}")"
f_test="$(run_q "${TEST_DB}" "${Q_FUNCS}")"
s_prod="$(run_q trendx "${Q_SEQS}")"
s_test="$(run_q "${TEST_DB}" "${Q_SEQS}")"
echo "[restore-test] tables (2 schémas) : prod=${t_prod} test=${t_test}  fonctions : prod=${f_prod} test=${f_test}  séquences : prod=${s_prod} test=${s_test}"
[ "${t_prod}" = "${t_test}" ] || fatal "parité tables KO (prod=${t_prod} test=${t_test})"
[ "${f_prod}" = "${f_test}" ] || fatal "parité fonctions KO (prod=${f_prod} test=${f_test})"
[ "${s_prod}" = "${s_test}" ] || fatal "parité séquences KO (prod=${s_prod} test=${s_test})"
ok "parité de structure avec la prod (tables/fonctions/séquences)"

# 5.2 Propriété canonique : 0 objet non trendx_migration dans les 2 schémas
own="$(run_q "${TEST_DB}" "${Q_OWN_NOT_MIG}")"
ownf="$(run_q "${TEST_DB}" "${Q_OWN_FUNC_NOT_MIG}")"
sch="$(run_q "${TEST_DB}" "${Q_SCHEMA_OWN}")"
[ "${own}" = "0" ] || fatal "objets non trendx_migration : ${own}"
[ "${ownf}" = "0" ] || fatal "fonctions non trendx_migration : ${ownf}"
[ "${sch}" = "0" ] || fatal "schémas non trendx_migration : ${sch}"
ok "0 objet / fonction / schéma hors propriété trendx_migration"

# 5.3 trendx_app : USAGE partout, CREATE nulle part, DML partout
nocr="$(run_q "${TEST_DB}" "${Q_APP_NO_CREATE}")"
usage="$(run_q "${TEST_DB}" "${Q_APP_USAGE}")"
dml="$(run_q "${TEST_DB}" "${Q_APP_DML}")"
[ "${nocr}" = "0" ] || fatal "trendx_app a CREATE sur ${nocr} schéma(s)"
[ "${usage}" = "0" ] || fatal "trendx_app n'a pas USAGE sur ${usage} schéma(s)"
[ "${dml}" = "0" ] || fatal "trendx_app sans DML complet sur ${dml} table(s)"
ok "trendx_app : USAGE OK, CREATE absent, DML complet"

# 5.4 Négatif : CREATE TABLE en trendx_app doit échouer sur les 3 schémas
for sch in trendx_catalog trendx_analytics public; do
  if docker exec "${PG}" psql -U postgres -d "${TEST_DB}" -q -c \
      "SET ROLE trendx_app; CREATE TABLE ${sch}.t_probe(id integer);" \
      >/dev/null 2>&1; then
    docker exec "${PG}" psql -U postgres -d "${TEST_DB}" -q -c \
      "DROP TABLE IF EXISTS ${sch}.t_probe;" >/dev/null 2>&1 || true
    fatal "négatif KO : CREATE TABLE en trendx_app accepté sur ${sch}"
  fi
done
ok "négatif : CREATE TABLE en trendx_app refusé sur trendx_catalog, trendx_analytics et public"

step "6/6 nettoyage (base jetable)"
docker exec "${PG}" psql -U postgres -v ON_ERROR_STOP=1 -q -c \
  "DROP DATABASE IF EXISTS ${TEST_DB} WITH (FORCE);"
ok "base ${TEST_DB} supprimée"

echo "[restore-test] SUCCÈS : restauration + état canonique prouvés"
