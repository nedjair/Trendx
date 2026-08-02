# Runbook — Rollback writeback ThingsBoard

**Version :** 1.0 — Phase 2 (dry-run uniquement)  
**Date :** 2026-08-02  
**Contexte :** B3 — Writeback AUTORISÉ SOUS CONDITIONS, Phase 2 = dry-run uniquement  
**Prérequis :** Runbook validé avant toute écriture effective vers ThingsBoard

---

## 1. Principe

Ce runbook documente la procédure complète de rollback des écritures ThingsBoard effectuées par Trendx. Il s'applique uniquement aux clés préfixées `_EPD_` (prédictions) ou `_ECD_` (champs calculés).

**Règle d'or :** Aucune écriture n'est autorisée sans runbook validé et checklist signée.

---

## 2. Table `writeback_log`

La table `writeback_log` dans le schéma `trendx_catalog` enregistre chaque opération de writeback :

```sql
CREATE TABLE trendx_catalog.writeback_log (
    job_id          UUID NOT NULL,
    device_id       VARCHAR(64) NOT NULL,
    entity_id       VARCHAR(64) NOT NULL,
    key             VARCHAR(128) NOT NULL,
    ts_start        TIMESTAMPTZ NOT NULL,
    ts_end          TIMESTAMPTZ NOT NULL,
    nb_points       INTEGER NOT NULL,
    written_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    operator        VARCHAR(128) NOT NULL,
    dry_run         BOOLEAN NOT NULL DEFAULT TRUE,
    PRIMARY KEY (job_id, key, ts_start)
);
```

**Champs obligatoires :**
- `job_id` : identifiant du job d'écriture
- `device_id` : device ThingsBoard
- `entity_id` : entité TB (device ou asset)
- `key` : clé télémétrique (doit commencer par `_EPD_` ou `_ECD_`)
- `ts_start`, `ts_end` : plage temporelle écrite
- `nb_points` : nombre de points écrits
- `written_at` : timestamp de l'écriture
- `operator` : identifiant de l'opérateur ayant validé
- `dry_run` : TRUE si simulation, FALSE si écriture réelle

---

## 3. Procédure de rollback

### 3.1 Identification des écritures à supprimer

```sql
SELECT job_id, device_id, entity_id, key, ts_start, ts_end, nb_points, written_at, operator, dry_run
FROM trendx_catalog.writeback_log
WHERE dry_run = FALSE
  AND key LIKE '_EPD_%' OR key LIKE '_ECD_%'
ORDER BY written_at DESC;
```

### 3.2 Validation pré-suppression

- [ ] Vérifier que `dry_run = FALSE`
- [ ] Vérifier que la clé commence bien par `_EPD_` ou `_ECD_`
- [ ] Vérifier que le `device_id` est dans `TB_WRITEBACK_DEVICE_ALLOWLIST`
- [ ] Vérifier que `TB_WRITEBACK_ENABLED = true`
- [ ] Vérifier que l'opérateur est habilité

**Refus automatique si l'une de ces conditions n'est pas remplie.**

### 3.3 Suppression par API ThingsBoard (paramétrée depuis le journal)

Utiliser la commande suivante pour chaque entrée du journal :

```bash
# Exemple de suppression d'une clé _EPD_ pour un device
curl -X DELETE \
  "${TB_API_URL}/api/plugins/telemetry/DEVICE/${DEVICE_ID}/keys/${KEY}" \
  -H "X-Authorization: Bearer ${TB_JWT_TOKEN}" \
  -H "Content-Type: application/json"
```

**Paramètres obligatoires :**
- `TB_API_URL` : URL de l'API ThingsBoard
- `DEVICE_ID` : extrait du journal (`device_id`)
- `KEY` : extrait du journal (`key`)
- `TB_JWT_TOKEN` : token d'authentification ThingsBoard (hors dépôt)

**Mode dry-run :** Ajouter `--dry-run` à la commande pour simuler sans exécuter.

### 3.4 Vérification post-suppression

```bash
# Vérifier que la clé n'existe plus
curl -X GET \
  "${TB_API_URL}/api/plugins/telemetry/DEVICE/${DEVICE_ID}/keys" \
  -H "X-Authorization: Bearer ${TB_JWT_TOKEN}" \
  -H "Content-Type: application/json" | jq '.'
```

**Checklist de vérification :**
- [ ] La clé n'apparaît plus dans la liste des clés du device
- [ ] Aucune erreur API ThingsBoard
- [ ] Logs Trendx sans erreur
- [ ] Table `writeback_log` marquée comme supprimée (ajouter colonne `deleted_at`)

---

## 4. Procédure de rollback complet

### 4.1 Arrêt du writeback

```bash
# Désactiver temporairement le writeback
export TB_WRITEBACK_ENABLED=false
docker compose restart worker
```

### 4.2 Suppression de toutes les clés _EPD_/_ECD_ pour le device de test

```bash
# Lister les clés à supprimer
curl -X GET \
  "${TB_API_URL}/api/plugins/telemetry/DEVICE/ALG16025001/keys" \
  -H "X-Authorization: Bearer ${TB_JWT_TOKEN}" \
  -H "Content-Type: application/json" | jq '.[] | select(. | startswith("_EPD_") or startswith("_ECD_"))'
```

### 4.3 Suppression par API

```bash
# Pour chaque clé identifiée
for KEY in $(curl -s ... | jq -r '.[] | select(. | startswith("_EPD_") or startswith("_ECD_"))'); do
  curl -X DELETE \
    "${TB_API_URL}/api/plugins/telemetry/DEVICE/ALG16025001/keys/${KEY}" \
    -H "X-Authorization: Bearer ${TB_JWT_TOKEN}"
done
```

### 4.4 Vérification post-suppression

```bash
# Vérifier qu'aucune clé _EPD_/_ECD_ ne subsiste
curl -X GET \
  "${TB_API_URL}/api/plugins/telemetry/DEVICE/ALG16025001/keys" \
  -H "X-Authorization: Bearer ${TB_JWT_TOKEN}" \
  -H "Content-Type: application/json" | jq '.[] | select(. | startswith("_EPD_") or startswith("_ECD_"))'
```

**Doit renvoyer un tableau vide.**

### 4.5 Nettoyage du journal

```sql
UPDATE trendx_catalog.writeback_log
SET deleted_at = NOW()
WHERE dry_run = FALSE
  AND device_id = 'ALG16025001'
  AND (key LIKE '_EPD_%' OR key LIKE '_ECD_%');
```

---

## 5. Checklist signée

**Avant toute écriture effective (dry_run = FALSE) :**

- [ ] Runbook validé par l'opérateur
- [ ] `TB_WRITEBACK_ENABLED = true`
- [ ] `TB_WRITEBACK_DEVICE_ALLOWLIST` contient le device
- [ ] Clé vérifiée : commence par `_EPD_` ou `_ECD_`
- [ ] Mode dry-run exécuté avec succès
- [ ] Vérification post-suppression réussie
- [ ] Backup base `trendx` effectué
- [ ] Opérateur habilité identifié
- [ ] Signature enregistrée dans `trendx_catalog.writeback_approval`

---

## 6. Refus automatique

Le writeback est REFUSÉ si :
- La clé ne commence pas par `_EPD_` ou `_ECD_`
- Le `device_id` n'est pas dans `TB_WRITEBACK_DEVICE_ALLOWLIST`
- `TB_WRITEBACK_ENABLED = false`
- Le runbook n'est pas validé
- La checklist n'est pas signée

---

## 7. Contact d'urgence

En cas de problème :
1. Activer `TB_WRITEBACK_ENABLED = false`
2. Exécuter la procédure de rollback complet (section 4)
3. Contacter l'équipe Trendx

---

## 8. Historique

| Version | Date | Auteur | Modification |
|---|---|---|---|
| 1.0 | 2026-08-02 | Trendx Agent | Création initiale |
