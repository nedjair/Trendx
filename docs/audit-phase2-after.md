# Audit Phase 2 — État final

**Date :** 2026-08-02  
**Opérateur :** Kilo  
**Séquence :** Étape 12 — snapshot après modifications

---

## 1. État disque (après)

```
Sys. de fichiers Taille Utilisé Dispo Uti% Monté sur
/dev/sdb2          1,1T    681G  362G  66% /
```

**Espace libre :** 362 GB  
**Seuil minimal (TRENDX_DISK_MIN_FREE_GB) :** 50 GB  
**Marge :** 312 GB  
**Consommation Trendx :** ~14 GB (images + containers + données)

---

## 2. Conteneurs Docker (après)

| Conteneur | Image | État | Santé | Créé le |
|---|---|---|---|---|
| trendx_reverse_proxy | trendx/reverse-proxy:latest | running | healthy | 2026-08-02 13:45:14 |
| trendx_api | trendx/api:latest | running | healthy | 2026-08-02 14:00:10 |
| trendx_worker | trendx/worker:latest | running | healthy | 2026-08-02 14:17:28 |
| thingsboard_thingsboard-ce_1 | thingsboard/tb-node:4.3.1.1-mobilis | running | — | 2026-06-15 16:27:16 |
| mobili_dahsboard-postgres-1 | postgres:16 | running | healthy | 2026-06-01 12:05:40 |

**Conteneurs Trendx :** 3 (reverse-proxy, api, worker)

---

## 3. Base de données ThingsBoard (après)

| Base | Propriétaire | Tables |
|---|---|---|
| thingsboard | postgres | Tables ThingsBoard CE |
| trendx | postgres | 58 tables Trendz |
| trendx_airflow | trendx_airflow | (existe, hors périmètre) |
| trendx_analytics | trendx_migration | (existe, hors périmètre) |
| trendx_mlflow | trendx_mlflow | (existe, hors périmètre) |

**Rôles PostgreSQL :**
- postgres (superuser)
- trendx_ro (nouveau, read-only, CONNECTION LIMIT 5)
- trendx_app (lecture/écriture trendx)
- trendx_migration (migrations)
- trendx_airflow, trendx_mlflow (hors périmètre)

**max_connections :** 100  
**Connexions actives :** 22 (inchangé)

---

## 4. Réseau Docker (après)

| Réseau | État |
|---|---|
| mobili_dahsboard_default | existe (external, non modifié) |
| trendx_internal | existe |
| trendx_edge | existe |

---

## 5. Compose utilisé

**Fichier :** /opt/trendx/docker-compose.trendx.yml  
**Projet :** trendx  
**Services :** reverse-proxy, api, worker

---

## 6. Points d'attention

1. Base `trendx` créée avec 58 tables (schéma Trendz)
2. Rôle `trendx_ro` créé avec CONNECTION LIMIT 5, read-only
3. Images Trendx construites : ~6 GB total
4. Worker fonctionne en mode APScheduler process, sans broker
5. Espace disque suffisant (362 GB libres)
6. ThingsBoard non impacté (conteneurs et base inchangés)

---

## 7. Vérifications doctor (après)

```
│  .env permissions (600 attendu) : OK
│  Compose file = docker-compose.trendx.yml ? : OK
│  Réseau mobili_dahsboard_default existe ? : OK
│  Port publié (seul 127.0.0.1:8443 autorisé) ? : OK
│  trendx_ro ne peut pas INSERT dans thingsboard ? : OK (INSERT refusé)
│  Ressources reverse-proxy/api/worker : OK
│  Espace libre / >= 50 GB ? : OK (361 GB)
│  Espace libre /var/lib/docker >= 50 GB ? : OK (361 GB)
```

**Tous les contrôles sont verts.**

---

## 8. Comparaison avant/après

| Métrique | Avant | Après | Δ |
|---|---|---|---|
| Espace libre /dev/sdb2 | 376 GB | 362 GB | -14 GB |
| Conteneurs Trendx | 0 | 3 | +3 |
| Images Docker Trendx | 0 | 3 | +3 |
| Bases Trendx | 1 (déjà existante) | 1 | 0 |
| Rôles trendx_* | 4 | 5 | +1 (trendx_ro) |
| Connexions PG actives | 22 | 22 | 0 |
| Conteneurs TB/PG | Inchangés | Inchangés | 0 |

---

## 9. Conclusion

Phase 2 infrastructure déployée avec succès.  
Tous les contrôles sont verts.  
ThingsBoard non impacté.  
Prêt pour Phase 3 (ingestion, entraînement, prévision).

---

*Fin du snapshot final.*
