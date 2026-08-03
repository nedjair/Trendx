# API Trendx — Endpoints documentés

## Authentification

Tous les endpoints `/api/v1/*` exigent un jeton Bearer ou `X-API-Key`.

```bash
curl -H "Authorization: Bearer ${TRENDX_API_TOKEN}" http://127.0.0.1:8443/api/v1/...
```

Le jeton attendu est la valeur de `TRENDX_API_TOKEN` dans `.env`. Il ne doit
jamais être écrit en clair dans un fichier, une documentation, un log, un
compte rendu ou une sortie de terminal : on référence toujours la variable
d'environnement `${TRENDX_API_TOKEN}`.

Endpoints publics : `/health`, `/metrics`, `/docs`, `/redoc`, `/openapi.json`.

---

## Ingestion

### Déclencher l'ingestion incrémentale

L'ingestion est **verrouillée par défaut**. Le déclenchement n'est accepté
(code 202) que si les deux conditions suivantes sont remplies dans `.env` :

- `TRENDX_INGEST_ENABLED=true`
- `TB_AUTH_CONFIGURED=true`

Sinon l'API répond **409** et journalise le refus avec son déclencheur.

```bash
curl -X POST http://127.0.0.1:8443/api/v1/ingestion/trigger \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer ${TRENDX_API_TOKEN}"
```

Réponse 202 :
```json
{
  "status": "triggered",
  "message": "Ingestion scheduled",
  "task_id": "<uuid>"
}
```

Réponse 409 (verrou fermé) :
```json
{
  "detail": "Ingestion verrouillée : TRENDX_INGEST_ENABLED doit être true"
}
```

L'ingestion est limitée aux devices découverts dans `trendx_catalog.business_entity` avec leurs métriques définies dans `trendx_catalog.metric_definition`. Les points de reprise sont persistés dans `trendx_catalog.ingestion_checkpoints`.

### Arrêt / reprise volontaire

Pour interrompre une ingestion en cours, supprimer le conteneur worker (le checkpoint persiste) :
```bash
docker compose -f /opt/trendx/docker-compose.trendx.yml -p trendx stop worker
```

Relance :
```bash
docker compose -f /opt/trendx/docker-compose.trendx.yml -p trendx start worker
```

L'ingestion reprend depuis le dernier watermark par device/metric.

---

## Endpoints de santé

### Health check (public)

```bash
curl -s http://127.0.0.1:8443/health
```

Réponse 200 :
```json
{
  "status": "ok",
  "service": "trendx-api",
  "detail": null
}
```

Si l'espace disque disponible est sous le seuil, le statut passe à `degraded` (503) et le détail contient l'erreur.

### Metrics (public)

```bash
curl -s http://127.0.0.1:8443/metrics
```
