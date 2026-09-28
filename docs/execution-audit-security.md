# Sécurité de la chaîne d'exécution et d'audit (W99–W115)

Ce document est la référence de sécurité de la chaîne d'exécution et d'audit.
Il ne décrit que ce qui est implémenté et vérifié sur `d9a01d7`.

## Les trois plans sont distincts

Il faut séparer trois notions, souvent confondues :

| Plan | Question | Implémentation | Échec |
|---|---|---|---|
| **Authentification** | « qui appelle ? » | `require_auth_middleware` | `401` |
| **Autorisation / tenant** | « a-t-il le droit ? » | contexte de tenant injecté | `403` |
| **Existence de l'objet** | « l'objet existe-t-il ? » | recherche | `404` |

Un objet **absent** ne doit pas être révélé à un tenant qui n'y a
pas droit : c'est pourquoi le contrôle de tenant précède la recherche, et rend
`403` avant `404`.

## Authentification

Middleware applicatif `require_auth_middleware`, dans `src/trendx/main.py` :

- s'applique à **tout** chemin préfixé `/api/v1` ;
- accepte `X-API-Key: <token>` **ou** `Authorization: Bearer <token>` ;
- compare en **temps constant** (`hmac.compare_digest`) ;
- n'expose que six chemins publics :
  `/`, `/health`, `/metrics`, `/docs`, `/redoc`, `/openapi.json`.

Le token attendu est `settings.trendx_api_token`.

### Statuts

| Situation | Statut | Corps |
|---|---|---|
| aucun credential | `401` | `{"detail": "Missing authentication credentials"}` |
| credential invalide | `401` | `{"detail": "Invalid authentication credentials"}` |

### Déclaration OpenAPI

Toutes les routes de la chaîne déclarent le même contrat de sécurité :

```json
"security": [{"BearerAuth": []}, {"ApiKeyAuth": []}]
```

avec `securitySchemes` :

```json
{"BearerAuth": {"type": "http", "scheme": "bearer"},
 "ApiKeyAuth": {"type": "apiKey", "in": "header", "name": "X-API-Key"}}
```

> **Historique.** Jusqu'au commit `49432f0`, les sept routes de lecture
> d'exécution déclaraient `security: null` alors qu'elles renvoyaient déjà `401`
> et qu'elles étaient déjà protégées. Ce décalage a été corrigé en `d9a01d7`
> (finding **F-01** de W115) : seule la déclaration a changé, **aucun
> comportement d'exécution**.

La **source de vérité** pour l'API reste l'OpenAPI généré par l'application
(`GET /openapi.json`), pas ce document.

## Contexte de tenant

Le modèle est **mono-tenant par déploiement** : le tenant vient de
`TRENDX_DEFAULT_TENANT_ID`, résolu par `get_tenant_context()`.

- Si le setting est vide ou ne contient que des blancs, la requête est
  refusée en **`403 tenant_context_unavailable`**. Le système **fail-closed** :
  aucun tenant implicite, aucun défaut silencieux.
- Si le client fournit `?tenant_id=` différent du contexte, la requête est
  refusée en **`403 tenant_forbidden`**.
- Le `tenant_id` des lectures est **toujours** surchargé par le contexte. Un
  client ne peut pas élargir sa portée.

## Sanitisation des réponses

Aucune réponse d'erreur ni de succès de la chaîne ne doit contenir :

| Interdit | Vérifié par |
|---|---|
| chemin de système de fichiers | motif `/tmp/...` et absolu |
| traceback | `Traceback (most recent call last)`, `File "...", line N` |
| URL de base de données | `postgresql://` |
| mot de passe | affectation `password=` |
| token Bearer | `Bearer <8+ chars>` |
| clé d'API | `X-API-Key: <valeur>` |
| SQL | `SELECT ... FROM` |
| détail de pilote | `psycopg2`, `sqlalchemy.exc` |
| chemin d'installation | `site-packages` |
| `repr(exception)` | `Exception:` |

Deux mécanismes coopèrent :

1. **`_redact_json`** — à la frontière API, remplace les clés de type credential
   par `[REDACTED]`, sans toucher aux données métier.
2. **Codes d'erreur stables** — une exception de store devient un `503` au code
   connu, jamais `repr(exc)`.

Le même contrat vaut pour l'export d'audit W111 : `assert_export_safe()` **lève**
`AuditControlError` au lieu de redacter, pour qu'une tentative de fuite échoue
fermement plutôt que de livrer un export partiellement expurgé.

## Limites de la sécurité de cette chaîne

À connaître explicitement :

- **Token unique partagé.** `TRENDX_API_TOKEN` est un secret unique, sans
  portée, sans rotation par tenant et sans révocation par appelant. Il n'y a pas
  d'identité d'utilisateur dans cette chaîne.
- **Mono-tenant par déploiement.** L'isolation est prouvée par le refus du
  `tenant_id` divergent, **pas** par un mécanisme multi-tenant à l'exécution.
- **Pas d'authentification sur `/health`, `/metrics`, `/docs`, `/redoc`,
  `/openapi.json`.** `/docs` et `/openapi.json` exposent donc la surface
  complète de l'API publiquement. À protéger au niveau réseau si le déploiement
  l'exige.
- **Redaction par convention.** `_redact_json` s'appuie sur des noms de clés
  conventionnels ; il ne classifie pas des données métier arbitraires.
