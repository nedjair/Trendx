# W109 — Authenticated execution-history recovery API

## Scope

W109 exposes one explicit, authenticated HTTP operation:

```text
POST /api/v1/forecast/executions/restore
```

The route is a thin adapter around the W108 `RecoveryService`.  It does not
parse archive files, access an archive path supplied by a client, duplicate
restore logic, or call a forecasting/business service.

There is no GET, PUT, PATCH or DELETE restore route, no purge route, no
automatic recovery, and no recovery scheduler.

## Request

The strict JSON body is:

```json
{
  "execution_id": "execution-123",
  "tenant_id": "tenant-A",
  "expected_archive_checksum": "<64 lowercase SHA-256 characters>",
  "conflict_policy": "FAIL_IF_EXISTS"
}
```

`archive_path` and all other unknown body or query fields are rejected.
Null bytes, blank identifiers and oversized identifiers are rejected.
`conflict_policy` is required and only `FAIL_IF_EXISTS` is accepted.

A restore request is rejected closed with `503` when the existing API token is
empty or still a documented placeholder; no backend is constructed.

The tenant in the body is an assertion, not an authorization source.  The
route compares it with the existing `TenantContext` dependency.  The existing
API middleware authenticates all `/api/v1/*` routes using the existing
Bearer/API-key mechanism.  OpenAPI documents those two existing alternatives;
W109 does not create a new credential system.

The server obtains both stores from application configuration:

- `TRENDX_EXECUTION_HISTORY_PATH`
- `TRENDX_EXECUTION_ARCHIVE_PATH`

The client cannot choose either path.

## Response and HTTP mapping

Successful domain outcomes use the W108 result contract:

- `200` — `RESTORED`
- `200` — `ALREADY_PRESENT`
- `409` — `CONFLICT`
- `403` — `TENANT_FORBIDDEN`
- `404` — `ARCHIVE_NOT_FOUND`
- `422` — `INVALID_ARCHIVE` or `INTEGRITY_FAILURE`
- `503` — `RESTORE_FAILED`, an invalid verify-only backend result, or recovery
  backend unavailable

Authentication failures are handled by the existing middleware and return
`401`.  Request validation and pre-service authorization/configuration errors
use the fixed `{"detail": ...}` error shape.  Domain outcomes use
`ExecutionRestoreResultOut`; OpenAPI represents the 403/422/503 cases that can
contain either shape as an explicit union.  Filesystem-looking identifiers are
redacted at the response boundary.

The JSON response contains the typed status/outcome, verification flags,
conflict/presence flags, deterministic reason and non-invasive restore
metadata.  It never contains an archive path, raw archive payload, traceback,
credential, token, or internal exception.

## Recovery flow

```text
existing auth middleware
        ↓
existing TenantContext
        ↓
strict HTTP request model
        ↓
application-wired RecoveryService
        ↓
W108 ArchiveStore verification
        ↓
W108 atomic ExecutionStore restore
        ↓
W99 / W104 / W106 read-only views
```

The API does not use `ExecutionHistoryService` to perform a restore.  W99,
W104 and W106 continue to read only active history after the W108 operation
has completed.

## Safety and concurrency

- W108 identity, checksum, tenant, archive immutability and conflict rules
  remain authoritative.
- `FAIL_IF_EXISTS` is the only public policy.
- Two concurrent POSTs produce one effective insertion and one
  `ALREADY_PRESENT` result.
- The archive is never modified by the API.
- GET requests remain read-only and cannot trigger recovery.
- The server rejects symlink history/archive roots and opens the active
  history in non-creating mode; a missing datastore is never created by a
  request.
- The literal path segment `restore` is reserved for this POST operation;
  `GET`/`HEAD` cannot dispatch it to the generic execution lookup.

## Observability

The existing Loguru integration records fixed fields such as:

```text
operation=restore outcome=success status_code=200
operation=restore outcome=already_present status_code=200
operation=restore outcome=conflict status_code=409
operation=restore outcome=tenant_forbidden status_code=403
operation=restore outcome=archive_not_found status_code=404
```

No authorization header, token, archive path, complete record, payload, or
traceback is logged.

## Operational validation

W109 tests cover:

- TestClient contract tests;
- real Uvicorn and HTTPX requests;
- valid, missing and invalid authentication;
- tenant and archive-tenant isolation;
- success, idempotence and conflicts;
- W99/W104/W106 visibility after HTTP restore;
- OpenAPI and HTTP status contracts;
- malformed, oversized and unknown-field requests;
- no arbitrary archive path;
- concurrent requests and two Uvicorn processes sharing one durable store;
- 10, 100 and 500 archive restore measurements without an invented SLA.

All tests use temporary local archives and stores.  Scheduler, worker,
ingestion, training, forecasting, MLflow, ThingsBoard, writeback, alarm and
anomaly paths are not invoked.

## Limitations

- The existing Trendx API token model is reused; W109 does not introduce
  claims, scopes, a new JWT, a new API key or a new user store.
- The current tenant context remains the existing configured tenant context;
  W109 does not create a new tenant identity system.
- W107's archive ownership checks remain in force.  The operational process
  test uses one effective UID; a deployment with API and worker under
  different UIDs must provision a shared ownership/ACL policy separately.
- A persistent recovery audit table is not added.  The W108 operation result
  and existing observability events are the audit surfaces.
- Recovery remains explicitly operator/API driven; no automatic restore is
  introduced.
