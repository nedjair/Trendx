# W110 — Execution History operational auditability

## 1. Baseline

W110 starts from the verified W109 commit:

```text
HEAD:  e6a1a3902dcd0acb9dc0f3321b9e1e94436022dd
parent: 2515d9d1f661165fe74fd97db80fee3725cf48bc
message: feat(forecasting): add execution history recovery api
```

The W99/W100 history store, W104 analytics, W106 reporting, W107 lifecycle,
W108 recovery and W109 recovery API remain separate.  Existing untracked WIP is
not an input to this change and is not removed.

## 2. Architecture

```text
execution / archive / purge / restore / recovery API
                         ↓
                ExecutionAuditService
                         ↓
                 ExecutionAuditStore
                         ↓
             versioned, checksummed events
```

`ExecutionRecord` is not extended.  Business history and operational audit
history have separate stores and separate read paths.  W110 does not call
W104/W106, MLflow, ThingsBoard, a scheduler, a worker, training or forecasting
code.

## 3. Audit contract

`ExecutionAuditEvent` contains only:

- `event_id`;
- `event_version`;
- UTC `occurred_at`;
- `operation`;
- `outcome`;
- `tenant_id`;
- nullable `execution_id` and `reference_key`;
- `actor_type` and `actor_id`;
- `request_id`;
- `source`;
- `reason_code`;
- deterministic `idempotency_key`;
- SHA-256 `checksum`.

There is no field for a credential, authorization header, request body,
archive content, active record payload, archive path, filesystem path or
traceback.  Untrusted free-form values are rejected or deterministically
hashed at the service boundary before persistence.

## 4. Event version

The only accepted version is `audit_event_version = "1"`.  Unknown versions
are rejected with a typed `AuditEventVersionError`; a query does not
silently reinterpret them.  No database migration is involved.

## 5. Actor model

The stable actor types are `SYSTEM`, `API`, `OPERATOR` and `TEST`.  W109 uses
`API` with the explicit actor id `api-key-context`, because the existing
authentication contract has no user or principal identity.  A credential is
never used as an actor id.

## 6. Request correlation

The API generates or accepts a strictly bounded `X-Request-ID`, stores it on
`request.state`, echoes it as `X-Request-ID`, and propagates it through a
context variable to W108.  W108 and W109 audit events therefore carry the same
request id.  Invalid or sensitive-looking inbound values are replaced with a
fresh generated id.

## 7. Persistence

`ExecutionAuditStore` is an independent directory configured by
`TRENDX_EXECUTION_AUDIT_PATH`.  Each event is one strict canonical JSON file.
Writers use a sibling `fcntl` lock, same-directory temporary files, file
`fsync`, atomic `os.replace` and directory `fsync`.  The store is process-safe
and thread-safe on the supported Linux host.  It is a local file backend, not
a database transaction or a signed ledger.

## 8. Tenant isolation

Every event has one tenant.  The HTTP audit query always injects the existing
`TenantContext` and never accepts a tenant as an authorization source.  A
forged tenant is `403`; an event or execution belonging to another tenant is
not returned.  Direct store queries can be explicitly tenant-scoped by callers.

## 9. Integrity

The canonical event payload is sorted compact UTF-8 JSON.  Its SHA-256 checksum
is stored beside it and verified on every read.  Malformed JSON, unknown
versions, missing fields, invalid permissions and checksum mismatches fail
closed.  The API converts these failures to a sanitized `503` response.

## 10. Idempotence

The default idempotency key is derived from operation, tenant, execution and
request id.  Replaying the same operation/request returns the existing event;
reusing the key for a different event is an explicit conflict.  No duplicate
event is silently written.

## 11. Archive audit

W107 emits sanitized `ARCHIVE` events for creation/reuse/failure and
`ARCHIVE_VERIFY` events for explicit verification outcomes when an audit
service is injected.  Events contain tenant, execution id, outcome, reason
and time, but never the archive path or archive payload.  Lifecycle business
behavior and archive-before-purge ordering are unchanged.

## 12. Purge audit

Final purge outcomes are represented by `PURGE` events with `SUCCESS`,
`ALREADY_PRESENT`, `REJECTED`, `CONFLICT` or `FAILED` as applicable.  Refused
and partially failed operations remain observable.  W110 does not add a
scheduler or automatic purge.

## 13. Restore audit

W108 emits a `RESTORE` event only after the restore result is known.  W108
statuses are mapped without inventing a new outcome taxonomy.  Archive
verification, tenant rejection, conflicts, integrity failures and backend
failures are recorded with deterministic reason codes.

## 14. W109 API audit

`POST /api/v1/forecast/executions/restore` emits a separate `RECOVERY_API`
event for authenticated tenant-scoped requests.  The event records the
request id, actor context, execution id, HTTP-derived outcome and reason.
Authentication failures without a trusted tenant context remain sanitized
runtime logs rather than tenant-attributed durable events.

## 15. Audit query

The optional read-only API is:

```text
GET /api/v1/forecast/executions/audit
GET /api/v1/forecast/executions/audit/statistics
```

Supported filters are `event_id`, `operation`, `outcome`, `execution_id`,
`reference_key`, `occurred_at_from`, `occurred_at_to`, `request_id`,
`actor_type`, `source`, `reason_code`, `limit` and `offset`.  Pagination is
bounded to 1000 events and a maximum offset of 10,000,000.  Unknown query
fields are rejected.  The API is authenticated and tenant-aware; it has no
write, delete, purge or scheduler route.

## 16. Audit statistics

Statistics are deterministic counters over the filtered audit set:

- `total_events`;
- `successful_events`;
- `failed_events`;
- `rejected_events`;
- `conflicts`;
- `forbidden`;
- `by_operation`;
- `by_outcome`;
- `by_tenant`.

They are not W104 execution analytics and do not read active history.

## 17. Read-only proof

Audit queries and statistics only read event documents.  They do not append,
rewrite, delete or purge events.  The W110 tests hash event files before and
after filtered queries, pagination and statistics.

## 18. Process A/B

A process that appends an event is followed by a separate Python process that
opens the same store and reads it without Python object reuse.  The operational
suite also uses two real Uvicorn processes sharing the store.

## 19. Concurrency

The audit store serializes writers across threads and processes.  Concurrent
audit appends do not create partial JSON.  Queries and statistics take a
coherent locked snapshot and cannot cross the tenant boundary.

## 20. Performance

The local tests exercise 10, 100, 500 and 1000-event query workloads and
concurrent appends.  Measurements are finite and non-negative; W110 does not
invent a latency SLA or add a new database/cache service.

## 21. Security

The audit contract rejects credentials, authorization values, raw payloads,
paths and traversal-like identifiers.  Event filenames are hashes rather than
client identifiers.  API errors and Loguru diagnostics contain fixed codes and
exception type names only.  Archive/history paths remain server-side
configuration.  Symlink roots and insecure event permissions fail closed.

## 22. Failure policy

The selected policy is **best effort**.  A successful W107/W108/W109 business
result is not rolled back because the audit append failed.  The failure is
visible as a sanitized `audit_write_failed` Loguru event.  A mandatory policy
is available for an explicitly provisioned deployment, but it is not the
W110 default and is tested as an explicit consequence.

## 23. Side effects

W110 introduces no business execution, training, forecast, worker, scheduler,
MLflow, ThingsBoard write, alarm or anomaly operation.  The existing W99/W104/
W106 active-history behavior is preserved.  Audit retention is not automated.

## 24. Regression

W110 tests are isolated in:

- `tests/unit/test_w110_execution_audit.py`;
- `tests/integration/test_w110_execution_audit_ops.py`.

The W98-W109 targeted regression, quality gates and full-suite comparison are
reported in the final W110 handoff.  Historical failures, if still present,
must remain unchanged.

## 25. Quality gates

Required gates are Ruff, Ruff format check, targeted mypy, pre-commit,
Bandit, Gitleaks, Typos, AST/syntax, compileall and `git diff --check`, plus
OpenAPI, authentication, tenant, persistence, integrity and sanitization tests.

## 26. Git

W110 is intended to be one local commit with the exact message:

```text
feat(forecasting): add execution history operational audit
```

No push, deployment, Docker, ThingsBoard or production action is part of W110.

## 27. Limitations

- The backend is a local file sidecar; it is not a distributed database,
  transactional outbox, cryptographic signature system or compliance ledger.
- The existing shared-token authentication has no user identity, claims or
  scopes; `api-key-context` is intentionally explicit.
- Deployment processes must share secure ownership/ACLs for the audit
  directory.  Different UIDs require a separately provisioned policy.
- Audit retention and purge policy remain a future concern; no automatic
  retention is enabled.
- An audit failure is observable but does not undo a completed business
  operation under the default best-effort policy.

## 28. Verdict

The final verdict is emitted only after the required tests, regression,
quality gates and Git parent checks pass:

```text
PASS-W110-EXECUTION-HISTORY-OPERATIONAL-AUDIT
```

No W111 work is started by this document.
