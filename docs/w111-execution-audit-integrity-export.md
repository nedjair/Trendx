# W111 — Execution audit integrity, reconciliation & controlled export

## 1. Baseline

```text
branch: fix/ml-w49-master-conflicts
HEAD:   5634d030dfcdf1c74a840830a0192ec66f0ae95a  (W110)
parent: e6a1a3902dcd0acb9dc0f3321b9e1e94436022dd  (W109)
```

Pre-existing untracked WIP (`.bridge/`, `.kilo/`, `.refact/`, `opencode.json`,
`remediation-report.md`, historical docs and scripts) was inventoried before any
mutation and is neither modified nor staged.

## 2. Architecture

```text
HTTP GET (read-only routes)
        ↓
ExecutionAuditService            (W110 auth + tenant + store policy)
        ↓
ExecutionAuditStore              (W110 storage, sanitation, checksum)
        ↓
ExecutionAuditControlService     (W111, read-only, no mutation primitive)
        ├── ExecutionAuditIntegrityService
        ├── ExecutionAuditReconciliationService
        └── ExecutionAuditExportService
```

W111 adds one module, `src/trendx/forecasting/audit_control.py`. It never opens
an audit document itself: every read goes through the W110 store through the new
read-only `scan_documents()` port, so the W110 sanitation, checksum, tenant and
idempotency contracts stay the single source of truth.

W111 does not modify `ExecutionRecord`, `ExecutionHistoryRecord`, the W107
`ArchiveStore`, the W108 `RecoveryService` or the W109 recovery API. The only
W110 change is additive: typed integrity sub-errors (all still
`AuditIntegrityError` subtypes), the `AuditDocumentRef`/`AuditDocumentScan`
read-only verdict types, `scan_documents()` and a shared `_build_audit_query`
helper so the W110 filter validation is reused instead of duplicated.

## 3. Integrity

`ExecutionAuditIntegrityService.verify(tenant_id)` returns an `IntegrityReport`
with `report_version`, `generated_at`, `tenant_id`, `documents_scanned`,
`events_scanned`, `valid_events`, `invalid_events`, `duplicate_events`,
`checksum_failures`, `schema_failures`, `version_failures`,
`canonicalization_failures`, `identity_failures`, `unsafe_documents`,
`unreadable_documents`, `malformed_documents`, `first_failure` and
`integrity_status` ∈ `VALID | INVALID | EMPTY | ERROR`.

Per-document failure codes are explicit and never expose a path or a file name:
`CHECKSUM`, `VERSION`, `MALFORMED_JSON`, `CANONICALIZATION`, `IDENTITY`,
`SCHEMA`, `UNSAFE_DOCUMENT`, `UNREADABLE`. A corrupt document is reported as
data (`INVALID`) instead of aborting the read, so an operator sees exactly what
failed. `document_ref` is a truncated SHA-256 of the internal document name, so
two independent scans can be correlated without disclosing a path.

Verification is deterministic: the same store state always yields the same
report and the same `first_failure`.

## 4. Reconciliation

`ExecutionAuditReconciliationService.reconcile(tenant_id)` returns a
`ReconciliationReport` with `checks_run`, `inconsistencies`, `warnings`,
`correlation_chains`, bounded `findings`, `first_failure` and
`reconciliation_status` ∈ `CONSISTENT | INCONSISTENT | INSUFFICIENT_DATA |
ERROR`.

Every rule is explicit and derived from the reason codes W107/W108/W109
actually write. A rule never invents a missing event: an undecidable case is a
warning, and the report degrades to `INSUFFICIENT_DATA`.

| Rule | Statement |
|---|---|
| R1 `IDENTITY_REQUIRED` | a completed archive/purge/restore event must carry `execution_id` and `tenant_id` |
| R2 `SUCCESS_WITH_NEGATIVE_REASON` | a `SUCCESS` outcome may not carry a refusal/failure reason |
| R3 `PURGE_SUCCESS_CONTRADICTS_REASON` | a purge success may not claim `dry_run`, `archive_required` or `record_changed` |
| R4 `RESTORE_SUCCESS_WITHOUT_EXECUTION` | a successful restore must identify what was restored |
| R5 `RECOVERY_API_*` | a W109 success must use a documented 200 reason, and a non-success may not |
| R6 `REQUEST_ID_MISSING` | every audit event keeps its correlation id |
| R7 `CORRELATION_ID_NOT_PRESERVED` / `CORRELATION_NOT_OBSERVABLE` | a restore correlated to a recovery-api event must share the `request_id`; when it is not observable it is a warning, never an invented failure |
| R8 `CHAIN_CLOCK_SKEW` | in a chain ordered `RECOVERY_API → RESTORE → ARCHIVE`, `occurred_at` must be non-decreasing (warning only) |
| R9 `DUPLICATE_LOGICAL_EVENT` | one idempotency key may not describe two different events |

`iter_chains()` returns a chain only when every link is present; an incomplete
chain yields nothing.

## 5. Controlled export

`ExecutionAuditExportService` exports `JSON` or `JSONL` only. It is read-only,
tenant-isolated, bounded (`limit` ≤ 1000, `offset` ≤ 10 000 000), paginated and
filterable through the reused W110 filters. The exported field set is exactly
the W110 durable event contract: no credential, authorization header, API key,
password, secret, traceback, environment, request body, archive path, lock path
or internal file name.

The control plane always evaluates the **complete** filtered match set and only
then applies the requested page window. A truncated internal read would make
reconciliation emit false "not observable" warnings for a correlated event that
simply sat beyond the first page, and would understate the export `total`. The
wire bound still limits each response, and the W110 store already materialises
the trail in memory for a single read, so this adds no new resource profile.

`assert_export_safe()` is a hard defence-in-depth invariant: a path-like,
secret-like, oversized or null-byte value **raises** rather than being silently
redacted, so a leak attempt fails closed.

## 6. Determinism and export checksum

* contractual order: `occurred_at` ASC, then `event_id` ASC;
* `export_checksum = SHA-256(canonical_export)` — never MD5;
* `generated_at` is transport metadata and is **excluded** from the checksummed
  bytes, so two processes reading the same state are byte-identical;
* JSON produces one canonical envelope plus a trailing newline; JSONL produces
  one canonical event per line, each newline-terminated;
* the same dataset and filters always reproduce the same checksum, and adding
  one event changes it.

The HTTP `content` field is the exact canonical byte sequence covered by
`export_checksum`, so a client can verify
`sha256(content.encode()) == export_checksum`.

## 7. API

Only read-only routes were added:

```text
GET /api/v1/forecast/executions/audit/integrity
GET /api/v1/forecast/executions/audit/reconciliation
GET /api/v1/forecast/executions/audit/export
GET /api/v1/forecast/executions/audit/export/checksum
```

There is no `POST`, `PUT`, `PATCH` or `DELETE` on any audit path (asserted in
unit and operational tests). Each route documents `200/401/403/422/503` and
reuses the W110 `BearerAuth`/`ApiKeyAuth` schemes and the `X-Request-ID`
correlation header.

Filters are the W110 contract, reused verbatim: `tenant_id`, `event_id`,
`operation`, `outcome`, `execution_id`, `reference_key`, `request_id`,
`actor_type`, `source`, `reason_code`, `occurred_at_from`, `occurred_at_to`,
`limit`, `offset`. The export routes additionally accept `format`.

## 8. Authentication and tenant isolation

W111 adds no authentication. Every route depends on the W110
`get_execution_audit_service` dependency, so it inherits the existing middleware
and the W110 `audit_authentication_not_configured` / `audit_service_unavailable`
policy. `TenantContextDep` pins the tenant, and a requested `tenant_id` that
differs from the authenticated tenant is rejected with `403` **before** the
backend runs. Events of another tenant are never counted, named, exported or
checksummed.

## 9. Read-only proof

`ExecutionAuditControlService` exposes no `append`, `record`, `delete`, `purge`,
`write` or `update` member, so a W111 caller cannot write by construction. The
unit tests wrap the store in a proxy that records any `append` call and assert
zero calls plus an identical before/after SHA-256 fingerprint of every audit
file; the operational tests do the same around a real Uvicorn server.

## 10. Error sanitization

| Situation | Behaviour |
|---|---|
| no/invalid credentials | `401` |
| placeholder auth configuration | `503 audit_authentication_not_configured` |
| tenant mismatch | `403 tenant_forbidden` |
| unknown field, bad bound, unsupported format | `422 invalid_audit_query` |
| store unavailable, unsafe or unreadable | `503 audit_service_unavailable` |
| corrupt document (integrity route) | `200` with `integrity_status=INVALID` |
| corrupt document (reconciliation/export/checksum) | `503`, refusing to answer from untrusted data |

No response or log contains a traceback, an absolute path, an archive path, a
lock path, an internal file name, a secret, a token or a credential.

## 11. Concurrency

Threads: 16 concurrent read-only control-plane calls in-process.
Processes: two real Uvicorn servers on the same store plus 12 interleaved
requests, and two independent `python -c` exporters. All observe the same
integrity status, the same reconciliation status and byte-identical export
checksums. No mutation is introduced.

## 12. Performance (observed, no SLA defined)

Measured with `time.perf_counter`; W111 deliberately defines no threshold.

| events | integrity | reconciliation | export | checksum |
|---:|---:|---:|---:|---:|
| 10 | 0.0026 s | 0.0023 s | 0.0020 s | 0.0030 s |
| 100 | 0.0222 s | 0.0210 s | 0.0193 s | 0.0278 s |
| 500 | 0.1044 s | 0.0969 s | 0.0978 s | 0.1392 s |
| 1000 | 0.2182 s | 0.1997 s | 0.1957 s | 0.2779 s |

## 13. Side effects

W111 adds no scheduler, worker, ingest, forecast, training, MLflow, ThingsBoard
writeback, alarm, PostgreSQL, Redis, Kafka, queue, SIEM, retention or purge. The
operational suite runs with
`TRENDX_SCHEDULER_FORECAST_ENABLED=false`, `TRENDX_INGEST_ENABLED=false`,
`TRENDX_WORKER_INGESTION_ENABLED=false`, `TRENDX_WORKER_FORECAST_ENABLED=false`,
`TB_WRITEBACK_ENABLED=false`, `TB_ALARMS_ENABLED=false`,
`ANOMALY_DETECTION_ENABLED=false` and `TRENDX_MLFLOW_ENABLED=false`, and asserts
no MLflow artefact and no forecast/training/writeback/alarm log marker.

The control plane never records an audit event about itself, so the
`audit → audit → audit` recursion cannot occur. `AUDIT_CONTROL_SOURCE` documents
the reserved source name if a host ever decides to audit W111 reads.

## 14. Limitations

* No automatic retention or purge: W111 never deletes anything, and the audit
  store keeps growing until an external, separately reviewed policy prunes it.
* A corrupt document fails reconciliation and export closed (`503`) on purpose;
  integrity must be consulted first to learn why.
* Store-level corruption counters are global by design: they never reveal which
  tenant owns the faulty document, but a tenant-scoped report may show a
  non-zero store counter caused by another tenant's document. Event-level data
  is always tenant-isolated.
* W111 is single-host, matching the W110 file store; it adds no replication and
  no cryptographic signature (only SHA-256 content checksums).
* `ReconciliationReport.findings` is bounded to 200 entries; counters always
  report the true total.
* The read path materialises the full filtered trail, as the W110 store already
  does. On a very large store that is the dominant cost; W111 defines no SLA and
  publishes only the observed durations in section 12.
