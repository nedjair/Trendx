# W113 — Execution audit operational health, capacity & archive readiness

> **W113 is diagnostic and read-only.** It reads, aggregates, diagnoses and
> exposes. It never archives, purges, restores, deletes or otherwise mutates an
> audit event, and it never writes an audit event of its own. There is no
> automatic retention, no automatic purge and no automatic remediation anywhere
> in this phase.

## 1. Baseline

```text
branch: fix/ml-w49-master-conflicts
HEAD:   545f3f1ede3e6675363fcfa9dd8336dc769c56ee  (W112)
parent: 90a25eeca63f9d6d245e9103e6e98e4e64a970cd  (W111)
```

21 pre-existing untracked WIP entries were inventoried before any mutation and
are neither modified, staged nor removed by W113.

## 2. Architecture

```text
ExecutionAuditStore
        ↓  scan_documents()            (W110/W111 per-document verdicts)
ExecutionAuditService
        ↓
W111 integrity / reconciliation      (consumed, never re-implemented)
        ↓
W112 archive lifecycle               (AuditArchiveStore, preview())
        ↓
W113 AuditHealthService              (read-only projection)
        ↓
GET /audit/{health,capacity,readiness}
```

W113 is a **projection layer**, not a data owner. It reuses the W110 store
verdicts, the W112 archive store and the W112 backlog preview. It does not
re-implement W110 serialization, W111 checksums or W112 integrity, and it does
not create a second source of truth.

### One coherent snapshot

`AuditHealthService.snapshot(tenant_id, policy=None)` reads the active store
**once** and derives `health`, `capacity`, `archive`, `backlog` and `readiness`
from that single object. A caller needing several projections never triggers
several scans; the three endpoints are three views of the same computation.
The archive listing and the W112 backlog preview are each read at most once per
snapshot and are genuinely different data sources.

No cache exists (§23: by default, no cache). No metric is retained between
requests, so two reads of an unchanged datastore are guaranteed to agree.

## 3. Audit health

`GET /api/v1/forecast/executions/audit/health` → `AuditOperationalStatus`:

| Status | Meaning |
|---|---|
| `HEALTHY` | The tenant journal was scanned and every document is valid. |
| `DEGRADED` | The journal is readable but at least one document is invalid (checksum, schema, version, canonicalization, identity, permission, malformed). |
| `UNAVAILABLE` | The store could not be read at all. |
| `EMPTY` | No valid event exists for this tenant. |

`EMPTY` is a documented fourth state, not an invented score. An empty journal is
**never** silently promoted to `HEALTHY`, and it is **never** `NOT_READY`
(see §12).

Counters exposed: `event_count`, `valid_event_count`,
`integrity_invalid_count`, `malformed_count`, `version_failure_count`,
`checksum_failure_count`, `identity_failure_count`, `unsafe_document_count`,
`schema_failure_count`, `canonicalization_failure_count`, `tenant_count`,
`success_count`, `failure_count`, `oldest_event_at`, `newest_event_at`,
`last_observed_event_at`, `checked_at`.

Every counter is a count of verdicts that W110/W111 already produced. A value is
never estimated, and a document that cannot be classified is reported as
`UNREADABLE` rather than dropped.

`tenant_count` is the store-wide count of distinct tenants and is an observable
fact, not a leak: it exposes no identifier and no event of another tenant.

## 4. Capacity

`GET /api/v1/forecast/executions/audit/capacity` reports `event_count`,
`active_bytes`, `active_file_count`, `archive_bytes`, `archive_file_count`,
`archived_event_count`, `total_known_event_count`, active/archived age bounds,
`current_count`, `current_size_bytes` and filesystem metrics.

### Active vs archive

An **active** event is a document currently in the W110 store. An **archived**
event is a document in the W112 archive. W112 `archive()` *copies* an event and
does not purge it, so an archived-but-not-purged event is present on **both**
sides. `total_known_event_count` is therefore the **union of identifiers** on
both sides, never the sum: an event that exists in the active store and in the
archive counts once.

### Byte metrics are a stat-only observation

No abstraction in the W110/W111/W112 chain exposes on-disk size, and W113 must
not read audit content. `directory_bytes()` therefore walks the two directories
with `os.stat` **only**: no file is opened, read or decoded, so it cannot become
a second source of truth for audit content. It returns numbers only — never a
path, a file name, content or a credential — and a symlinked or unreadable
directory is reported as a measurement gap rather than as `0`.

`filesystem_metrics()` uses `os.statvfs` on a **directory** and reports
`filesystem_free_bytes`, `filesystem_total_bytes`, `filesystem_used_bytes`,
`inode_free` and `inode_total`. On a platform where these are not available the
result is `available=False` with `detail="filesystem_metrics_unavailable"`. No
threshold is applied to these numbers: W113 observes, it does not grade.

`capacity_measurement_complete` is `False` (with the informational reason code
`CAPACITY_MEASUREMENT_INCOMPLETE`) whenever a *configured* source could not be
observed. A metric that was never configured is simply `None` and is not a
failure.

## 5. Growth

W113 persists **no** historical snapshot, so it can never report an invented
`events_per_day`. The projection exposes the observed facts only:
`current_count`, `current_size_bytes`, `current_oldest_event_at`,
`current_newest_event_at`.

`observed_growth` and `observation_window` are `None` unless a previous
observation is actually supplied. A growth figure, when present, is an
observation over a stated interval — never a predictive projection. W113 is not
a forecasting component.

## 6. Archive health and backlog

`archive_status` is one of `HEALTHY`, `DEGRADED`, `CORRUPTED`, `UNAVAILABLE`,
`NOT_CONFIGURED`. Counters: `archive_document_count`, `archived_event_count`,
`valid_archive_count`, `invalid_archive_count`, `checksum_failure_count`,
`malformed_archive_count`, `tenant_mismatch_count`,
`oldest_archived_event_at`, `newest_archived_event_at`, `last_verified_at`.

W113 reads the archive **through** the W112 store and never opens an archived
file itself. The W112 store is fail-closed: `list_documents()` raises on the
first corrupt document, and the ids of a corrupt archive are not knowable
without bypassing that contract, which is forbidden. So when enumeration fails
the report is explicit rather than invented:

* `archive_status = CORRUPTED` with `ARCHIVE_CORRUPTED`;
* `enumeration_complete = False`;
* `archive_document_count`, `valid_archive_count`, `malformed_archive_count` are
  `None` (**unavailable**, not zero);
* `invalid_archive_count` and `checksum_failure_count` are `1` — the *observed
  minimum* implied by the failure that was actually seen.

When enumeration succeeds, every returned document is valid by construction of
the W112 verification, so `valid_archive_count = archive_document_count` and the
failure counters are `0`.

`NOT_CONFIGURED` (no W112 archive wired) and an empty archive are **not**
operational failures: readiness stays `READY`.

### Backlog vs protection

`eligible_event_count` comes from the W112 `preview()` under a policy supplied
by the caller, so the W112 policy stays the single authority on eligibility.
`protected_event_count` is what `minimum_events_to_keep` shields, and
`minimum_events_protected_count` echoes that setting. A backlog is a
**diagnostic**: it is never converted into an `archive()`, `purge()` or
`restore()` call, and the read-only proof asserts exactly that.

## 7. Readiness

`GET /api/v1/forecast/executions/audit/readiness` → `READY` or `NOT_READY` with
stable reason codes. Blocking codes (any one ⇒ `NOT_READY`):

```text
AUDIT_STORE_UNAVAILABLE   the journal could not be read
AUDIT_SCHEMA_INVALID      a document declares an unknown schema or version
AUDIT_DATA_CORRUPTED      a document is malformed, tampered or unsafe
ARCHIVE_UNAVAILABLE       a configured archive could not be read
ARCHIVE_CORRUPTED         a configured archive failed W112 verification
TENANT_CONTEXT_INVALID    the tenant identifier is not usable
INTEGRITY_CHECK_FAILED    the integrity read failed
```

Informational codes (never blocking): `AUDIT_JOURNAL_EMPTY`,
`ARCHIVE_NOT_CONFIGURED`, `CAPACITY_MEASUREMENT_INCOMPLETE`.

A `DEGRADED` answer is **not** an HTTP error: the endpoints return `200` with the
verdict in the body. HTTP status is reserved for the real failure modes of the
diagnostic surface itself (`401`, `403`, `422`, `503`).

`checks_run` counts the checks that were **actually evaluated** — store read,
document verdicts, archive enumeration and capacity observation, plus one more
when a policy supplied a backlog. It is derived, not a constant.

## 8. Liveness vs readiness

Liveness (`GET /health`) answers only "does the component respond?" and is
untouched by W113. Readiness answers "can the audit component serve correct
diagnostics?" A journal with **no** event is `EMPTY` yet `READY`: absence of data
is a legitimate operational state, whereas corruption is not. A degraded or
corrupt journal is `NOT_READY` with reason codes, which is a diagnostic verdict
delivered over `200`.

## 9. Tenant isolation

Every projection is computed under the `TenantContext` of the request, reusing
the existing authentication and tenant resolution. No new authentication
mechanism is introduced. No endpoint accepts a tenant parameter, so a caller
cannot ask for another tenant's view. No global administrative aggregate is
exposed, and a `tenant_id` is returned only for the caller's own tenant.

## 10. API

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/api/v1/forecast/executions/audit/health` | active journal health |
| `GET` | `/api/v1/forecast/executions/audit/capacity` | capacity snapshot |
| `GET` | `/api/v1/forecast/executions/audit/readiness` | readiness verdict |

All three are `GET`, read-only, reuse the existing auth, carry
`security: [{BearerAuth}, {ApiKeyAuth}]` and document `200`, `401`, `403`, `422`
and `503`. A consolidated `/audit/operational-status` route is deliberately
**not** added: the three projections already come from one snapshot, so a fourth
route would be redundant rather than additive.

Errors are stable and sanitized:

```json
{"detail": {"code": "audit_health_unavailable", "message": "..."}}
```

No `repr(exception)`, no traceback, no filesystem path, no token and no archive
location is ever returned or logged.

## 11. Observability and recursion

Requests reuse the W103/W110/W111 observation pattern:

```text
audit_health operation=health outcome=health_HEALTHY status_code=200
audit_health operation=capacity outcome=capacity_True status_code=200
audit_health operation=readiness outcome=readiness_READY status_code=200
```

W113 writes **no** audit event, so the recursion
`GET health → event → count → event` cannot occur: the event count a read
reports is exactly the count a subsequent read observes, which the tests assert
over 20 consecutive reads. The W112 recursion guard is not needed because no
audit write is attempted; the guarantee here is structural, not conditional.

## 12. Determinism

For an unchanged datastore, `health(A) == health(B)`, `capacity(A) == capacity(B)`
and `readiness(A) == readiness(B)`. Documents are iterated in sorted order,
failures are counted in a dict and reason codes are de-duplicated while
preserving first-seen order, so no projection depends on an arbitrary traversal.
Timestamps are **observation** instants: `observed_at`, `checked_at`,
`measured_at` and `last_verified_at`. A `clock` is injectable for tests, so
these are fully controllable and never random.

## 13. Performance (observed, no SLA defined)

W113 defines no SLA and no "acceptable" threshold. Measured on this host, one
snapshot, seconds:

| events | read (store scan) | snapshot (all projections) | health | capacity | readiness |
|---:|---:|---:|---:|---:|---:|
| 10 | 0.0024 | 0.0064 | ~1e-06 | ~1e-06 | ~1e-06 |
| 100 | 0.0188 | 0.0556 | 1e-06 | 1e-06 | 0.0 |
| 500 | 0.0945 | 0.2804 | 1e-06 | 1e-06 | 0.0 |
| 1000 | 0.1887 | 0.5907 | 1e-06 | 1e-06 | 0.0 |
| 5000 | 1.0407 | 2.9396 | 1e-06 | 2e-06 | 1e-06 |

HTTP latency per endpoint over a real Uvicorn: `0.0322 s` at 100 events,
`0.2248 s` at 1000 events.

The `health` / `capacity` / `readiness` columns are field accesses on an
already-computed snapshot, which is the point of the single-snapshot design: a
second and third projection cost no additional scan. Cost is linear in the
number of documents and is dominated by the single W110 scan.

## 14. Limitations

* **No historical series.** `observed_growth` is `None` until an external caller
  supplies a previous observation; W113 persists nothing.
* **Incomplete archive enumeration.** A corrupt archive cannot be fully counted
  without bypassing the fail-closed W112 store, so those counters are `None` and
  flagged `enumeration_complete = False`.
* **Whole-store scan per request.** The projection is O(N) over documents. It is
  bounded and cheap at the measured sizes, but there is no cache and no
  incremental state, by design.
* **Byte metrics depend on the filesystem.** They are unavailable on a store
  without a location, and inode counts are unavailable where `statvfs` does not
  expose them.
* **No threshold grading.** W113 reports observed facts and reason codes; it does
  not score, and it will not say "critical" because a number crossed a line that
  no requirement justified.
* **Diagnostics are not remediation.** A `NOT_READY` verdict tells an operator
  what to look at; acting on it remains a W112 operator decision.

## 15. Prohibited side effects

W113 adds no infrastructure: no Prometheus, Grafana, Elasticsearch, Loki, SIEM,
Redis, Kafka, extra database, volume, daemon, cron, systemd timer, sidecar or
exporter. It reuses only existing TrendX components. A future phase may export
these metrics to an external collector; nothing here does.
