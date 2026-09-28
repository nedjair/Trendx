# W114 — Execution audit end-to-end lifecycle consistency

> **W114 is a validation phase, not a feature phase.** It adds no storage, no
> backend, no scheduler and no destructive automation. It proves that the
> contracts built in W98 → W113 form one coherent chain, and it reports every
> divergence instead of hiding it.

## 1. Baseline

```text
branch: fix/ml-w49-master-conflicts
HEAD:   a8da0b87ee6c4a31968b8bf0926d86a90f1c58e3  (W113)
parent: 545f3f1ede3e6675363fcfa9dd8336dc769c56ee  (W112)
```

21 pre-existing untracked WIP entries were inventoried before any mutation and
are neither modified, staged nor removed by W114.

## 2. Architecture

`src/trendx/forecasting/audit_e2e.py` is the only new module. It contains no
business logic: it *gathers* the projections owned by the existing services and
*compares* them.

```text
ExecutionHistoryService          (W99)  ─┐
ExecutionHistoryAnalytics        (W104) ─┤
ExecutionAnalyticsReportingService (W106)┤
ExecutionAuditIntegrityService   (W111) ─┼─→ AuditE2EValidator ─→ LifecycleSnapshot
ExecutionAuditReconciliationService (W111)┤        │                  │
ExecutionAuditExportService      (W111) ─┤        │                  └─ fingerprint (SHA-256)
AuditLifecycleService            (W112) ─┤        └─ compare() → ConsistencyCheck
AuditHealthService               (W113) ─┘
```

The validator computes **no** statistic, no aggregate, no checksum of its own and
no operational status. Every number it reports is read from the service that
owns that truth. W114 introduces no new route: the operational suite drives the
existing HTTP surface.

## 3. Consistency matrix

Each cell compares the same store state across the whole chain. `PASS` means the
projections agree under that contract; where a dimension legitimately changes
across a transition, the test asserts the *specific* expected change instead of
demanding equality.

| State | History | Analytics | Report | Integrity | Archive | Reconciliation | Export | Health | Capacity | Readiness |
|---|---|---|---|---|---|---|---|---|---|---|
| **S0** initial | PASS | PASS | PASS | PASS | PASS | PASS | PASS | PASS | PASS | PASS |
| **S1** post-execution | PASS | PASS | PASS | PASS | PASS | PASS | PASS | PASS | PASS | PASS |
| **S2** post-archive | PASS | PASS | PASS | PASS | PASS | PASS | PASS | PASS | PASS | PASS |
| **S3** post-purge | PASS | PASS | PASS | PASS | PASS | PASS | PASS | PASS | PASS | PASS |
| **S4** post-restore | PASS | PASS | PASS | PASS | PASS | PASS | PASS | PASS | PASS | PASS |
| **S5** corrupted (fixture) | PASS | PASS | PASS | PASS | expected | reported | reported | PASS | PASS | PASS |

Justifications per cell:

* **History** — `ExecutionHistoryService.statistics()` never changes during an
  audit transition. The durable execution store is byte-identical before and
  after archive, purge and restore.
* **Analytics** — the W104 summary equals the W99 statistics at every state, and
  the audit lifecycle never adds an execution.
* **Report** — the W106 `summary` equals the W104 `summary` at every state
  (W106 is defined as a representation of one W104 result).
* **Integrity** — `VALID` at S0–S4. A tampered document yields a non-`VALID`
  status, never an invented `VALID`.
* **Archive** — `HEALTHY` with an increasing document count; `CORRUPTED` at S5
  with `enumeration_complete=False`.
* **Reconciliation** — `CONSISTENT` at S0/S1 and S2; `INSUFFICIENT_DATA` at
  S3/S4 because the correlation chain left the active journal. Never
  `INCONSISTENT` at any state. The active/archived split is reported.
* **Export** — deterministic digest for an unchanged state; the
  `ACTIVE_AND_ARCHIVED` perimeter only exceeds `ACTIVE_ONLY` **after** a purge,
  because W112 `archive()` copies without purging.
* **Health** — `HEALTHY` / `DEGRADED` reflecting the active journal only.
* **Capacity** — `total_known_event_count` is the **union** of identifiers, so an
  event present on both sides counts once.
* **Readiness** — `READY` on a healthy chain, `NOT_READY` with
  `ARCHIVE_CORRUPTED` or `AUDIT_DATA_CORRUPTED` at S5.

## 4. The canonical scenario

```text
tenant-A: x-0 SUCCESS, x-1 SUCCESS, x-2 FAILED   (model_error)
tenant-B: x-3 SUCCESS
audit:    5 tenant-A events (3 EXECUTION + RECOVERY_API + RESTORE_EXECUTE)
          1 tenant-B event
```

The `RECOVERY_API` + `RESTORE_EXECUTE` pair shares a `request_id` and an
`execution_id`, which is what gives W111 a correlation chain to evaluate. The
`RECOVERY_API` event uses `record_already_present`, one of the two reason codes
W111 accepts for a successful recovery call; any other reason code makes W111
correctly report `RECOVERY_API_SUCCESS_NOT_A_200`.

Audit events are written through `ExecutionAuditService(..., clock=...)` with a
fixed instant, so retention eligibility and every timestamp are deterministic.

## 5. What the fingerprint contains

`LifecycleSnapshot.fingerprint` is a SHA-256 over a canonical JSON of the
business facts only. It **excludes** the observation instant (`observed_at`,
`captured_at`, `checked_at`, `measured_at`, `last_verified_at`), so two snapshots
of the same logical state compare equal regardless of when they were taken.

### S0 fingerprint is deliberately *not* equal to S4

§40 asks for `S0 == S4` "if the contract provides for complete restoration". The
W112 contract does restore every original event, but W112 also **audits every
mutating operation**. A complete `archive → purge → restore` cycle therefore
appends one `AUDIT_ARCHIVE`, one `AUDIT_PURGE` and one `AUDIT_RESTORE` per
restored event. Those events are part of the audit record, not a leak.

What W114 asserts instead, precisely:

* the execution side is identical (`execution_ids`, statistics, analytics,
  report, integrity, readiness);
* every original event id is present again in the active journal;
* every event *added* by the cycle has a W112 `AUDIT_*` operation;
* the only differing dimensions are `ARCHIVE`, `CAPACITY`, `EXPORT`, `HEALTH`
  and `RECONCILIATION` — all of which are direct consequences of the archived
  perimeter and the lifecycle audit trail;
* `HISTORY`, `ANALYTICS`, `REPORT`, `INTEGRITY` and `READINESS` never differ.

## 6. Fail-closed contracts are respected, not worked around

Three existing contracts refuse to answer rather than guess, and W114 reports the
refusal instead of bypassing it:

| Situation | W111 / W112 behaviour | W114 reporting |
|---|---|---|
| corrupt **active** journal | `verify()` raises `AuditIntegrityError` | `integrity_status=None`, dimension in `unavailable` |
| corrupt **archived** document | `list_documents()` / `archived_events()` raise `AuditArchiveIntegrityError` | archive perimeter `None`, W113 status `CORRUPTED` |
| cross-tenant restore | `NOT_FOUND` (service) / `TENANT_FORBIDDEN` + 403 (HTTP) | asserted as the existing contract |

Because a projection that could not be read is reported as **unknown** rather
than `0`, `audit_outcomes` and `audit_execution_links` are nullable: an empty
dict would read as "zero events", which is a lie.

## 7. Purge safety

A purge never succeeds without a verified archive. W112 blocks the corrupt
document *itself* (`guards={"archive_corrupt": 1}`) while healthy documents
remain purgeable; the invariant W114 enforces is that the corrupt event is never
purged. After a purge, every document still in the archive verifies, the
archived count is unchanged, and reconciliation still counts the archived
perimeter — **no data leaves the lifecycle without a valid archive**.

## 8. Tenant isolation

The full cycle (archive, purge, restore) is performed on tenant-A only. After it,
every tenant-A HTTP response must contain none of `tenant-B`, `x-3`, `ref-3` or
`dev-3`. A cross-tenant restore over HTTP returns `403 TENANT_FORBIDDEN`; at the
service layer it returns `NOT_FOUND`, which discloses strictly less because the
archive is tenant-scoped at the storage layer.

## 9. Concurrency and determinism

80 concurrent reads across the ten read-only endpoints return byte-identical
*business* projections. Filesystem capacity metrics are excluded from that
comparison on purpose: `filesystem_free_bytes` is an **infrastructure
observation**, not a business fact, and the host filesystem legitimately moves
between two reads of an unchanged journal.

## 10. Read-only proof

SHA-256 fingerprints of the execution store, the audit store and the archive are
identical before and after: the whole read surface, 80 concurrent reads, and
both Process A and Process B sessions.

## 11. Performance (observed, no SLA defined)

W114 defines no SLA. Cumulative seed cost for the whole batch, and single
operation cost, in seconds:

| events | execution | audit | integrity | analytics | report | archive | purge | restore | health snapshot |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 10 | 0.0090 | 0.0093 | 0.0027 | 0.0011 | 0.0012 | 0.0186 | 0.0208 | 0.0070 | 0.0202 |
| 100 | 0.3804 | 0.1864 | 0.0217 | 0.0081 | 0.0081 | 0.1935 | 0.2019 | 0.0372 | 0.1425 |
| 500 | 8.1202 | 3.1005 | 0.1139 | 0.0402 | 0.0399 | 0.9313 | 1.0481 | 0.1874 | 0.6628 |
| 1000 | 31.2652 | 11.7017 | 0.2462 | 0.0817 | 0.0797 | 1.8744 | 2.0548 | 0.3336 | 1.3522 |

HTTP latency per endpoint (real Uvicorn, 50 executions / 51 audit events):
`history` 0.0192 s, `analytics` 0.0124 s, `report` 0.0130 s, `audit` 0.0217 s,
`integrity` 0.0222 s, `reconciliation` 0.0218 s, `export` 0.0296 s, `health`
0.0205 s, `capacity` 0.0208 s, `readiness` 0.0172 s.

## 12. Limitations

* **The W98 durable store is O(N) per save.** `DurableExecutionStore` rewrites
  its whole file atomically on every transition, so seeding 1000 executions costs
  ~31 s while 100 cost ~0.4 s. This is an existing W98 property, observed and
  reported, not something W114 changes.
* **S0 fingerprint ≠ S4 fingerprint**, by the W112 self-audit contract (§5).
* **`ACTIVE_AND_ARCHIVED` is service-level only.** The HTTP export route exposes
  the active journal and has no `scope` parameter; the combined perimeter is
  covered in-process.
* **The validator reports, it never repairs.** A `NOT_READY` verdict or a
  `CORRUPTED` archive is a diagnosis; remediation stays a W112 operator action.
* **Reconciliation status after a purge is W111's choice.** W114 asserts the
  contract (`INSUFFICIENT_DATA`, never `INCONSISTENT`) rather than demanding
  `CONSISTENT`, because the active journal genuinely no longer holds the chain.
* **No scoring.** W114 emits `CONSISTENT` / `INCONSISTENT` / `UNAVAILABLE` per
  dimension; it never grades the chain with a number.
