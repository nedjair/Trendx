# W112 — Execution audit lifecycle, retention & archive

## 1. Baseline

```text
branch: fix/ml-w49-master-conflicts
HEAD:   90a25eeca63f9d6d245e9103e6e98e4e64a970cd  (W111)
parent: 5634d030dfcdf1c74a840830a0192ec66f0ae95a  (W110)
```

Pre-existing untracked WIP was inventoried before any mutation and is neither
modified nor staged.

## 2. Architecture

```text
POST /audit/lifecycle/{preview,archive,purge,restore}
        ↓  (W110 auth + TenantContext, reused)
AuditLifecycleService
        ├── ExecutionAuditService ──→ ExecutionAuditStore   (ACTIVE)
        └── FileSystemAuditArchiveStore                    (ARCHIVED)
```

W112 manages the lifecycle of `ExecutionAuditEvent` documents **only**. It never
archives, purges or restores an `ExecutionRecord`: the W107 lifecycle owns those
and its `ArchiveStore` protocol is typed on `ExecutionRecord`, so W112 defines a
distinct `AuditArchiveStore` contract even though both are local filesystems.

W112 adds `delete_if_unchanged()` to the W110 store (the §13 compare-and-delete
primitive) and five W110 operations (`AUDIT_LIFECYCLE_PREVIEW`,
`AUDIT_ARCHIVE`, `AUDIT_ARCHIVE_VERIFY`, `AUDIT_PURGE`, `AUDIT_RESTORE`). W112
never opens an audit file directly: it reads through `scan_documents()` and
mutates only through the store's compare-and-delete.

## 3. Retention policy

`AuditRetentionPolicy(retention_days, reference_time, archive_before_purge=True,
minimum_events_to_keep=0, dry_run=True)`.

* `retention_days` and `reference_time` are **mandatory**: W112 never invents a
  retention period and never falls back to the wall clock or to local time.
* `eligible_at = reference_time - retention_days`, computed in UTC.
* Eligibility is inclusive: `occurred_at <= eligible_at`.
* Every timestamp is normalised to an explicit UTC datetime.
* `dry_run` defaults to `True`.

## 4. Preview (strictly read-only)

`LifecyclePreviewReport` carries `scanned`, `eligible`, `protected`, `invalid`,
`already_archived`, `archive_candidates`, `purge_candidates`, `warnings` and
`status`.

Candidate ordering is deterministic — newest first by
`(occurred_at, event_id)` — and the first `minimum_events_to_keep` entries are
protected regardless of age, so a run can never wipe a recent trail. That
protection applies to the tenant's newest events whatever their operation, which
means a lifecycle audit event can occupy a protection slot; the rule is
deterministic and documented rather than surprising.

**`preview` is deliberately not self-audited.** Recording a preview event would
append to the very store the preview exists to prove is unchanged, so a preview
writes nothing at all. The same rule applies to any `dry_run=True` execution of
archive or purge. Only effective (mutating) runs are self-audited.

## 5. Archive

Each archive document preserves the **complete** W110 event contract (all 15
fields, none dropped or renamed) and adds only:

```text
archive_version
archived_at
original_event_checksum
archive_document_checksum
```

The archive *identity* is the event, not the publication instant: two envelopes
holding the same event are the same archive. Re-running an archive is therefore
`ALREADY_PRESENT` rather than a false conflict, and a published document is never
rewritten.

Integrity covers canonical form, schema, event version, identity, both checksums
and tenant. Only SHA-256 is used.

## 6. Atomic publication

```text
temporary file → write → flush → fsync(file) → os.replace → fsync(parent dir)
```

Directory `0700`, documents `0600`. Temporary files are never valid archives
(listing only reads `*.json`) and are removed on both success and failure. A
crash before publication can never leave a partially valid archive.

## 7. Purge safety

The sequence is fixed and never shortened:

```text
SELECT → VALIDATE → ELIGIBILITY → ARCHIVE → VERIFY ARCHIVE → COMPARE → DELETE
```

`SELECT → DELETE` does not exist. Guards that block a purge, each recorded under
a named `AuditPurgeGuard` key: `archive_missing`, `archive_corrupt`,
`checksum_mismatch`, `conflict`, `invalid_event`, `minimum_kept`, `not_eligible`,
`protected`, `tenant_mismatch`, `already_archived`.

`archive_before_purge=False` is refused outright — a purge without an archive is
impossible by construction.

`SUCCESS` is never reported when a removal happened without a verified archive:
`integrity_failures` or `conflicts` force `PARTIAL` or `CONFLICT`.

## 8. Compare-and-delete

`ExecutionAuditStore.delete_if_unchanged(expected)` runs under the same exclusive
lock as `append`, re-reads the persisted document and compares identity, version,
tenant, `occurred_at`, checksum and the full canonical representation. Any
divergence returns `CONFLICT` and removes nothing. Outcomes are `DELETED`,
`CONFLICT`, `NOT_FOUND` and `INVALID`; the latter three are fail-closed.

## 9. Restore

`AuditRestoreRequest` / `AuditRestoreResult` restore one archived event:
`RESTORED`, `ALREADY_PRESENT`, `CONFLICT`, `NOT_FOUND`, `INTEGRITY_FAILURE`,
`TENANT_FORBIDDEN`, `RESTORE_FAILED`. An active event is never overwritten and no
`FORCE_OVERWRITE` option exists.

## 10. W110 / W111 compatibility

* W110 never returns a purged event.
* W111 integrity stays `VALID` after archive and purge — a correctly archived
  event is never treated as corruption.
* W111 reconciliation gained explicit `active_events` / `archived_events`
  counters and a `lifecycle_reader` flag through the optional
  `AuditLifecycleReader` port, so `ACTIVE` and `ARCHIVED` are distinguishable.
  Without a reader it reports `ACTIVE` for every observed event, exactly as
  before.
* W111 export gained an explicit `scope`: `ACTIVE_ONLY` (default) or
  `ACTIVE_AND_ARCHIVED`. The scope is part of the canonical bytes, so the two
  perimeters are never confused, and the checksum of a given scope is stable. An
  event present in both stores is emitted once.

## 11. API

```text
POST /api/v1/forecast/executions/audit/lifecycle/preview
POST /api/v1/forecast/executions/audit/lifecycle/archive
POST /api/v1/forecast/executions/audit/lifecycle/purge
POST /api/v1/forecast/executions/audit/lifecycle/restore
```

Each documents `200/401/403/409/422/503` and reuses the W110 auth schemes and
`X-Request-ID`. Request bodies use `extra="forbid"`, so an unknown field is a
hard `422`. The restore route returns the **mapped** HTTP status (404/403/409/503)
together with the same typed, sanitized body, so a client never reads a failure
as a success.

There is no `DELETE` route on the audit surface: every removal goes through the
purge safety gate. Authentication is the existing W110 one — no new system.

## 12. Self-audit and recursion

W112 writes its own operations as ordinary W110 events with
`source = AUDIT_LIFECYCLE`. A `ContextVar` re-entrancy guard refuses any
lifecycle started while another one runs, and a lifecycle audit write is a plain
W110 append that can never start another lifecycle, so no `audit → audit → audit`
chain is possible.

## 13. Performance (observed, no SLA defined)

| events | preview | archive | verify (20) | purge | restore |
|---:|---:|---:|---:|---:|---:|
| 10 | 0.0029 s | 0.0176 s | 0.0078 s | 0.0206 s | 0.0066 s |
| 100 | 0.0281 s | 0.1695 s | 0.0409 s | 0.1970 s | 0.0357 s |
| 500 | 0.1400 s | 0.9677 s | 0.1796 s | 1.0492 s | 0.1654 s |
| 1000 | 0.2816 s | 1.8740 s | 0.3389 s | 2.0652 s | 0.3310 s |

W112 defines no threshold; these are measurements only.

## 14. Limitations

* No automatic retention or purge: nothing runs by itself, there is no
  scheduler, cron or worker, and `dry_run` is the default.
* A lifecycle run evaluates the tenant's full active trail; on a very large store
  that read dominates the cost.
* `minimum_events_to_keep` protects the newest tenant events whatever their
  operation, so a lifecycle audit event can occupy a protection slot.
* The archive is single-host and local; W112 adds no replication and no
  cryptographic signature (only SHA-256 checksums).
* There is no compaction or de-duplication of the archive beyond per-event
  idempotence, and no scheduled pruning of the archive itself.
