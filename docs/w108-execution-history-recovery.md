# W108 — Recovery of archived execution history

## Scope

W108 adds an explicit recovery boundary for records created by the W107
`ArchiveStore` lifecycle.  It is a service-only capability: there is no
automatic restore, scheduler, worker hook, HTTP route, forecasting call, or
writeback integration.

The active history remains owned by `DurableExecutionStore` (or the
in-memory test adapter).  The archive remains owned by `ArchiveStore`.
`RecoveryService` uses those two ports and does not parse W100 JSON or open
archive files directly.

## Flow

```text
explicit ExecutionRestoreRequest
        ↓
ArchiveStore.exists()
        ↓
ArchiveStore.verify()       (schema, version, checksums, record)
        ↓
ArchiveStore.read()         (identity and tenant checks)
        ↓
ExecutionStore.restore_if_absent()   (atomic insert + locked snapshot)
        ↓
RecoveryService             (strict snapshot revalidation)
```

W99, W104 and W106 continue to read only the active `ExecutionStore`.  A
read from one of those services never invokes recovery.

## Restore contract

`ExecutionRestoreRequest` requires:

- `execution_id`;
- an explicit `tenant_id`;
- `expected_archive_checksum`;
- the safe `FAIL_IF_EXISTS` conflict policy.

The checksum is the W107 record checksum returned by
`ArchiveStore.verify()`.  It is an authorization value, not a replacement for
archive content.  The archive is the source of truth for the restored record.
The request cannot replace `reference_key`, historical timestamps, status,
provenance, result, or any other record field.

`ExecutionRestoreResult` is serializable and contains typed status/outcome,
the requested identity, verification state, deterministic reason, and optional
non-invasive restore metadata.  It contains no traceback, archive path,
credential, or full record payload.

Stable states are:

- `VERIFIED`;
- `RESTORED`;
- `ALREADY_PRESENT`;
- `CONFLICT`;
- `ARCHIVE_NOT_FOUND`;
- `INVALID_ARCHIVE`;
- `INTEGRITY_FAILURE`;
- `TENANT_FORBIDDEN`;
- `RESTORE_FAILED`.

No force-overwrite policy exists.

## Safety properties

- Verification completes before the active store is called.
- Tenant and execution identity are checked against both the record and
  archive metadata.
- The active store performs an atomic insert-if-absent operation under its
  existing process lock and atomic-write implementation.  The store returns a
  typed locked state snapshot (`INSERTED`, `ALREADY_PRESENT`, or `CONFLICT`), so
  recovery does not infer the state through a racy post-insert `get()`.
- A duplicate execution ID or reference-key conflict is rejected without
  mutation.
- An identical existing record is reported as `ALREADY_PRESENT`.
- The inserted or already-present record snapshot is compared with strict
  canonical JSON before a successful result is returned.
- A failed or doubtful archive never reaches the active store.
- The archive is never rewritten, purged, chmod'ed, or otherwise modified by
  recovery.
- Recovery metadata is attached to the operation result only; historical
  record metadata and timestamps are not changed.
- W108 restores only terminal `SUCCESS` or `FAILED` records.  A `PENDING` or
  `RUNNING` archive is rejected and never becomes active history.
- Any active record with the same `reference_key`, including a different
  execution ID, is treated as a conflict; recovery never overwrites it.

## Verification and security

The W107 archive adapter remains responsible for strict JSON parsing, archive
format/version validation, document and record checksum validation, canonical
record validation, secure root/file ownership and permissions, symlink refusal,
and hashed archive keys.  W108 adds no path interpretation or filesystem
fallback.  Errors are converted to fixed, sanitized result reasons; raw
adapter exceptions are not returned.

W108 does not introduce a new logger, cache, queue, database, or external
service.  Existing Loguru observability emits only operation, outcome, status,
and fixed reason fields.  It does not log archive paths, complete records,
payloads, authorization headers, or credentials.

## Process and concurrency model

The service is safe to call from separate Python processes.  The durable store
lock serializes the check and insertion.  Readers use the durable store's
lock/atomic publication semantics and observe either the pre-publication empty
state or the complete restored record.  A second concurrent restore reports
`ALREADY_PRESENT`; it does not create a duplicate or overwrite content.

## W99/W104/W106 compatibility

A restored terminal record is ordinary active history.  W99 queries, W104
analytics, and W106 reports see it without a special `restored` flag.  This
keeps totals, status/failure aggregates, dimensions, trends, and tenant scope
consistent with a record that had never been archived.

## API decision

No public restore endpoint is added in W108.  The existing API remains
read-only for execution history, and the OpenAPI contract therefore has no
restore, GET-restore, or DELETE-restore operation.  An operational HTTP
integration, if separately approved, must authenticate the caller, derive and
validate tenant context, require the expected checksum, and delegate only to
this service.

## Limitations

- Recovery is deliberately operator-driven; archives are not automatically
  reinserted.
- A restore result is an operation audit report, not a new durable audit-log
  table or a second history source of truth.
- The contract supports only the safe `FAIL_IF_EXISTS` policy.
- The local W107 archive adapter remains the tested implementation; remote
  archive adapters must provide the same `ArchiveStore` verification contract.
