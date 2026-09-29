"""W118 §5 — Global Closure Gate : certification W98-W117 versionnée.

Le gate remplace le harness jetable exécuté depuis ``/tmp/opencode/w117``
(I-05) par une infrastructure de certification versionnée, déterministe et
reproductible depuis un clone propre.

Invariants figés (contrat) :
    * chaîne de commits W98-W116, sujets et descendance ;
    * W102 explicitement ``N/A`` — phase jamais existée comme implémentation ;
    * 21 routes de la chaîne, leurs ``operationId`` et leur sécurité ;
    * versions contractuelles des 11 artefacts certifiés (``"1"``) ;
    * gel des enums / codes d'erreur de la chaîne ;
    * isolation tenant, séparation lifecycle, recovery, intégrité,
      analytics/reporting, health/capacity/readiness ;
    * documentation W98-W116 présente, référencée, sans route fictive ;
    * flags de production ``OFF`` dans le contexte du gate.

Invariants volontairement NON figés (§5.3) :
    timestamps, ``statvfs``, tailles de filesystem, mémoire/CPU, latences,
    PID, ports, URL, chemins temporaires, environnement personnel et
    compteurs historiques de tests. Les observations environnementales ne
    sont comparées que par leur projection métier stable ; aucune performance
    observée n'est transformée en SLA.

Exécution :
    pytest -q tests/integration/test_w118_global_closure_gate.py
    pytest -q tests/integration/test_w118_global_closure_gate.py -s
"""

from __future__ import annotations

import enum
import hashlib
import importlib
import inspect
import json
import os
import re
import subprocess
import sys
import tempfile
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

# ── identité du gate ─────────────────────────────────────────────────────

#: Version du gate lui-même. Distinct de ``event_version``,
#: ``contract_version``, ``archive_version`` et ``report contract_version``.
GLOBAL_CLOSURE_GATE_VERSION = "1"

#: Dernier commit certifié par W117 (début de phase W118).
GATE_BASELINE = "b614d4434cd9c37d5b161583b24d17c3b539d0a3"

REPO = Path(__file__).resolve().parents[2]
DOCS = REPO / "docs"
SRC = REPO / "src"

GATE_SECTIONS = (
    "contract",
    "api",
    "security",
    "tenant",
    "durability",
    "lifecycle",
    "recovery",
    "integrity",
    "analytics",
    "reporting",
    "health",
    "documentation",
    "production-safety",
)

SECTION_REPORT_KEYS = {
    "contract": "CONTRACT",
    "api": "API_ROUTES",
    "security": "OPENAPI_SECURITY",
    "tenant": "TENANT_ISOLATION",
    "durability": "DURABILITY",
    "lifecycle": "LIFECYCLE",
    "recovery": "RECOVERY",
    "integrity": "INTEGRITY",
    "analytics": "ANALYTICS",
    "reporting": "REPORTING",
    "health": "HEALTH",
    "documentation": "DOCUMENTATION",
    "production-safety": "PRODUCTION_SAFETY",
}

#: Chaîne certifiée : (phase, commit, sujet attendu).
CHAIN: tuple[tuple[str, str | None, str | None], ...] = (
    ("W98", "806da40", "feat(forecasting): persist forecast execution provenance"),
    ("W99", "d027884", "feat(forecasting): add execution history observability"),
    ("W100", "5968514", "feat(forecasting): persist execution history durably"),
    ("W101", "0c4d533", "feat(forecasting): expose execution history api"),
    ("W102", None, None),
    ("W103", "b3fdb64", "test(forecasting): harden execution history contract"),
    ("W104", "84af67e", "feat(forecasting): add execution history analytics"),
    (
        "W105",
        "80298d7",
        "test(forecasting): validate execution history analytics operations",
    ),
    (
        "W106",
        "7c99c34",
        "feat(forecasting): add execution history reporting contract",
    ),
    ("W107", "7a3bd98", "feat(forecasting): add execution history lifecycle"),
    ("W108", "2515d9d", "feat(forecasting): add execution history archive recovery"),
    ("W109", "e6a1a39", "feat(forecasting): add execution history recovery api"),
    ("W110", "5634d03", "feat(forecasting): add execution history operational audit"),
    ("W111", "90a25ee", "feat(forecasting): add execution audit integrity and export"),
    ("W112", "545f3f1", "feat(forecasting): add execution audit lifecycle"),
    ("W113", "a8da0b8", "feat(forecasting): add execution audit operational health"),
    (
        "W114",
        "49432f0",
        "feat(forecasting): certify execution audit end-to-end consistency",
    ),
    ("W115", "d9a01d7", "fix(forecasting): close execution audit readiness blockers"),
    ("W116", "b614d44", "docs(forecasting): complete execution audit documentation"),
)

DOC_FOR_PHASE = {
    "W98": "w98-forecast-execution-persistence.md",
    "W99": "w99-execution-history.md",
    "W100": "w100-durable-execution-history.md",
    "W101": "w101-execution-history-api.md",
    "W102": "w101-execution-history-api.md",
    "W103": "w103-execution-history-observability.md",
    "W104": "w104-execution-history-analytics.md",
    "W105": "w105-analytics-operationalization.md",
    "W106": "w106-execution-history-reporting.md",
    "W107": "w107-execution-history-lifecycle.md",
    "W108": "w108-execution-history-recovery.md",
    "W109": "w109-execution-history-recovery-api.md",
    "W110": "w110-execution-history-operational-audit.md",
    "W111": "w111-execution-audit-integrity-export.md",
    "W112": "w112-execution-audit-lifecycle.md",
    "W113": "w113-execution-audit-operational-health.md",
    "W114": "w114-execution-audit-e2e-consistency.md",
    "W115": "w115-execution-audit-production-readiness.md",
    "W116": "execution-audit-cross-reference.md",
}

#: Documents de la chaîne utilisés pour les contrôles de documentation.
CHAIN_DOCS = (
    "w98-forecast-execution-persistence.md",
    "w99-execution-history.md",
    "w100-durable-execution-history.md",
    "w101-execution-history-api.md",
    "w103-execution-history-observability.md",
    "w104-execution-history-analytics.md",
    "w105-analytics-operationalization.md",
    "w106-execution-history-reporting.md",
    "w115-execution-audit-production-readiness.md",
    "execution-audit-chain.md",
    "execution-audit-api-reference.md",
    "execution-audit-security.md",
    "execution-audit-troubleshooting.md",
    "execution-audit-cross-reference.md",
)

#: 11 artefacts certifiés par W117 — leur version doit rester ``"1"``.
CERTIFIED_ARTEFACTS = (
    "audit_event",
    "audit_store_format",
    "audit_control_report",
    "audit_export",
    "audit_archive",
    "audit_lifecycle_report",
    "audit_health_report",
    "lifecycle_consistency_report",
    "execution_store_format",
    "restore",
    "report_contract",
)

#: Les 21 routes de la chaîne, figées (méthode + chemin + operationId).
EXPECTED_CHAIN_ROUTES = {
    "GET /api/v1/forecast/executions": "list_executions_api_v1_forecast_executions_get",
    "GET /api/v1/forecast/executions/analytics": (
        "execution_analytics_api_v1_forecast_executions_analytics_get"
    ),
    "GET /api/v1/forecast/executions/audit": (
        "query_execution_audit_api_v1_forecast_executions_audit_get"
    ),
    "GET /api/v1/forecast/executions/audit/capacity": (
        "execution_audit_capacity_api_v1_forecast_executions_audit_capacity_get"
    ),
    "GET /api/v1/forecast/executions/audit/export": (
        "execution_audit_export_api_v1_forecast_executions_audit_export_get"
    ),
    "GET /api/v1/forecast/executions/audit/export/checksum": (
        "execution_audit_export_checksum_api_v1_forecast_executions_audit_export_checksum_get"
    ),
    "GET /api/v1/forecast/executions/audit/health": (
        "execution_audit_health_api_v1_forecast_executions_audit_health_get"
    ),
    "GET /api/v1/forecast/executions/audit/integrity": (
        "execution_audit_integrity_api_v1_forecast_executions_audit_integrity_get"
    ),
    "GET /api/v1/forecast/executions/audit/readiness": (
        "execution_audit_readiness_api_v1_forecast_executions_audit_readiness_get"
    ),
    "GET /api/v1/forecast/executions/audit/reconciliation": (
        "execution_audit_reconciliation_api_v1_forecast_executions_audit_reconciliation_get"
    ),
    "GET /api/v1/forecast/executions/audit/statistics": (
        "execution_audit_statistics_api_v1_forecast_executions_audit_statistics_get"
    ),
    "GET /api/v1/forecast/executions/reference/{reference_key}": (
        "get_execution_by_reference_api_v1_forecast_executions_reference__reference_key__get"
    ),
    "GET /api/v1/forecast/executions/report": (
        "execution_report_api_v1_forecast_executions_report_get"
    ),
    "GET /api/v1/forecast/executions/statistics": (
        "execution_statistics_api_v1_forecast_executions_statistics_get"
    ),
    "GET /api/v1/forecast/executions/{execution_id}": (
        "get_execution_api_v1_forecast_executions__execution_id__get"
    ),
    "GET /api/v1/forecast/executions/{execution_id}/diagnostic": (
        "get_execution_diagnostic_api_v1_forecast_executions__execution_id__diagnostic_get"
    ),
    "POST /api/v1/forecast/executions/audit/lifecycle/archive": (
        "archive_audit_lifecycle_api_v1_forecast_executions_audit_lifecycle_archive_post"
    ),
    "POST /api/v1/forecast/executions/audit/lifecycle/preview": (
        "preview_audit_lifecycle_api_v1_forecast_executions_audit_lifecycle_preview_post"
    ),
    "POST /api/v1/forecast/executions/audit/lifecycle/purge": (
        "purge_audit_lifecycle_api_v1_forecast_executions_audit_lifecycle_purge_post"
    ),
    "POST /api/v1/forecast/executions/audit/lifecycle/restore": (
        "restore_audit_lifecycle_api_v1_forecast_executions_audit_lifecycle_restore_post"
    ),
    "POST /api/v1/forecast/executions/restore": (
        "restore_execution_api_v1_forecast_executions_restore_post"
    ),
}

EXPECTED_CHAIN_ROUTE_COUNT = 21
EXPECTED_SECURITY_SCHEMES = ["ApiKeyAuth", "BearerAuth"]
EXPECTED_SECURITY = [{"BearerAuth": []}, {"ApiKeyAuth": []}]

#: Modules dont les enums composent le gel contractuel (``api`` exclu :
#: son ``_ApiErrorCode`` est compté à part).
CHAIN_MODULES = (
    "execution",
    "history",
    "analytics",
    "reporting",
    "recovery",
    "lifecycle",
    "audit",
    "audit_control",
    "audit_lifecycle",
    "audit_health",
    "audit_e2e",
)
EXPECTED_ENUM_DECLARATIONS = 25
EXPECTED_ENUM_MEMBERS = 121
EXPECTED_API_ERROR_CODES = 17

#: Flags de production : ``OFF`` dans tout contexte de gate.
PRODUCTION_FLAGS = (
    ("TRENDX_SCHEDULER_FORECAST_ENABLED", "trendx_scheduler_forecast_enabled"),
    ("TRENDX_INGEST_ENABLED", "trendx_ingest_enabled"),
    ("TRENDX_WORKER_INGESTION_ENABLED", "trendx_worker_ingestion_enabled"),
    ("TB_WRITEBACK_ENABLED", "tb_writeback_enabled"),
    ("TB_ALARMS_ENABLED", "tb_alarms_enabled"),
    ("ANOMALY_DETECTION_ENABLED", "anomaly_detection_enabled"),
)

CHAIN_READS = (
    "history",
    "statistics",
    "analytics",
    "report",
    "audit",
    "integrity",
    "reconciliation",
    "export",
    "health",
    "capacity",
    "readiness",
)

TENANT_A = "tenant-A"
TENANT_B = "tenant-B"
NOW = datetime(2026, 4, 1, tzinfo=UTC)
AUDIT_INSTANT = NOW - timedelta(days=100)

#: Noms des clés du rapport structuré, dans l'ordre contractuel (§5.6).
REPORT_KEYS = (
    "GATE",
    "VERSION",
    "BASELINE",
    *(phase for phase, _commit, _subject in CHAIN),
    *(SECTION_REPORT_KEYS[section] for section in GATE_SECTIONS),
    "RESULT",
)


# ── diagnostic de gate ───────────────────────────────────────────────────


class GateFailureError(AssertionError):
    """Échec explicite : phase, invariant, attendu, observé."""

    def __init__(
        self,
        *,
        invariant: str,
        expected: Any,
        observed: Any,
        phase: str | None = None,
    ) -> None:
        self.invariant = invariant
        self.expected = expected
        self.observed = observed
        self.phase = phase
        super().__init__(str(self))

    def __str__(self) -> str:
        prefix = f"phase={self.phase} | " if self.phase else ""
        return (
            f"GATE FAIL | {prefix}invariant={self.invariant} | "
            f"expected={self.expected!r} | observed={self.observed!r}"
        )


def require(
    *,
    condition: bool,
    invariant: str,
    expected: Any,
    observed: Any,
    phase: str | None = None,
) -> None:
    if not condition:
        raise GateFailureError(
            invariant=invariant, expected=expected, observed=observed, phase=phase
        )


# ── registre des sections ────────────────────────────────────────────────


@dataclass(frozen=True)
class GateCheck:
    check_id: str
    section: str
    func: Callable[..., None]


CHECKS: list[GateCheck] = []


def gate(check_id: str, section: str) -> Callable[[Callable[..., None]], Callable[..., None]]:
    if section not in GATE_SECTIONS:
        raise ValueError(f"section inconnue : {section}")

    def decorate(func: Callable[..., None]) -> Callable[..., None]:
        CHECKS.append(GateCheck(check_id, section, func))
        return func

    return decorate


# ── helpers lecture seule ────────────────────────────────────────────────


def git(*args: str) -> str:
    return subprocess.run(
        ["git", *args], cwd=REPO, capture_output=True, text=True, check=False
    ).stdout


def commit_files(commit: str) -> list[str]:
    return [line for line in git("show", "--name-only", "--format=", commit).split() if line]


def is_ancestor(commit: str) -> bool:
    return (
        subprocess.run(
            ["git", "merge-base", "--is-ancestor", commit, "HEAD"],
            cwd=REPO,
            capture_output=True,
            check=False,
        ).returncode
        == 0
    )


_LIVE_OPENAPI: dict[str, Any] | None = None


def live_openapi() -> dict[str, Any]:
    """OpenAPI de l'application, construit une seule fois par session."""

    global _LIVE_OPENAPI
    if _LIVE_OPENAPI is None:
        if str(SRC) not in sys.path:
            sys.path.insert(0, str(SRC))
        from trendx.main import app

        _LIVE_OPENAPI = app.openapi()
    return _LIVE_OPENAPI


def chain_routes(spec: dict[str, Any] | None = None) -> dict[str, dict[str, Any]]:
    spec = spec if spec is not None else live_openapi()
    routes: dict[str, dict[str, Any]] = {}
    for path, item in spec["paths"].items():
        for method, operation in item.items():
            if method in ("get", "post") and "/executions" in path:
                routes[f"{method.upper()} {path}"] = operation
    return routes


def business_tree_digest() -> str:
    """Empreinte SHA-256 de src/ + migrations/ (artefacts métier)."""

    digest = hashlib.sha256()
    for folder in (SRC, REPO / "migrations"):
        if not folder.exists():
            continue
        for item in sorted(folder.rglob("*")):
            if item.is_file() and item.suffix != ".pyc":
                digest.update(item.relative_to(REPO).as_posix().encode())
                digest.update(item.read_bytes())
    return digest.hexdigest()


def evaluate_phase(phase: str) -> tuple[str, str]:
    """Statut d'une phase certifiée : ``PASS``, ``FAIL`` ou ``N/A``."""

    entry = next(row for row in CHAIN if row[0] == phase)
    _phase, commit, subject = entry
    if commit is None:
        return "N/A", "phase never existed as implementation/commit"

    problems: list[str] = []
    if git("cat-file", "-t", commit).strip() != "commit":
        problems.append("commit absent de l'historique")
    elif git("log", "-1", "--format=%s", commit).strip() != subject:
        problems.append("sujet du commit modifié")
    elif not is_ancestor(commit):
        problems.append("commit plus ancêtre de HEAD")

    if not (DOCS / DOC_FOR_PHASE[phase]).exists():
        problems.append(f"document manquant : {DOC_FOR_PHASE[phase]}")

    files = (
        commit_files(commit)
        if not problems or git("cat-file", "-t", commit).strip() == "commit"
        else []
    )
    code = sorted(name for name in files if name.startswith("src/"))
    tests = sorted(name for name in files if name.startswith("tests/"))
    for name in code + tests:
        if not (REPO / name).exists():
            problems.append(f"fichier référencé absent : {name}")
    if phase in {"W105", "W116"}:
        if code:
            problems.append("phase sans code attendue")
    elif not code:
        problems.append("aucun fichier src/ dans le commit")
    if phase not in {"W105", "W116"} and not tests:
        problems.append("aucun fichier tests/ dans le commit")

    if problems:
        return "FAIL", "; ".join(problems)
    return "PASS", "ok"


def collect_enums() -> dict[str, dict[str, list[str]]]:
    found: dict[str, dict[str, list[str]]] = {}
    for name in CHAIN_MODULES:
        module = importlib.import_module(f"trendx.forecasting.{name}")
        for attr, value in vars(module).items():
            if (
                isinstance(value, type)
                and issubclass(value, enum.Enum)
                and value.__module__ == module.__name__
            ):
                values = [member.value for member in value]
                if len(values) != len(set(values)):
                    raise GateFailureError(
                        invariant="enum_values_unique",
                        expected=f"{name}.{attr} sans doublon",
                        observed=values,
                    )
                found[f"{name}.{attr}"] = values
    return found


# ── pile de services (tmp_path uniquement, aucun service externe) ────────


def fingerprint(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    if path.is_file():
        return {path.name: hashlib.sha256(path.read_bytes()).hexdigest()}
    return {
        item.relative_to(path).as_posix(): hashlib.sha256(item.read_bytes()).hexdigest()
        for item in sorted(path.rglob("*"))
        if item.is_file()
    }


class Stack:
    """Graphe de services réel, enraciné dans ``tmp_path``."""

    def __init__(self, root: Path) -> None:
        from trendx.forecasting.audit import ExecutionAuditService, ExecutionAuditStore
        from trendx.forecasting.audit_control import (
            ExecutionAuditControlService,
            ExecutionAuditExportService,
            ExecutionAuditIntegrityService,
            ExecutionAuditReconciliationService,
        )
        from trendx.forecasting.audit_health import AuditHealthService
        from trendx.forecasting.audit_lifecycle import (
            AuditLifecycleService,
            FileSystemAuditArchiveStore,
        )

        self.root = root
        self.exec_path = root / "executions"
        self.audit_path = root / "audit"
        self.archive_path = root / "audit-archive"
        self.audit_store = ExecutionAuditStore(self.audit_path)
        self.audit = ExecutionAuditService(self.audit_store, clock=lambda: AUDIT_INSTANT)
        self.control = ExecutionAuditControlService(self.audit)
        self.integrity = ExecutionAuditIntegrityService(self.audit)
        self.reconciliation = ExecutionAuditReconciliationService(self.audit)
        self.export = ExecutionAuditExportService(self.audit)
        self.audit_archive = FileSystemAuditArchiveStore(self.archive_path)
        self.lifecycle = AuditLifecycleService(self.audit, self.audit_archive)
        self.health = AuditHealthService(
            self.audit,
            archive_store=self.audit_archive,
            lifecycle=self.lifecycle,
            active_path=self.audit_path,
            archive_path=self.archive_path,
        )
        self.event_ids: list[str] = []
        self.tenant_a_event_ids: list[str] = []

    def fingerprint(self) -> dict[str, dict[str, str]]:
        return {
            "execution": fingerprint(self.exec_path),
            "audit": fingerprint(self.audit_path),
            "audit_archive": fingerprint(self.archive_path),
        }

    def policy(self, **overrides: Any) -> Any:
        from trendx.forecasting.audit_lifecycle import AuditRetentionPolicy

        base: dict[str, Any] = {
            "retention_days": 10,
            "reference_time": NOW,
            "archive_before_purge": True,
            "minimum_events_to_keep": 0,
            "dry_run": True,
        }
        base.update(overrides)
        return AuditRetentionPolicy(**base)

    def seed(self, tenant_a: int = 3, tenant_b: int = 1) -> None:
        from trendx.forecasting.audit import (
            AuditActorType,
            AuditOperation,
            AuditOutcome,
        )
        from trendx.forecasting.execution import (
            DurableExecutionStore,
            ExecutionProvenance,
            ExecutionRecord,
        )

        executions = DurableExecutionStore(self.exec_path)
        index = 0
        for tenant, count in ((TENANT_A, tenant_a), (TENANT_B, tenant_b)):
            for position in range(count):
                succeeds = not (tenant == TENANT_A and position == tenant_a - 1)
                record = executions.create(
                    ExecutionRecord(
                        execution_id=f"x-{index}",
                        reference_key=f"ref-{index}",
                        tenant_id=tenant,
                        entity_type="DEVICE",
                        entity_id=f"dev-{index}",
                        target_metric="temperature",
                        frequency="1h",
                        horizon=24,
                        model_id="prophet",
                        model_version="1",
                        algorithm="prophet",
                        created_at=(NOW - timedelta(days=50 + index)).isoformat(),
                        metadata={"source": "gate"},
                    )
                )
                executions.mark_started(record.execution_id)
                provenance = ExecutionProvenance(
                    model_id="prophet",
                    model_version="1",
                    algorithm="prophet",
                    feature_schema_version="schema-A",
                    prediction_count=3,
                )
                final = (
                    executions.mark_success(record.execution_id, provenance)
                    if succeeds
                    else executions.mark_failed(
                        record.execution_id,
                        error_code="model_error",
                        error_reason="fit failed",
                        provenance=provenance,
                    )
                )
                event = self.audit.record(
                    operation=AuditOperation.EXECUTION,
                    outcome=AuditOutcome.SUCCESS if succeeds else AuditOutcome.FAILED,
                    tenant_id=tenant,
                    execution_id=final.execution_id,
                    reference_key=final.reference_key,
                    actor_type=AuditActorType.SYSTEM,
                    actor_id="scheduler",
                    request_id=f"req-{index}",
                    source="gate",
                    reason_code="execution_completed",
                )
                if event is None:
                    raise GateFailureError(
                        invariant="audit_event_recorded",
                        expected="un événement d'audit",
                        observed=None,
                        phase="W110",
                    )
                self.event_ids.append(event.event_id)
                if tenant == TENANT_A:
                    self.tenant_a_event_ids.append(event.event_id)
                index += 1
        executions.close()


def history_service(stack: Stack) -> Any:
    from trendx.forecasting.execution import DurableExecutionStore
    from trendx.forecasting.history import ExecutionHistoryService

    return ExecutionHistoryService(DurableExecutionStore(stack.exec_path, create_if_missing=False))


def query(tenant: str = TENANT_A) -> Any:
    from trendx.forecasting.history import ExecutionQuery

    return ExecutionQuery(tenant_id=tenant)


def audit_query(tenant: str = TENANT_A) -> Any:
    from trendx.forecasting.audit import AuditQuery

    return AuditQuery(tenant_id=tenant)


def restore_request(event_id: str, tenant: str = TENANT_A) -> Any:
    from trendx.forecasting.audit_lifecycle import AuditRestoreRequest

    return AuditRestoreRequest(event_id=event_id, tenant_id=tenant)


# ── §contract ────────────────────────────────────────────────────────────


@gate("TEST-CLOSURE-01", "contract")
def test_closure_01_gate_is_versioned_and_executable() -> None:
    """Le gate est versionné, son schéma de rapport est stable et chaque
    section du gate possède au moins un contrôle explicite."""

    require(
        condition=GLOBAL_CLOSURE_GATE_VERSION == "1",
        invariant="gate_version",
        expected="1",
        observed=GLOBAL_CLOSURE_GATE_VERSION,
    )
    require(
        condition=GATE_BASELINE == "b614d4434cd9c37d5b161583b24d17c3b539d0a3",
        invariant="gate_baseline",
        expected="b614d4434cd9c37d5b161583b24d17c3b539d0a3",
        observed=GATE_BASELINE,
    )
    derived = (
        "GATE",
        "VERSION",
        "BASELINE",
        *(phase for phase, _commit, _subject in CHAIN),
        *(SECTION_REPORT_KEYS[section] for section in GATE_SECTIONS),
        "RESULT",
    )
    require(
        condition=REPORT_KEYS == derived,
        invariant="report_schema",
        expected=derived,
        observed=REPORT_KEYS,
    )
    require(
        condition=callable(live_openapi) and Path(__file__).is_relative_to(REPO),
        invariant="gate_is_versioned_in_repository",
        expected=str(REPO),
        observed=__file__,
    )
    covered = {check.section for check in CHECKS}
    require(
        condition=covered == set(GATE_SECTIONS),
        invariant="every_section_has_a_check",
        expected=sorted(GATE_SECTIONS),
        observed=sorted(covered),
    )
    ids = [check.check_id for check in CHECKS]
    require(
        condition=len(ids) == len(set(ids)),
        invariant="check_ids_unique",
        expected="identifiants uniques",
        observed=ids,
    )
    require(
        condition=len(CHECKS) >= len(GATE_SECTIONS),
        invariant="composable_sections",
        expected=f">= {len(GATE_SECTIONS)} contrôles",
        observed=len(CHECKS),
    )


@gate("TEST-CLOSURE-02", "contract")
def test_closure_02_w102_stays_not_applicable() -> None:
    """W102 reste explicitement N/A : ni commit, ni fichier, ni test."""

    status, detail = evaluate_phase("W102")
    require(
        condition=status == "N/A",
        invariant="w102_is_not_applicable",
        expected="N/A",
        observed=status,
        phase="W102",
    )
    require(
        condition=git("log", "--all", "--oneline", "--grep=W102").strip() == "",
        invariant="w102_never_committed",
        expected="aucun commit mentionnant W102",
        observed=git("log", "--all", "--oneline", "--grep=W102").strip(),
        phase="W102",
    )
    tracked = [name for name in git("ls-files").splitlines() if "w102" in name.lower()]
    require(
        condition=tracked == [],
        invariant="w102_never_delivered_a_file",
        expected="aucun fichier w102 versionné",
        observed=tracked,
        phase="W102",
    )
    text = (DOCS / DOC_FOR_PHASE["W102"]).read_text(encoding="utf-8")
    require(
        condition="W102 n'existe pas" in text,
        invariant="w102_documented_as_absent",
        expected="« W102 n'existe pas »",
        observed=detail,
        phase="W102",
    )


@gate("TEST-CLOSURE-03", "contract")
def test_closure_03_eighteen_verified_phases_are_present() -> None:
    statuses = {phase: evaluate_phase(phase) for phase, _commit, _subject in CHAIN}
    verified = [phase for phase, (status, _detail) in statuses.items() if status == "PASS"]
    not_applicable = [phase for phase, (status, _detail) in statuses.items() if status == "N/A"]
    require(
        condition=len(verified) == 18,
        invariant="verified_phases_count",
        expected=18,
        observed=verified,
    )
    require(
        condition=not_applicable == ["W102"],
        invariant="not_applicable_phases",
        expected=["W102"],
        observed=not_applicable,
    )
    failing = {phase: detail for phase, (status, detail) in statuses.items() if status == "FAIL"}
    require(
        condition=failing == {},
        invariant="every_phase_still_resolves",
        expected={},
        observed=failing,
    )


@gate("TEST-CLOSURE-04", "contract")
def test_closure_04_no_expected_commit_disappears() -> None:
    links = 0
    previous: str | None = None
    for phase, commit, subject in CHAIN:
        if commit is None:
            continue
        require(
            condition=git("cat-file", "-t", commit).strip() == "commit",
            invariant="commit_exists",
            expected="commit",
            observed=git("cat-file", "-t", commit).strip(),
            phase=phase,
        )
        require(
            condition=git("log", "-1", "--format=%s", commit).strip() == subject,
            invariant="commit_subject_unchanged",
            expected=subject,
            observed=git("log", "-1", "--format=%s", commit).strip(),
            phase=phase,
        )
        require(
            condition=is_ancestor(commit),
            invariant="commit_is_ancestor_of_head",
            expected=True,
            observed=False,
            phase=phase,
        )
        if previous is not None:
            parent = git("rev-parse", f"{commit}^").strip()[:7]
            require(
                condition=parent == previous,
                invariant="chain_is_linear",
                expected=previous,
                observed=parent,
                phase=phase,
            )
            links += 1
        previous = commit
    require(
        condition=links == 17,
        invariant="chain_link_count",
        expected=17,
        observed=links,
    )
    require(
        condition=previous == "b614d44",
        invariant="chain_head_commit",
        expected="b614d44",
        observed=previous,
    )


@gate("TEST-CLOSURE-08", "contract")
def test_closure_08_certified_artefact_versions_stay_one() -> None:
    from trendx.forecasting.audit import AUDIT_EVENT_VERSION, ExecutionAuditStore
    from trendx.forecasting.audit_control import (
        AUDIT_CONTROL_REPORT_VERSION,
        AUDIT_EXPORT_VERSION,
    )
    from trendx.forecasting.audit_e2e import LIFECYCLE_CONSISTENCY_REPORT_VERSION
    from trendx.forecasting.audit_health import AUDIT_HEALTH_REPORT_VERSION
    from trendx.forecasting.audit_lifecycle import (
        AUDIT_ARCHIVE_VERSION,
        AUDIT_LIFECYCLE_REPORT_VERSION,
        FileSystemAuditArchiveStore,
    )
    from trendx.forecasting.execution import DurableExecutionStore
    from trendx.forecasting.recovery import RESTORE_VERSION
    from trendx.forecasting.reporting import REPORT_CONTRACT_VERSION

    artefacts = {
        "audit_event": AUDIT_EVENT_VERSION,
        "audit_store_format": ExecutionAuditStore.FORMAT_VERSION,
        "audit_control_report": AUDIT_CONTROL_REPORT_VERSION,
        "audit_export": AUDIT_EXPORT_VERSION,
        "audit_archive": AUDIT_ARCHIVE_VERSION,
        "audit_lifecycle_report": AUDIT_LIFECYCLE_REPORT_VERSION,
        "audit_health_report": AUDIT_HEALTH_REPORT_VERSION,
        "lifecycle_consistency_report": LIFECYCLE_CONSISTENCY_REPORT_VERSION,
        "execution_store_format": DurableExecutionStore.FORMAT_VERSION,
        "restore": RESTORE_VERSION,
        "report_contract": REPORT_CONTRACT_VERSION,
    }
    require(
        condition=tuple(sorted(artefacts)) == tuple(sorted(CERTIFIED_ARTEFACTS)),
        invariant="certified_artefact_set",
        expected=sorted(CERTIFIED_ARTEFACTS),
        observed=sorted(artefacts),
    )
    require(
        condition=all(str(value) == "1" for value in artefacts.values()),
        invariant="certified_artefact_versions",
        expected={name: "1" for name in artefacts},
        observed={name: str(value) for name, value in artefacts.items()},
    )
    require(
        condition=FileSystemAuditArchiveStore.FORMAT_VERSION == AUDIT_ARCHIVE_VERSION,
        invariant="archive_store_format_matches_archive_version",
        expected=str(AUDIT_ARCHIVE_VERSION),
        observed=str(FileSystemAuditArchiveStore.FORMAT_VERSION),
    )


@gate("TEST-CLOSURE-22", "contract")
def test_closure_22_contract_has_not_diverged() -> None:
    from trendx.forecasting.api import _ApiErrorCode
    from trendx.forecasting.audit_health import BLOCKING_REASON_CODES

    enums = collect_enums()
    require(
        condition=len(enums) == EXPECTED_ENUM_DECLARATIONS,
        invariant="enum_declaration_count",
        expected=EXPECTED_ENUM_DECLARATIONS,
        observed=len(enums),
    )
    members = sum(len(values) for values in enums.values())
    require(
        condition=members == EXPECTED_ENUM_MEMBERS,
        invariant="enum_member_count",
        expected=EXPECTED_ENUM_MEMBERS,
        observed=members,
    )
    for name, values in enums.items():
        for value in values:
            require(
                condition=isinstance(value, str) and value == value.strip(),
                invariant="enum_values_are_stripped_strings",
                expected="chaîne nettoyée",
                observed=(name, value),
            )
    codes = [code.value for code in _ApiErrorCode]
    require(
        condition=len(codes) == EXPECTED_API_ERROR_CODES,
        invariant="api_error_code_count",
        expected=EXPECTED_API_ERROR_CODES,
        observed=codes,
    )
    require(
        condition=all(code == code.lower() for code in codes) and len(set(codes)) == len(codes),
        invariant="api_error_codes_lowercase_and_unique",
        expected="codes uniques en minuscules",
        observed=codes,
    )
    require(
        condition=not (set(codes) & set(BLOCKING_REASON_CODES)),
        invariant="api_error_codes_disjoint_from_blocking_reasons",
        expected=set(),
        observed=sorted(set(codes) & set(BLOCKING_REASON_CODES)),
    )


# ── §api ─────────────────────────────────────────────────────────────────


@gate("TEST-CLOSURE-05", "api")
def test_closure_05_chain_routes_stay_present() -> None:
    routes = chain_routes()
    require(
        condition=len(routes) == EXPECTED_CHAIN_ROUTE_COUNT,
        invariant="chain_route_count",
        expected=EXPECTED_CHAIN_ROUTE_COUNT,
        observed=sorted(routes),
    )
    missing = sorted(set(EXPECTED_CHAIN_ROUTES) - set(routes))
    extra = sorted(set(routes) - set(EXPECTED_CHAIN_ROUTES))
    require(
        condition=missing == [],
        invariant="chain_routes_present",
        expected=sorted(EXPECTED_CHAIN_ROUTES),
        observed=missing,
    )
    require(
        condition=extra == [],
        invariant="no_unexpected_chain_route",
        expected=[],
        observed=extra,
    )
    changed = {
        key: (EXPECTED_CHAIN_ROUTES[key], routes[key]["operationId"])
        for key in EXPECTED_CHAIN_ROUTES
        if routes[key]["operationId"] != EXPECTED_CHAIN_ROUTES[key]
    }
    require(
        condition=changed == {},
        invariant="chain_operation_ids_unchanged",
        expected={},
        observed=changed,
    )


@gate("TEST-CLOSURE-23", "api")
def test_closure_23_documented_api_shape_matches_live_openapi() -> None:
    spec = live_openapi()
    api_reference = (DOCS / "execution-audit-api-reference.md").read_text(encoding="utf-8")
    claimed_paths = re.search(r"(\d+) `paths`", api_reference)
    claimed_operations = re.search(r"(\d+) `operationId`", api_reference)
    claimed_routes = re.search(r"\*\*(\d+) routes\*\*", api_reference)
    operations = sum(
        1
        for item in spec["paths"].values()
        for method in item
        if method in ("get", "post", "put", "delete", "patch")
    )
    require(
        condition=claimed_paths is not None and int(claimed_paths.group(1)) == len(spec["paths"]),
        invariant="documented_path_count_matches_live_openapi",
        expected=len(spec["paths"]),
        observed=claimed_paths.group(1) if claimed_paths else None,
    )
    require(
        condition=claimed_operations is not None and int(claimed_operations.group(1)) == operations,
        invariant="documented_operation_count_matches_live_openapi",
        expected=operations,
        observed=claimed_operations.group(1) if claimed_operations else None,
    )
    require(
        condition=claimed_routes is not None
        and int(claimed_routes.group(1)) == EXPECTED_CHAIN_ROUTE_COUNT,
        invariant="documented_chain_route_count",
        expected=EXPECTED_CHAIN_ROUTE_COUNT,
        observed=claimed_routes.group(1) if claimed_routes else None,
    )
    require(
        condition=sorted(spec["components"]["securitySchemes"]) == EXPECTED_SECURITY_SCHEMES,
        invariant="security_schemes",
        expected=EXPECTED_SECURITY_SCHEMES,
        observed=sorted(spec["components"]["securitySchemes"]),
    )


# ── §security ────────────────────────────────────────────────────────────


@gate("TEST-CLOSURE-06", "security")
def test_closure_06_every_chain_route_is_secured() -> None:
    routes = chain_routes()
    unsecured = [
        key for key, operation in routes.items() if operation.get("security") != EXPECTED_SECURITY
    ]
    require(
        condition=len(routes) == EXPECTED_CHAIN_ROUTE_COUNT,
        invariant="chain_route_count",
        expected=EXPECTED_CHAIN_ROUTE_COUNT,
        observed=len(routes),
    )
    require(
        condition=unsecured == [],
        invariant="all_chain_routes_declare_bearer_and_api_key",
        expected=[],
        observed=unsecured,
    )
    without_denial = [
        key
        for key, operation in routes.items()
        if not set(operation.get("responses", {})) >= {"200", "401", "403"}
    ]
    require(
        condition=without_denial == [],
        invariant="chain_routes_refuse_unauthenticated_and_forbidden",
        expected={"200", "401", "403"},
        observed=without_denial,
    )


@gate("TEST-CLOSURE-07", "security")
def test_closure_07_no_chain_route_is_openapi_unsecured() -> None:
    spec = live_openapi()
    unsecured = [
        f"{method.upper()} {path}"
        for path, item in spec["paths"].items()
        for method, operation in item.items()
        if method in ("get", "post") and "/executions" in path and not operation.get("security")
    ]
    require(
        condition=unsecured == [],
        invariant="zero_openapi_unsecured_chain_route",
        expected=0,
        observed=unsecured,
    )
    require(
        condition=sorted(spec["components"]["securitySchemes"]) == EXPECTED_SECURITY_SCHEMES,
        invariant="security_scheme_names",
        expected=EXPECTED_SECURITY_SCHEMES,
        observed=sorted(spec["components"]["securitySchemes"]),
    )


# ── §tenant ──────────────────────────────────────────────────────────────


@gate("TEST-CLOSURE-09", "tenant")
def test_closure_09_tenant_isolation_invariants(tmp_path: Path) -> None:
    from trendx.forecasting.audit import AuditQuery
    from trendx.forecasting.audit_lifecycle import AuditRestoreStatus

    stack = Stack(tmp_path)
    stack.seed(tenant_a=3, tenant_b=2)

    tenant_a = stack.control.query(AuditQuery(tenant_id=TENANT_A))
    tenant_b = stack.control.query(AuditQuery(tenant_id=TENANT_B))
    require(
        condition=tenant_a.total == 3 and tenant_b.total == 2,
        invariant="audit_query_is_tenant_scoped",
        expected={"tenant-A": 3, "tenant-B": 2},
        observed={"tenant-A": tenant_a.total, "tenant-B": tenant_b.total},
    )
    require(
        condition=not (
            {event.event_id for event in tenant_a.events}
            & {event.event_id for event in tenant_b.events}
        ),
        invariant="tenant_projections_are_disjoint",
        expected=set(),
        observed=sorted(
            {event.event_id for event in tenant_a.events}
            & {event.event_id for event in tenant_b.events}
        ),
    )
    for reader in (stack.health.health, stack.health.capacity, stack.health.readiness):
        require(
            condition=reader(TENANT_A).tenant_id == TENANT_A
            and reader(TENANT_B).tenant_id == TENANT_B,
            invariant="health_projection_echoes_the_requested_tenant",
            expected=[TENANT_A, TENANT_B],
            observed=(reader(TENANT_A).tenant_id, reader(TENANT_B).tenant_id),
        )
    export_a = stack.export.export(TENANT_A, generated_at=AUDIT_INSTANT)
    export_b = stack.export.export(TENANT_B, generated_at=AUDIT_INSTANT)
    require(
        condition=len(export_a.events) == 3
        and all(event.tenant_id == TENANT_A for event in export_a.events),
        invariant="export_is_tenant_scoped",
        expected=3,
        observed=[event.tenant_id for event in export_a.events],
    )
    require(
        condition=len(export_b.events) == 2,
        invariant="export_does_not_leak_other_tenants",
        expected=2,
        observed=len(export_b.events),
    )

    stack.lifecycle.archive(TENANT_A, stack.policy(dry_run=False))
    before = stack.fingerprint()
    refusals = [
        stack.lifecycle.restore(
            restore_request(stack.tenant_a_event_ids[0], TENANT_B),
            authenticated_tenant=TENANT_A,
        )
        for _ in range(3)
    ]
    after = stack.fingerprint()
    require(
        condition=all(result.status is AuditRestoreStatus.TENANT_FORBIDDEN for result in refusals),
        invariant="cross_tenant_restore_is_refused",
        expected=AuditRestoreStatus.TENANT_FORBIDDEN.name,
        observed=[result.status.name for result in refusals],
    )
    require(
        condition=after["audit_archive"] == before["audit_archive"],
        invariant="refused_restore_changes_no_archive_document",
        expected=before["audit_archive"],
        observed=after["audit_archive"],
    )
    added = set(after["audit"]) - set(before["audit"])
    require(
        condition=len(added) == 3,
        invariant="refused_restore_is_audited_once_per_attempt",
        expected=3,
        observed=sorted(added),
    )
    for key in added:
        document = json.loads((stack.audit_path / key).read_text(encoding="utf-8"))
        require(
            condition=document["outcome"] == "FORBIDDEN"
            and document["tenant_id"] == TENANT_A
            and document["reason_code"] == "tenant_forbidden",
            invariant="refusal_is_recorded_as_forbidden",
            expected={"outcome": "FORBIDDEN", "reason_code": "tenant_forbidden"},
            observed={
                "outcome": document["outcome"],
                "reason_code": document["reason_code"],
            },
        )
    observed_tenant_b = stack.control.query(AuditQuery(tenant_id=TENANT_B)).total
    require(
        condition=observed_tenant_b == tenant_b.total,
        invariant="refused_restore_never_reaches_the_other_tenant",
        expected=tenant_b.total,
        observed=observed_tenant_b,
    )


# ── §durability ──────────────────────────────────────────────────────────


@gate("TEST-CLOSURE-11", "durability")
def test_closure_11_archive_recovery_integrity_are_coherent(tmp_path: Path) -> None:
    from trendx.forecasting.audit import AuditQuery
    from trendx.forecasting.audit_control import AuditExportScope
    from trendx.forecasting.audit_lifecycle import AuditRestoreStatus, LifecycleStatus
    from trendx.forecasting.reporting import ExecutionAnalyticsReportingService

    stack = Stack(tmp_path)
    stack.seed(tenant_a=3, tenant_b=1)
    history = history_service(stack)

    report = ExecutionAnalyticsReportingService(history).report(query())
    require(
        condition=report.contract_version == "1",
        invariant="report_contract_version",
        expected="1",
        observed=str(report.contract_version),
    )
    require(
        condition=report.summary.total_executions == 3,
        invariant="report_counts_the_tenant_execution",
        expected=3,
        observed=report.summary.total_executions,
    )
    require(
        condition=stack.lifecycle.archive(TENANT_A, stack.policy(dry_run=False)).status
        is LifecycleStatus.SUCCESS,
        invariant="archive_succeeds",
        expected=LifecycleStatus.SUCCESS.name,
        observed="failure",
    )
    archived = {document.event.event_id for document in stack.audit_archive.list_documents()}
    require(
        condition=set(stack.tenant_a_event_ids) <= archived,
        invariant="every_tenant_event_is_archived",
        expected=sorted(stack.tenant_a_event_ids),
        observed=sorted(archived),
    )
    for event_id in stack.tenant_a_event_ids:
        require(
            condition=stack.audit_archive.verify(event_id).verified is True,
            invariant="archived_document_checksum_verifies",
            expected=True,
            observed=False,
        )
    purge = stack.lifecycle.purge(TENANT_A, stack.policy(dry_run=False))
    require(
        condition=purge.status is LifecycleStatus.SUCCESS
        and purge.integrity_failures == 0
        and purge.conflicts == 0,
        invariant="purge_after_verified_archive_is_clean",
        expected={"integrity_failures": 0, "conflicts": 0},
        observed={
            "status": purge.status.name,
            "integrity_failures": purge.integrity_failures,
            "conflicts": purge.conflicts,
        },
    )
    event_id = stack.tenant_a_event_ids[0]
    statuses = [
        stack.lifecycle.restore(
            restore_request(event_id, TENANT_A), authenticated_tenant=TENANT_A
        ).status
        for _ in range(3)
    ]
    require(
        condition=statuses[0] is AuditRestoreStatus.RESTORED
        and all(status is AuditRestoreStatus.ALREADY_PRESENT for status in statuses[1:]),
        invariant="restore_is_idempotent",
        expected=[AuditRestoreStatus.RESTORED.name, "ALREADY_PRESENT", "ALREADY_PRESENT"],
        observed=[status.name for status in statuses],
    )
    restored = [
        event
        for event in stack.control.query(AuditQuery(tenant_id=TENANT_A)).events
        if event.event_id == event_id
    ]
    require(
        condition=len(restored) == 1,
        invariant="restore_does_not_duplicate",
        expected=1,
        observed=len(restored),
    )
    integrity = stack.integrity.verify(TENANT_A)
    require(
        condition=integrity.integrity_status.value == "VALID",
        invariant="integrity_after_restore",
        expected="VALID",
        observed=integrity.integrity_status.value,
    )
    reconciliation = stack.reconciliation.reconcile(TENANT_A, lifecycle=stack.lifecycle)
    require(
        condition=reconciliation.tenant_id == TENANT_A,
        invariant="reconciliation_echoes_the_tenant",
        expected=TENANT_A,
        observed=reconciliation.tenant_id,
    )
    export = stack.export.export(TENANT_A, generated_at=AUDIT_INSTANT, lifecycle=stack.lifecycle)
    require(
        condition=export.scope
        in {AuditExportScope.ACTIVE_ONLY, AuditExportScope.ACTIVE_AND_ARCHIVED},
        invariant="export_scope_is_one_of_the_contractual_values",
        expected=sorted(member.name for member in AuditExportScope),
        observed=export.scope.name,
    )
    checksum = stack.export.checksum(TENANT_A, generated_at=AUDIT_INSTANT)
    require(
        condition=len(checksum["export_checksum"]) == 64,
        invariant="export_checksum_is_sha256",
        expected=64,
        observed=len(checksum["export_checksum"]),
    )


@gate("TEST-CLOSURE-21", "durability")
def test_closure_21_read_operations_are_read_only(tmp_path: Path) -> None:
    """Preuve Q : aucune des 11 lectures de la chaîne ne modifie un store."""

    from trendx.forecasting.analytics import ExecutionHistoryAnalytics
    from trendx.forecasting.reporting import ExecutionAnalyticsReportingService

    stack = Stack(tmp_path)
    stack.seed(tenant_a=3, tenant_b=1)
    history = history_service(stack)
    reads: dict[str, Callable[[], Any]] = {
        "history": lambda: history.query(query()),
        "statistics": lambda: history.statistics(query()),
        "analytics": lambda: ExecutionHistoryAnalytics(history).analyze(query()),
        "report": lambda: ExecutionAnalyticsReportingService(history).report(query()),
        "audit": lambda: stack.control.query(audit_query()),
        "integrity": lambda: stack.integrity.verify(TENANT_A),
        "reconciliation": lambda: stack.reconciliation.reconcile(TENANT_A),
        "export": lambda: stack.export.export(TENANT_A, generated_at=AUDIT_INSTANT),
        "health": lambda: stack.health.health(TENANT_A),
        "capacity": lambda: stack.health.capacity(TENANT_A),
        "readiness": lambda: stack.health.readiness(TENANT_A),
    }
    require(
        condition=tuple(reads) == CHAIN_READS,
        invariant="chain_read_set",
        expected=list(CHAIN_READS),
        observed=list(reads),
    )
    before = stack.fingerprint()
    mutated: list[str] = []
    for name, operation in reads.items():
        snapshot = stack.fingerprint()
        operation()
        if stack.fingerprint() != snapshot:
            mutated.append(name)
    require(
        condition=mutated == [],
        invariant="every_chain_read_is_read_only",
        expected=[],
        observed=mutated,
    )
    require(
        condition=stack.fingerprint() == before,
        invariant="datastore_digest_unchanged_by_the_read_chain",
        expected=before,
        observed=stack.fingerprint(),
    )


# ── §lifecycle ───────────────────────────────────────────────────────────


@gate("TEST-CLOSURE-10", "lifecycle")
def test_closure_10_w107_and_w112_use_separate_contracts(tmp_path: Path) -> None:
    from trendx.forecasting.audit_lifecycle import AuditArchiveStore, AuditLifecycleService
    from trendx.forecasting.execution import ExecutionStore
    from trendx.forecasting.lifecycle import ExecutionHistoryLifecycle

    stack = Stack(tmp_path)
    stack.seed(tenant_a=1, tenant_b=0)
    source = inspect.getsource(AuditLifecycleService)
    require(
        condition="ExecutionRecord" not in source,
        invariant="audit_lifecycle_never_handles_execution_records",
        expected="ExecutionRecord absent",
        observed="ExecutionRecord présent",
    )
    require(
        condition="ExecutionAuditEvent" in source,
        invariant="audit_lifecycle_handles_audit_events",
        expected="ExecutionAuditEvent présent",
        observed="ExecutionAuditEvent absent",
    )
    require(
        condition=ExecutionHistoryLifecycle is not None and ExecutionStore is not AuditArchiveStore,
        invariant="execution_and_audit_lifecycle_are_distinct_types",
        expected="types distincts",
        observed=(ExecutionHistoryLifecycle, ExecutionStore, AuditArchiveStore),
    )
    require(
        condition=stack.audit_archive is not stack.audit.store,
        invariant="archive_store_is_not_the_audit_journal",
        expected="objets distincts",
        observed="objet unique",
    )
    require(
        condition=stack.archive_path != stack.exec_path and stack.audit_path != stack.exec_path,
        invariant="lifecycle_datastores_are_separate",
        expected=(str(stack.archive_path), str(stack.audit_path), str(stack.exec_path)),
        observed=(str(stack.archive_path), str(stack.audit_path), str(stack.exec_path)),
    )


@gate("TEST-CLOSURE-24", "lifecycle")
def test_closure_24_purge_is_fail_closed(tmp_path: Path) -> None:
    from trendx.forecasting.audit import AuditQuery

    stack = Stack(tmp_path)
    stack.seed(tenant_a=2, tenant_b=0)
    stack.lifecycle.archive(TENANT_A, stack.policy(dry_run=False))

    refused = stack.lifecycle.purge(
        TENANT_A, stack.policy(archive_before_purge=False, dry_run=False)
    )
    require(
        condition=refused.purged == 0 and refused.guards.get("archive_missing", 0) > 0,
        invariant="purge_without_archive_removes_nothing",
        expected={"purged": 0, "archive_missing": "> 0"},
        observed={"purged": refused.purged, "guards": refused.guards},
    )
    for event_id in stack.tenant_a_event_ids:
        require(
            condition=stack.audit.get(event_id, tenant_id=TENANT_A) is not None,
            invariant="guarded_event_stays_in_the_journal",
            expected=event_id,
            observed=None,
            phase="W112",
        )

    victim = stack.tenant_a_event_ids[0]
    archive_file = next(
        path
        for path in stack.archive_path.rglob("*.json")
        if json.loads(path.read_text(encoding="utf-8"))["event"]["event_id"] == victim
    )
    archive_file.write_bytes(b"corrupted")
    require(
        condition=stack.audit_archive.verify(victim).verified is False,
        invariant="corrupted_archive_is_detected",
        expected=False,
        observed=True,
    )
    report = stack.lifecycle.purge(TENANT_A, stack.policy(dry_run=False))
    require(
        condition=report.guards.get("archive_corrupt") == 1,
        invariant="purge_guards_a_corrupted_archive",
        expected=1,
        observed=report.guards,
    )
    live = {event.event_id for event in stack.control.query(AuditQuery(tenant_id=TENANT_A)).events}
    require(
        condition=victim in live,
        invariant="guarded_event_is_never_deleted",
        expected=victim,
        observed=sorted(live),
    )


# ── §recovery ────────────────────────────────────────────────────────────


@gate("TEST-CLOSURE-16", "recovery")
def test_closure_16_recovery_invariants_hold(tmp_path: Path) -> None:
    from trendx.forecasting.audit_lifecycle import AuditRestoreStatus

    stack = Stack(tmp_path)
    stack.seed(tenant_a=2, tenant_b=0)

    missing = stack.lifecycle.restore(
        restore_request("absent", TENANT_A), authenticated_tenant=TENANT_A
    )
    require(
        condition=missing.status is AuditRestoreStatus.NOT_FOUND,
        invariant="restore_of_an_absent_archive_reports_not_found",
        expected=AuditRestoreStatus.NOT_FOUND.name,
        observed=missing.status.name,
    )

    stack.lifecycle.archive(TENANT_A, stack.policy(dry_run=False))
    victim = stack.tenant_a_event_ids[0]
    archive_file = next(
        path
        for path in stack.archive_path.rglob("*.json")
        if json.loads(path.read_text(encoding="utf-8"))["event"]["event_id"] == victim
    )
    good = archive_file.read_bytes()
    envelope = json.loads(good)
    envelope["event"] = {**envelope["event"], "event_version": "999"}
    envelope.pop("archive_document_checksum", None)
    archive_file.write_text(json.dumps(envelope), encoding="utf-8")
    require(
        condition=stack.audit_archive.verify(victim).verified is False,
        invariant="invalid_envelope_is_detected",
        expected=False,
        observed=True,
    )
    refused = stack.lifecycle.restore(
        restore_request(victim, TENANT_A), authenticated_tenant=TENANT_A
    )
    require(
        condition=refused.status is not AuditRestoreStatus.RESTORED,
        invariant="restore_refuses_an_invalid_archive",
        expected=f"!= {AuditRestoreStatus.RESTORED.name}",
        observed=refused.status.name,
    )
    archive_file.write_bytes(good)
    require(
        condition=stack.audit_archive.verify(victim).verified is True,
        invariant="archive_is_restored_to_a_valid_state",
        expected=True,
        observed=False,
    )
    restored = stack.lifecycle.restore(
        restore_request(victim, TENANT_A), authenticated_tenant=TENANT_A
    )
    require(
        condition=restored.status
        in {AuditRestoreStatus.RESTORED, AuditRestoreStatus.ALREADY_PRESENT},
        invariant="valid_archive_restores_after_repair",
        expected={AuditRestoreStatus.RESTORED.name, AuditRestoreStatus.ALREADY_PRESENT.name},
        observed=restored.status.name,
    )


# ── §integrity ───────────────────────────────────────────────────────────


@gate("TEST-CLOSURE-17", "integrity")
def test_closure_17_integrity_and_audit_event_contract_hold(tmp_path: Path) -> None:
    stack = Stack(tmp_path)
    stack.seed(tenant_a=2, tenant_b=0)

    event = stack.audit.get(stack.event_ids[0], tenant_id=TENANT_A)
    require(
        condition=event is not None,
        invariant="audit_event_is_readable",
        expected=stack.event_ids[0],
        observed=None,
    )
    require(
        condition=event.event_version == "1",
        invariant="audit_event_version",
        expected="1",
        observed=event.event_version,
    )
    require(
        condition=event.checksum == type(event).from_dict(event.to_dict()).calculated_checksum,
        invariant="audit_event_checksum_recomputes",
        expected=event.checksum,
        observed=type(event).from_dict(event.to_dict()).calculated_checksum,
    )
    require(
        condition=stack.integrity.verify(TENANT_A).integrity_status.value == "VALID",
        invariant="integrity_is_valid_on_a_clean_journal",
        expected="VALID",
        observed=stack.integrity.verify(TENANT_A).integrity_status.value,
    )

    target = next(path for path in stack.audit_path.rglob("*") if path.is_file())
    original = target.read_text(encoding="utf-8")
    document = json.loads(original)
    document["reason_code"] = "tampered"
    target.write_text(json.dumps(document), encoding="utf-8")
    tampered = stack.integrity.verify(TENANT_A)
    require(
        condition=tampered.integrity_status.value in {"INVALID", "ERROR"}
        and tampered.invalid_events > 0,
        invariant="tampering_is_detected",
        expected={"status": "INVALID or ERROR", "invalid_events": "> 0"},
        observed={
            "status": tampered.integrity_status.value,
            "invalid_events": tampered.invalid_events,
        },
    )
    target.write_text(original, encoding="utf-8")
    require(
        condition=stack.integrity.verify(TENANT_A).integrity_status.value == "VALID",
        invariant="integrity_recovers_after_restoration",
        expected="VALID",
        observed=stack.integrity.verify(TENANT_A).integrity_status.value,
    )

    malformed = next(
        path for path in stack.audit_path.rglob("*") if path.is_file() and path != target
    )
    backup = malformed.read_text(encoding="utf-8")
    malformed.write_bytes(b"{ not json")
    from trendx.forecasting.audit_health import BLOCKING_REASON_CODES

    readiness = stack.health.readiness(TENANT_A)
    require(
        condition=readiness.readiness_status.value == "NOT_READY"
        and set(readiness.reason_codes) & set(BLOCKING_REASON_CODES),
        invariant="a_corrupt_journal_blocks_readiness",
        expected={"readiness": "NOT_READY", "blocking_reason": "present"},
        observed={
            "readiness": readiness.readiness_status.value,
            "reason_codes": sorted(readiness.reason_codes),
        },
    )
    malformed.write_text(backup, encoding="utf-8")
    require(
        condition=stack.health.readiness(TENANT_A).readiness_status.value == "READY",
        invariant="readiness_recovers_after_restoration",
        expected="READY",
        observed=stack.health.readiness(TENANT_A).readiness_status.value,
    )


# ── §analytics ───────────────────────────────────────────────────────────


@gate("TEST-CLOSURE-18", "analytics")
def test_closure_18_analytics_stays_consistent(tmp_path: Path) -> None:
    from trendx.forecasting.analytics import ExecutionHistoryAnalytics
    from trendx.forecasting.audit_e2e import AuditE2EValidator, ConsistencyDimension, LifecycleState
    from trendx.forecasting.reporting import ExecutionAnalyticsReportingService

    stack = Stack(tmp_path)
    stack.seed(tenant_a=3, tenant_b=1)
    history = history_service(stack)
    analytics = ExecutionHistoryAnalytics(history).analyze(query())
    require(
        condition=analytics.tenant_id == TENANT_A,
        invariant="analytics_echoes_the_tenant",
        expected=TENANT_A,
        observed=analytics.tenant_id,
    )
    require(
        condition=analytics.summary.total_executions == history.query(query()).total,
        invariant="analytics_matches_the_history_projection",
        expected=history.query(query()).total,
        observed=analytics.summary.total_executions,
    )
    require(
        condition=analytics.summary.total_executions == 3,
        invariant="analytics_counts_only_the_requested_tenant",
        expected=3,
        observed=analytics.summary.total_executions,
    )

    validator = AuditE2EValidator(
        history=history,
        integrity=stack.integrity,
        reconciliation=stack.reconciliation,
        export=stack.export,
        health=stack.health,
        analytics=ExecutionHistoryAnalytics(history),
        reporting=ExecutionAnalyticsReportingService(history),
        lifecycle=stack.lifecycle,
    )
    require(
        condition=set(ConsistencyDimension)
        == {
            ConsistencyDimension.HISTORY,
            ConsistencyDimension.ANALYTICS,
            ConsistencyDimension.REPORT,
            ConsistencyDimension.INTEGRITY,
            ConsistencyDimension.ARCHIVE,
            ConsistencyDimension.RECONCILIATION,
            ConsistencyDimension.EXPORT,
            ConsistencyDimension.HEALTH,
            ConsistencyDimension.CAPACITY,
            ConsistencyDimension.READINESS,
        },
        invariant="consistency_dimensions_are_the_contractual_ten",
        expected=10,
        observed=len(set(ConsistencyDimension)),
    )
    first = validator.snapshot(LifecycleState.INITIAL, TENANT_A)
    second = validator.snapshot(LifecycleState.INITIAL, TENANT_A)
    verdict = validator.compare(first, second)
    require(
        condition={item.dimension for item in verdict} == set(ConsistencyDimension),
        invariant="every_dimension_is_compared",
        expected=sorted(member.name for member in ConsistencyDimension),
        observed=sorted({item.dimension.name for item in verdict}),
    )
    require(
        condition=all(item.is_consistent for item in verdict),
        invariant="a_stable_snapshot_is_consistent",
        expected=True,
        observed=[(item.dimension.name, item.is_consistent) for item in verdict],
    )
    absent = validator.snapshot(LifecycleState.INITIAL, "tenant-absent")
    missing = validator.compare(first, absent)
    require(
        condition=any(not item.is_consistent for item in missing),
        invariant="an_absent_tenant_is_never_reported_consistent",
        expected="au moins une dimension incohérente",
        observed=[(item.dimension.name, item.is_consistent) for item in missing],
    )


# ── §reporting ───────────────────────────────────────────────────────────


@gate("TEST-CLOSURE-19", "reporting")
def test_closure_19_reporting_contract_holds(tmp_path: Path) -> None:
    from trendx.forecasting.analytics import ExecutionHistoryAnalytics
    from trendx.forecasting.history import MAX_HISTORY_PAGE_SIZE
    from trendx.forecasting.reporting import (
        REPORT_CONTRACT_VERSION,
        ExecutionAnalyticsReportingService,
    )

    stack = Stack(tmp_path)
    stack.seed(tenant_a=3, tenant_b=2)
    history = history_service(stack)
    analytics = ExecutionHistoryAnalytics(history).analyze(query())
    report = ExecutionAnalyticsReportingService(history).report(query())

    require(
        condition=str(report.contract_version) == "1" and REPORT_CONTRACT_VERSION == "1",
        invariant="report_contract_version",
        expected="1",
        observed=(str(report.contract_version), REPORT_CONTRACT_VERSION),
    )
    require(
        condition=report.generated_at is None,
        invariant="report_is_deterministic_in_deterministic_mode",
        expected=None,
        observed=report.generated_at,
    )
    require(
        condition=report.tenant_context.tenant_id == TENANT_A,
        invariant="report_tenant_context",
        expected=TENANT_A,
        observed=report.tenant_context.tenant_id,
    )
    require(
        condition=report.summary.total_executions == analytics.summary.total_executions,
        invariant="report_and_analytics_agree",
        expected=analytics.summary.total_executions,
        observed=report.summary.total_executions,
    )
    require(
        condition=MAX_HISTORY_PAGE_SIZE == 1000,
        invariant="bounded_pagination",
        expected=1000,
        observed=MAX_HISTORY_PAGE_SIZE,
    )


# ── §health ──────────────────────────────────────────────────────────────


@gate("TEST-CLOSURE-20", "health")
def test_closure_20_health_capacity_readiness_invariants(tmp_path: Path) -> None:
    from trendx.forecasting.audit_health import (
        ARCHIVE_CORRUPTED,
        AUDIT_DATA_CORRUPTED,
        BLOCKING_REASON_CODES,
        AuditHealthService,
    )

    empty = Stack(tmp_path / "empty")
    empty_status = empty.health.health(TENANT_A)
    require(
        condition=empty_status.status.value == "EMPTY",
        invariant="an_empty_journal_reports_empty",
        expected="EMPTY",
        observed=empty_status.status.value,
    )
    require(
        condition="AUDIT_JOURNAL_EMPTY" in empty_status.reason_codes
        and "AUDIT_JOURNAL_EMPTY" not in BLOCKING_REASON_CODES,
        invariant="emptiness_is_informational_not_blocking",
        expected="motif informatif hors blocking codes",
        observed=sorted(empty_status.reason_codes),
    )
    require(
        condition=empty.health.readiness(TENANT_A).readiness_status.value == "READY",
        invariant="an_empty_journal_is_ready",
        expected="READY",
        observed=empty.health.readiness(TENANT_A).readiness_status.value,
    )

    seeded = Stack(tmp_path / "seeded")
    seeded.seed(tenant_a=2, tenant_b=0)
    require(
        condition=seeded.health.health(TENANT_A).status.value == "HEALTHY",
        invariant="a_seeded_journal_is_healthy",
        expected="HEALTHY",
        observed=seeded.health.health(TENANT_A).status.value,
    )
    require(
        condition=seeded.health.readiness(TENANT_A).readiness_status.value == "READY",
        invariant="a_seeded_journal_is_ready",
        expected="READY",
        observed=seeded.health.readiness(TENANT_A).readiness_status.value,
    )

    corrupt = Stack(tmp_path / "corrupt")
    corrupt.seed(tenant_a=1, tenant_b=0)
    next(path for path in corrupt.audit_path.rglob("*") if path.is_file()).write_bytes(b"{ x")
    corrupted = corrupt.health.health(TENANT_A)
    require(
        condition=corrupted.status.value in {"DEGRADED", "UNAVAILABLE"},
        invariant="a_corrupt_journal_is_degraded",
        expected={"DEGRADED", "UNAVAILABLE"},
        observed=corrupted.status.value,
    )
    require(
        condition=ARCHIVE_CORRUPTED in BLOCKING_REASON_CODES
        and AUDIT_DATA_CORRUPTED in BLOCKING_REASON_CODES,
        invariant="blocking_reason_codes_are_declared",
        expected=[ARCHIVE_CORRUPTED, AUDIT_DATA_CORRUPTED],
        observed=sorted(BLOCKING_REASON_CODES),
    )
    require(
        condition=corrupt.health.readiness(TENANT_A).readiness_status.value == "NOT_READY",
        invariant="a_corrupt_journal_is_not_ready",
        expected="NOT_READY",
        observed=corrupt.health.readiness(TENANT_A).readiness_status.value,
    )

    bare = AuditHealthService(seeded.audit, active_path=seeded.audit_path)
    snapshot = bare.snapshot(TENANT_A)
    require(
        condition=snapshot.archive.archive_status.value == "NOT_CONFIGURED",
        invariant="an_unconfigured_archive_is_reported_as_such",
        expected="NOT_CONFIGURED",
        observed=snapshot.archive.archive_status.value,
    )

    # Les valeurs ``filesystem`` changent selon la machine : seules les
    # projections métier doivent être stables.
    first = seeded.health.capacity(TENANT_A)
    second = seeded.health.capacity(TENANT_A)
    for field in (
        "event_count",
        "active_bytes",
        "active_file_count",
        "archive_bytes",
        "archive_file_count",
        "archived_event_count",
        "total_known_event_count",
    ):
        require(
            condition=getattr(first, field) == getattr(second, field),
            invariant="capacity_business_projection_is_stable",
            expected=getattr(first, field),
            observed=getattr(second, field),
        )
    require(
        condition=first.filesystem is not None,
        invariant="filesystem_observation_is_reported",
        expected="une observation filesystem",
        observed=None,
    )


# ── §documentation ───────────────────────────────────────────────────────


@gate("TEST-CLOSURE-12", "documentation")
def test_closure_12_documents_exist_and_are_referenced() -> None:
    chain_index = (DOCS / "execution-audit-chain.md").read_text(encoding="utf-8")
    matrix = (DOCS / "execution-audit-cross-reference.md").read_text(encoding="utf-8")
    for name in CHAIN_DOCS:
        require(
            condition=(DOCS / name).exists(),
            invariant="chain_document_exists",
            expected=name,
            observed="absent",
        )
    for phase, _commit, _subject in CHAIN:
        document = DOC_FOR_PHASE[phase]
        require(
            condition=(DOCS / document).exists(),
            invariant="phase_document_exists",
            expected=document,
            observed="absent",
            phase=phase,
        )
        if phase == "W102":
            require(
                condition="w101-execution-history-api.md#w102--phase-sans-code" in chain_index,
                invariant="w102_is_documented_as_absent",
                expected="ancre w102 dans le guide",
                observed="ancre absente",
                phase=phase,
            )
            continue
        require(
            condition=document in chain_index,
            invariant="phase_document_is_referenced",
            expected=document,
            observed="non référencé",
            phase=phase,
        )
    require(
        condition="UNDOCUMENTED" not in matrix.replace("`UNDOCUMENTED`", ""),
        invariant="cross_reference_has_no_undocumented_cell",
        expected="aucune cellule UNDOCUMENTED",
        observed="cellule UNDOCUMENTED présente",
    )
    for phase, _commit, _subject in CHAIN:
        if phase == "W102":
            continue
        require(
            condition=phase in matrix,
            invariant="cross_reference_lists_the_phase",
            expected=phase,
            observed="phase absente de la matrice",
            phase=phase,
        )
    link_pattern = re.compile(r"\[[^\]]+\]\(([^)#][^)]*?)(?:#[^)]*)?\)")
    broken: list[tuple[str, str]] = []
    for name in CHAIN_DOCS:
        body = (DOCS / name).read_text(encoding="utf-8")
        for target in link_pattern.findall(body):
            if target.startswith(("http://", "https://", "mailto:")):
                continue
            if not (DOCS / target).exists():
                broken.append((name, target))
    require(
        condition=broken == [],
        invariant="documentation_links_resolve",
        expected=[],
        observed=broken,
    )
    require(
        condition="PASS-W115-EXECUTION-AUDIT-PRODUCTION-READINESS"
        in (DOCS / "w115-execution-audit-production-readiness.md").read_text(encoding="utf-8"),
        invariant="w115_verdict_is_published",
        expected="PASS-W115-EXECUTION-AUDIT-PRODUCTION-READINESS",
        observed="verdict absent",
    )


@gate("TEST-CLOSURE-13", "documentation")
def test_closure_13_documentation_references_no_fictional_route() -> None:
    paths = set(live_openapi()["paths"])
    fictional: list[tuple[str, str]] = []
    for name in CHAIN_DOCS:
        body = (DOCS / name).read_text(encoding="utf-8")
        for route in re.findall(r"/api/v1/forecast/executions[\w/{}.-]*", body):
            if route.rstrip(".,)`\"'*:") not in paths:
                fictional.append((name, route))
    require(
        condition=fictional == [],
        invariant="no_fictional_route_in_documentation",
        expected=[],
        observed=fictional,
    )
    for name in CHAIN_DOCS:
        text = (DOCS / name).read_text(encoding="utf-8")
        for forbidden in (
            "nous garantissons",
            "garantit un temps",
            "temps de réponse garanti",
            "sous la seconde",
        ):
            require(
                condition=forbidden not in text.lower(),
                invariant="no_documentation_claims_a_guaranteed_latency",
                expected=f"« {forbidden} » absent",
                observed=f"« {forbidden} » présent",
            )


# ── §production-safety ───────────────────────────────────────────────────


@gate("TEST-CLOSURE-14", "production-safety")
def test_closure_14_production_flags_stay_off(tmp_path: Path) -> None:
    environment = os.environ.copy()
    environment.update({name: "false" for name, _attribute in PRODUCTION_FLAGS})
    environment["PYTHONPATH"] = str(SRC) + os.pathsep + environment.get("PYTHONPATH", "")
    attributes = [attribute for _name, attribute in PRODUCTION_FLAGS]
    probe = (
        f"import json,sys; sys.path.insert(0, {str(SRC)!r});"
        "from trendx.config import settings;"
        f"print(json.dumps({{k: getattr(settings, k) for k in {attributes!r}}}))"
    )
    observed = subprocess.run(
        [sys.executable, "-c", probe],
        env=environment,
        cwd=REPO,
        capture_output=True,
        text=True,
        check=False,
        timeout=300,
    )
    require(
        condition=observed.returncode == 0,
        invariant="production_flag_probe_runs",
        expected=0,
        observed=observed.returncode,
    )
    values = json.loads(observed.stdout.strip().splitlines()[-1])
    require(
        condition=values == {attribute: False for _name, attribute in PRODUCTION_FLAGS},
        invariant="production_flags_are_off_in_the_gate_context",
        expected={name: "false" for name, _attribute in PRODUCTION_FLAGS},
        observed=values,
    )
    del tmp_path


@gate("TEST-CLOSURE-15", "production-safety")
def test_closure_15_gate_never_mutates_business_artefacts() -> None:
    """Le gate ne modifie aucun artefact métier : il observe, il n'écrit que
    dans ``tmp_path``."""

    before = business_tree_digest()
    report = run_gate(skip={"TEST-CLOSURE-15"})
    after = business_tree_digest()
    require(
        condition=before == after,
        invariant="gate_leaves_business_artefacts_untouched",
        expected=before,
        observed=after,
    )
    require(
        condition=report["RESULT"] == "PASS",
        invariant="gate_runs_to_completion",
        expected="PASS",
        observed=report["RESULT"],
    )
    status_keys = [phase for phase, _commit, _subject in CHAIN]
    status_keys += [SECTION_REPORT_KEYS[section] for section in GATE_SECTIONS]
    require(
        condition=all(report[key] in {"PASS", "N/A"} for key in status_keys),
        invariant="no_section_reports_failure",
        expected="PASS ou N/A sur les 19 phases et les 13 sections",
        observed={key: report[key] for key in status_keys if report[key] not in {"PASS", "N/A"}},
    )


@gate("TEST-CLOSURE-25", "production-safety")
def test_closure_25_no_automatic_runtime_trigger_exists() -> None:
    from trendx.forecasting.audit_health import AuditHealthService
    from trendx.forecasting.audit_lifecycle import AuditLifecycleService, AuditRetentionPolicy

    for service in (AuditLifecycleService, AuditHealthService):
        source = inspect.getsource(service)
        for forbidden in ("asyncio", "APScheduler", "cron", "while True", "sleep("):
            require(
                condition=forbidden not in source,
                invariant="no_automatic_trigger_in_the_audit_lifecycle",
                expected=f"« {forbidden} » absent",
                observed=f"« {forbidden} » présent",
            )
    require(
        condition=AuditRetentionPolicy(retention_days=1, reference_time=NOW).dry_run is True,
        invariant="retention_policy_defaults_to_dry_run",
        expected=True,
        observed=AuditRetentionPolicy(retention_days=1, reference_time=NOW).dry_run,
    )


@gate("TEST-CLOSURE-26", "contract")
def test_closure_26_global_result_never_masks_a_failing_section() -> None:
    """Aucun PASS global si une sous-section échoue."""

    failing = build_report(
        phase_status={phase: "PASS" for phase, _commit, _subject in CHAIN},
        section_status={section: "PASS" for section in GATE_SECTIONS},
    )
    require(
        condition=failing["RESULT"] == "PASS",
        invariant="a_clean_gate_passes",
        expected="PASS",
        observed=failing["RESULT"],
    )
    for section in GATE_SECTIONS:
        report = build_report(
            phase_status={phase: "PASS" for phase, _commit, _subject in CHAIN},
            section_status={
                candidate: "FAIL" if candidate == section else "PASS" for candidate in GATE_SECTIONS
            },
        )
        require(
            condition=report["RESULT"] == "FAIL",
            invariant="a_failing_section_forces_a_global_fail",
            expected=f"FAIL (section {section})",
            observed=report["RESULT"],
        )
    report = build_report(
        phase_status={phase: "FAIL" if phase == "W112" else "PASS" for phase, _c, _s in CHAIN},
        section_status={section: "PASS" for section in GATE_SECTIONS},
    )
    require(
        condition=report["W112"] == "FAIL" and report["RESULT"] == "FAIL",
        invariant="a_failing_phase_forces_a_global_fail",
        expected={"W112": "FAIL", "RESULT": "FAIL"},
        observed={"W112": report["W112"], "RESULT": report["RESULT"]},
    )
    report = build_report(
        phase_status={phase: "PASS" for phase, _c, _s in CHAIN},
        section_status={
            section: "NOT_RUN" if section == "health" else "PASS" for section in GATE_SECTIONS
        },
    )
    require(
        condition=report["RESULT"] == "FAIL",
        invariant="an_unrun_section_forces_a_global_fail",
        expected="FAIL",
        observed=report["RESULT"],
    )
    require(
        condition=failing["W102"] == "N/A",
        invariant="w102_stays_not_applicable_in_the_report",
        expected="N/A",
        observed=failing["W102"],
    )


# ── exécution du gate ────────────────────────────────────────────────────


def build_report(*, phase_status: dict[str, str], section_status: dict[str, str]) -> dict[str, str]:
    """Rapport structuré §5.6 — ``RESULT`` n'est jamais ``PASS`` si une
    section est ``FAIL``/``NOT_RUN`` ou si une phase échoue."""

    report: dict[str, str] = {
        "GATE": "global-closure",
        "VERSION": GLOBAL_CLOSURE_GATE_VERSION,
        "BASELINE": GATE_BASELINE,
    }
    for phase, _commit, _subject in CHAIN:
        # W102 n'a jamais existé comme implémentation : le rapport ne peut
        # jamais afficher autre chose que N/A pour cette phase.
        report[phase] = "N/A" if phase == "W102" else phase_status[phase]
    for section in GATE_SECTIONS:
        report[SECTION_REPORT_KEYS[section]] = section_status[section]
    outcomes = [report[phase] for phase, _commit, _subject in CHAIN]
    outcomes += [report[SECTION_REPORT_KEYS[section]] for section in GATE_SECTIONS]
    report["RESULT"] = "PASS" if all(outcome in {"PASS", "N/A"} for outcome in outcomes) else "FAIL"
    return report


def run_gate(*, skip: set[str] | None = None) -> dict[str, str]:
    """Exécute chaque section du gate et renvoie le rapport structuré."""

    skipped = skip or set()
    phase_status = {phase: evaluate_phase(phase)[0] for phase, _commit, _subject in CHAIN}
    section_status = {section: "NOT_RUN" for section in GATE_SECTIONS}
    failures: list[str] = []

    for check in CHECKS:
        if check.check_id in skipped:
            continue
        try:
            if "tmp_path" in inspect.signature(check.func).parameters:
                # les contrôles qui manipulent des données les manipulent
                # exclusivement dans un répertoire jetable : jamais dans src/
                with tempfile.TemporaryDirectory(prefix="w118-gate-") as directory:
                    check.func(Path(directory))
            else:
                check.func()
        except GateFailureError as failure:
            section_status[check.section] = "FAIL"
            failures.append(f"{check.check_id}: {failure}")
        except Exception as error:  # un contrôle qui plante échoue
            section_status[check.section] = "FAIL"
            failures.append(f"{check.check_id}: GATE ERROR | {error!r}")
        else:
            if section_status[check.section] != "FAIL":
                section_status[check.section] = "PASS"

    if any(status == "FAIL" for status in phase_status.values()):
        section_status["contract"] = "FAIL"

    report = build_report(phase_status=phase_status, section_status=section_status)
    report["_FAILURES"] = json.dumps(failures, ensure_ascii=False, indent=2)
    return report


def format_report(report: dict[str, str]) -> str:
    lines = [f"{key} = {value}" for key, value in report.items() if key != "_FAILURES"]
    return "\n".join(lines)


def test_global_closure_gate_report_is_pass() -> None:
    """Le gate complet : rapport structuré, aucune section en échec."""

    report = run_gate()
    failures = report.pop("_FAILURES", "[]")
    require(
        condition=tuple(report) == REPORT_KEYS,
        invariant="report_keys_match_the_contract",
        expected=REPORT_KEYS,
        observed=tuple(report),
    )
    require(
        condition=report["VERSION"] == GLOBAL_CLOSURE_GATE_VERSION,
        invariant="report_version",
        expected=GLOBAL_CLOSURE_GATE_VERSION,
        observed=report["VERSION"],
    )
    require(
        condition=report["BASELINE"] == GATE_BASELINE,
        invariant="report_baseline",
        expected=GATE_BASELINE,
        observed=report["BASELINE"],
    )
    require(
        condition=report["W102"] == "N/A",
        invariant="report_w102",
        expected="N/A",
        observed=report["W102"],
    )
    require(
        condition=report["RESULT"] == "PASS",
        invariant="global_result",
        expected="PASS",
        observed={"result": report["RESULT"], "failures": failures},
    )
    print("\n" + format_report(report))


@pytest.fixture(scope="module")
def gate_report() -> Iterator[dict[str, str]]:
    """Rapport unique partagé par les tests qui en ont besoin."""

    report = run_gate()
    report.pop("_FAILURES", None)
    yield report
