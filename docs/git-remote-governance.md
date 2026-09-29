# Git Remote Governance — TrendX (W119 Phase 9B)

> Statut : constats prouvés uniquement. Toute valeur non déterminée est
> explicitement marquée `UNDECLARED`. Ce document ne déclare aucune source de
> vérité par défaut : la désignation d'un dépôt canonique relève d'une décision
> opérateur distincte.

## Git Source of Truth

```text
Official repository: UNDECLARED (no governance declaration found in
  AGENTS.md, README, docs/, Makefile, or CI configuration as of W119)
Canonical branch:    UNDECLARED (operational main branch observed: `master`;
  no release/tag process observed)
```

## Remotes (observed, read-only inventory)

```text
origin
  URL:  git@github.com:nedjair/Trendx.git
  Role: PUBLICATION_TARGET (W119 f71b3f5 published here, Phase 7, after
    explicit Phase 6 authorization). No CI observed. Not canonical.

gitlab
  URL:  git@gitlab.com:raouf-groupe/trendx.git
  Role: SECONDARY_CI + partial UPSTREAM (GitLab CI on master/MR/push;
    upstream of ~10 local branches). Not canonical.

gitea-migration
  URL:  http://10.0.0.101:3000/admin/trendx-migration.git
  Role: PRIMARY_CI + CONTAINER_REGISTRY + partial UPSTREAM.
    Gitea 1.25.4 ACTIVE (proven HTTP 200); public repository
    `admin/trendx-migration` (default branch `master`); upstream of local
    `master` and ~6 feature branches; 60 remote-tracking refs; wired into
    `.env` (GITEA_URL/OWNER/REPO) and `scripts/mcp-gitea.sh`.
    Active != canonical: CANONICAL = UNDECLARED.
```

## Gitea role (Q7)

```text
canonical: NO (undeclared)
upstream:  YES (master + several branches)
mirror:    UNKNOWN (server flag `mirror=false`; "migration mirror" label
  in description is not operational proof)
CI:        YES (Gitea Actions, push + pull_request)
registry:  YES (10.0.0.101:3000/trendx/* — the only registry referenced)
```

## GitLab role

```text
GitLab CI (master, merge_request_event, push). Upstream of ~10 branches.
Not canonical.
```

## GitHub / origin role

```text
Publication target for W119 (explicitly authorized). No CI detected.
Not canonical.
```

## Container registry

```text
REGISTRY (observed): 10.0.0.101:3000/trendx/*
GIT_SOURCE_OF_TRUTH and CONTAINER_REGISTRY are separate concerns.
```

## Deployment source

```text
DEPLOYMENT_SOURCE = REGISTRY/ARTIFACTS (no git remote is consumed
directly by Makefile/docker/deployment docs).
```

## Branch / upstream observations

```text
Local `master` tracks gitea-migration/master. Roughly ten local branches
track gitlab/*, roughly six track gitea-migration/*. The W119 branch
fix/ml-w49-master-conflicts tracks nothing. No `main`, `develop`, or
`release/*` branches observed; tags are ad hoc (no release process).
Upstream links are operational facts, not canonicity proof.
```

## Follow-up (human decision required)

## Follow-up (human decision required)

```text
1. Declare the official source of truth (repository + canonical branch).
2. Define Gitea's exact role and whether W119 must be synchronized there.
3. Resolve the AGENTS.md host-vs-service ambiguity for 10.0.0.101.
4. Define publication, synchronization, and divergence policies.
None of the above is executed or pre-decided by this document.
```

## Synchronization policy

```text
Source -> Gitea:   UNDECLARED (DO_NOT_SYNCHRONIZE until declared)
Source -> GitLab:  UNDECLARED (DO_NOT_SYNCHRONIZE until declared)
Gitea -> Source:   UNDECLARED
GitLab -> Source:  UNDECLARED
W119_GITEA_SYNC_POLICY = UNDECLARED (governance decision required;
  W119 currently on origin ONLY: W119_ON_GITEA = UNKNOWN, W119_ON_GITLAB = UNKNOWN)
```

## Publication policy

```text
WHO_CAN_PUSH / WHAT_BRANCHES / WHAT_TAGS / WHAT_REMOTE: UNDECLARED in general.
Observed precedent: W119 publication to origin/fix/ml-w49-master-conflicts
was explicitly authorized (Phase 6: REMOTE + BRANCH + PUSH_AUTHORIZATION).
REQUIRES_REVIEW / REQUIRES_CI / FORCE_PUSH_ALLOWED: UNDECLARED.
FORCE_PUSH: never authorized by default.
```

## Divergence policy (target procedure, documentary only)

```text
IF remotes diverge:
  DO NOT AUTO-MERGE
  DO NOT AUTO-FORCE-PUSH
  STOP SYNCHRONIZATION
  OPEN GOVERNANCE INCIDENT
  COMPARE COMMITS
  IDENTIFY AUTHORITY (currently UNDECLARED -> escalate to operator)
  RESOLVE FROM SOURCE_OF_TRUTH (once declared)
```

## W119 publication

```text
W119_SHA = f71b3f5466cd39dcb4b4e0e14908c59eb09d187c
W119_ON_ORIGIN = YES (Phase 7, verified ls-remote exact)
W119_ON_GITEA = UNKNOWN (no fetch performed)
W119_ON_GITLAB = UNKNOWN (no fetch performed)
```

## Known governance ambiguity (documented, not resolved here)

```text
AGENTS.md states 10.0.0.101 abandoned with no references to subsist, while
tracked .env and W118-frozen .gitea/workflows/ci.yaml reference
10.0.0.101:3000 operationally (Gitea + registry). Distinguish HOST_ROLE
(deployment target, disk) from SERVICE_ROLE (Gitea/registry active).
Resolution requires a governance decision outside W119.
```

## Compatibility

```text
This document adds no code, SQL, test, CI, remote, branch, registry, or
secret change. It does not contradict single-DB architecture, W118 rules,
the W119 migration contract, deployment constraints, or security rules.
```
