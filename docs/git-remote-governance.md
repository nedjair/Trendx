# Git Remote Governance — TrendX (decided W120 Phase 2, designated W121 Phase 2, implemented W121 Phase 3)

> Statut : décisions opérateur implémentées ci-dessous. Les faits
> restent distingués des décisions : tout rôle non décidé est marqué
> `UNDECLARED`. Publier vers un remote ou l'utiliser ne constitue aucune
> désignation de canonicité.

## Operator decisions (W120 Phase 2, implemented here)

```text
SOURCE_OF_TRUTH = gitea-migration
GITEA_POLICY = D2-A
GIT_SYNC = NO
AGENTS_POLICY = D3-A
PUBLICATION_POLICY = D4-A
CANONICAL_BRANCH = master
```

## Git Source of Truth

```text
Official repository: gitea-migration
  (http://10.0.0.101:3000/admin/trendx-migration.git)
  DESIGNATED BY OPERATOR (W120 Phase 2, decision D1), not inferred from
  existence, reachability, or usage. Prior W119 state was UNDECLARED.
Canonical branch:    master
  DESIGNATED BY OPERATOR (W121 Phase 2, decision D1 = DESIGNATE_MASTER).
  Gitea default branch (`master`) and canonical branch are now aligned values
  but remain distinct concepts. Prior state was UNDECLARED. This designation
  implies NO protection, NO auto-merge, NO sync, and NO automatic publication.
```

## Remotes (observed, read-only inventory)

```text
origin
  URL:  git@github.com:nedjair/Trendx.git
  Role: non-canonical publication/legacy remote (SOURCE_OF_TRUTH = NO).
    W119 f71b3f5 was published here on explicit Phase 6 authorization;
    that publication did not designate origin as source of truth.
    No CI observed. No new push authorized by this document.

gitlab
  URL:  git@gitlab.com:raouf-groupe/trendx.git
  Role: secondary CI / additional remote (SOURCE_OF_TRUTH = NO).
    GitLab CI on master/MR/push; upstream of ~10 local branches.
    GIT_SYNC = NO. No fetch or synchronization performed.

gitea-migration
  URL:  http://10.0.0.101:3000/admin/trendx-migration.git
  Role: SOURCE_OF_TRUTH (operator-designated, W120 Phase 2 D1).
    Gitea 1.25.4 ACTIVE (proven HTTP 200); public repository
    `admin/trendx-migration` (default branch `master`); upstream of local
    `master` and ~6 feature branches; 60 remote-tracking refs; wired into
    `.env` (GITEA_URL/OWNER/REPO) and `scripts/mcp-gitea.sh`.
    CI = ACTIVE, REGISTRY = ACTIVE, GIT_SYNC = NO (policy D2-A below).
```

## Gitea role (Q7, decided D2-A)

```text
source_of_truth: YES (operator-designated, D1)
canonical:       branch master (designated W121 Phase 2; PROTECTION UNDECIDED)
upstream:  YES (master + several branches)
mirror:    UNKNOWN (server flag `mirror=false`; "migration mirror" label
  in description is not operational proof)
CI:        YES / ACTIVE (Gitea Actions, push + pull_request)
registry:  YES / ACTIVE (10.0.0.101:3000/trendx/* — the only registry referenced)
git_sync:  NO (policy D2-A: no automatic or mandatory Git synchronization
  to any other remote; any additional sync needs a separate decision)
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

```text
1. DONE (W120 D1 + W121 D1): source of truth = gitea-migration,
   canonical branch = master.
2. Define Gitea's exact role and whether W119 must be synchronized there.
3. Resolve the AGENTS.md host-vs-service ambiguity for 10.0.0.101.
4. Define publication, synchronization, and divergence policies.
Items 2-4 are not executed or pre-decided by this document.
```

## Synchronization policy (decided D2-A)

```text
Source -> Gitea:   NO SYNC DEFINED (DO_NOT_SYNCHRONIZE until a separate decision)
Source -> GitLab:  NO SYNC DEFINED (DO_NOT_SYNCHRONIZE until a separate decision)
Gitea -> Source:   NO SYNC DEFINED (gitea-migration IS the designated source;
  no mirror mechanism created by this policy)
GitLab -> Source:  NO SYNC DEFINED
W119_GITEA_SYNC_POLICY = NO (D2-A: W119 currently on origin ONLY;
  W119_ON_GITEA = UNKNOWN, W119_ON_GITLAB = UNKNOWN; no fetch performed)
```

## Publication policy (decided D4-A)

```text
PUBLICATION_POLICY = D4-A: no push without prior explicit operator
authorization for the targeted remote AND branch. No implicit push, no
automatic multi-remote push, no target deduced from local configuration.
Each publication requires its own explicit phase or authorization.
WHO_CAN_PUSH / WHAT_BRANCHES / WHAT_TAGS: per-authorization (no standing rule).
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
  IDENTIFY AUTHORITY (gitea-migration / master per W120-W121 decisions)
  RESOLVE FROM SOURCE_OF_TRUTH (gitea-migration, canonical branch master)
```

## W119 publication

```text
W119_SHA = f71b3f5466cd39dcb4b4e0e14908c59eb09d187c
W119_ON_ORIGIN = YES (Phase 7, verified ls-remote exact)
W119_ON_GITEA = UNKNOWN (no fetch performed)
W119_ON_GITLAB = UNKNOWN (no fetch performed)
```

## Known governance ambiguity (decided D3-A, documented only)

```text
AGENTS.md states 10.0.0.101 abandoned with no references to subsist, while
tracked .env and W118-frozen .gitea/workflows/ci.yaml reference
10.0.0.101:3000 operationally (Gitea + registry). Decided interpretation
(D3-A): the AGENTS.md rule concerns the HOST_ROLE (deployment target,
disk), not the SERVICE_ROLE (Gitea/registry active, which keep operating).
AGENTS.md is NOT modified by W120 Phase 3; no historical resolution claimed.
```

## Compatibility

```text
This document adds no code, SQL, test, CI, remote, branch, registry, or
secret change. It does not contradict single-DB architecture, W118 rules,
the W119 migration contract, deployment constraints, or security rules.
```
