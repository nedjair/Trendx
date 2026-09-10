"""OPTION A : customer explicite fail-closed pour le training.

A : UUID valide accepté ; B : variable absente -> fail-closed ;
C : UUID invalide -> fail-closed ; D : deux customers distincts ;
E : aucune dérivation tenant -> customer.
"""

from __future__ import annotations

import uuid

import pytest
from trendx.config import settings
from trendx.services.training import resolve_customer_scope

CUST_A = str(uuid.uuid4())
CUST_B = str(uuid.uuid4())


def test_a_valid_customer_accepted(monkeypatch) -> None:
    monkeypatch.setattr(settings, "trendx_default_customer_id", "")
    assert resolve_customer_scope(CUST_A) == CUST_A


def test_b_missing_customer_fails_closed(monkeypatch) -> None:
    monkeypatch.setattr(settings, "trendx_default_customer_id", "")
    with pytest.raises(ValueError, match="customer_id explicite requis"):
        resolve_customer_scope(None)


def test_c_invalid_uuid_fails_closed(monkeypatch) -> None:
    monkeypatch.setattr(settings, "trendx_default_customer_id", "")
    with pytest.raises(ValueError, match="UUID attendu"):
        resolve_customer_scope("not-a-uuid")
    monkeypatch.setattr(settings, "trendx_default_customer_id", "tenant-xyz")
    with pytest.raises(ValueError, match="UUID attendu"):
        resolve_customer_scope(None)


def test_d_distinct_customers_distinct_contexts() -> None:
    assert resolve_customer_scope(CUST_A) != resolve_customer_scope(CUST_B)


def test_e_no_tenant_derivation(monkeypatch) -> None:
    monkeypatch.setattr(settings, "trendx_default_customer_id", "")
    monkeypatch.setattr(settings, "trendx_default_tenant_id", str(uuid.uuid4()))
    with pytest.raises(ValueError, match="customer_id explicite requis"):
        resolve_customer_scope(None)


def test_env_default_used_when_no_arg(monkeypatch) -> None:
    monkeypatch.setattr(settings, "trendx_default_customer_id", CUST_A)
    assert resolve_customer_scope(None) == CUST_A
    assert resolve_customer_scope(CUST_B) == CUST_B  # explicite prioritaire
