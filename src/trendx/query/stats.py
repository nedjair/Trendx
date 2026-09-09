"""Descriptive stats with explicit edge-case rules (deterministic)."""

from __future__ import annotations

import math
from typing import Any

import numpy as np
import numpy.typing as npt
import pandas as pd


def describe_series(values: pd.Series | list[float] | npt.NDArray[np.float64]) -> dict[str, Any]:
    """Return n/min/max/mean/std/sum/null_count.

    Rules: empty -> n=0, all None, null_count=0; nulls/NaN excluded from
    computations, counted in null_count; single obs -> std=None;
    constant -> std=0.0; pandas sample std (ddof=1).
    """
    s = (
        pd.Series(values, dtype="float64")
        if not isinstance(values, pd.Series)
        else values.astype("float64")
    )
    null_count = int(s.isna().sum())
    clean = s.dropna()
    n = int(len(clean))
    if n == 0:
        return {
            "n": 0,
            "min": None,
            "max": None,
            "mean": None,
            "std": None,
            "sum": None,
            "null_count": null_count,
        }
    out = {
        "n": n,
        "min": float(clean.min()),
        "max": float(clean.max()),
        "mean": float(clean.mean()),
        "std": float(clean.std(ddof=1)) if n > 1 else None,
        "sum": float(clean.sum()),
        "null_count": null_count,
    }
    if n > 1 and out["std"] is not None and math.isnan(out["std"]):
        out["std"] = 0.0
    return out
