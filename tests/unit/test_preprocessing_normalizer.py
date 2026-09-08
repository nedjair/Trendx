from __future__ import annotations

import tempfile
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from trendx.preprocessing.normalizer import Normalizer


@pytest.mark.unit
def test_auto_select_robust_scaler():
    rng = np.random.default_rng(42)
    data = np.concatenate([rng.normal(0, 1, 80), rng.normal(0, 10, 20)])
    normalizer = Normalizer(method="auto")
    normalizer.fit(data)
    assert normalizer.method == "robust"


@pytest.mark.unit
def test_auto_select_minmax_scaler():
    rng = np.random.default_rng(38)
    data = rng.gamma(1.5, 2, 100)
    normalizer = Normalizer(method="auto")
    normalizer.fit(data)
    assert normalizer.method == "minmax"


@pytest.mark.unit
def test_auto_select_standard_scaler():
    rng = np.random.default_rng(42)
    data = rng.normal(0, 1, 200)
    normalizer = Normalizer(method="auto")
    normalizer.fit(data)
    assert normalizer.method == "standard"


@pytest.mark.unit
def test_fit_transform_inverse():
    rng = np.random.default_rng(42)
    data = rng.normal(50, 10, 100)
    normalizer = Normalizer(method="standard")
    transformed = normalizer.fit_transform(data)
    assert abs(float(np.mean(transformed))) < 0.5
    assert abs(float(np.std(transformed)) - 1.0) < 0.2

    reconstructed = normalizer.inverse_transform(transformed)
    np.testing.assert_allclose(data, reconstructed.ravel(), atol=1e-10)


@pytest.mark.unit
def test_save_load_params():
    rng = np.random.default_rng(42)
    data = rng.normal(50, 10, 100)
    normalizer = Normalizer(method="standard")
    normalizer.fit(data)

    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as tmp:
        path = tmp.name

    try:
        normalizer.save_params(path)
        loaded = Normalizer.load_params(path)

        assert loaded.fitted
        assert loaded.method == "standard"

        transformed_orig = normalizer.transform(data[:5])
        transformed_loaded = loaded.transform(data[:5])
        np.testing.assert_allclose(transformed_orig, transformed_loaded)
    finally:
        Path(path).unlink(missing_ok=True)


@pytest.mark.unit
def test_fit_with_pandas_series():
    rng = np.random.default_rng(42)
    s = pd.Series(rng.normal(50, 10, 100), name="voltage")
    normalizer = Normalizer(method="standard")
    result = normalizer.fit_transform(s)
    assert result.shape[0] == 100
    assert normalizer.fitted


@pytest.mark.unit
def test_fit_with_dataframe():
    rng = np.random.default_rng(42)
    df = pd.DataFrame({"a": rng.normal(50, 10, 100), "b": rng.normal(30, 5, 100)})
    normalizer = Normalizer(method="standard")
    result = normalizer.fit_transform(df)
    assert result.shape == (100, 2)


@pytest.mark.unit
def test_invalid_method():
    with pytest.raises(ValueError, match="Unknown scaler method"):
        Normalizer(method="invalid")


@pytest.mark.unit
def test_transform_before_fit_raises():
    normalizer = Normalizer(method="standard")
    with pytest.raises(RuntimeError, match="not been fitted"):
        normalizer.transform(np.array([1.0, 2.0]))


@pytest.mark.unit
def test_all_nan_input():
    data = np.array([np.nan, np.nan, np.nan])
    normalizer = Normalizer(method="standard")
    with pytest.raises(ValueError, match="No finite values"):
        normalizer.fit(data)
