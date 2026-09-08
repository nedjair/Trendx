from __future__ import annotations

import json
from pathlib import Path
from typing import Any, ClassVar

import numpy as np
import numpy.typing as npt
import pandas as pd
from loguru import logger
from scipy import stats as scipy_stats
from sklearn.preprocessing import (
    MinMaxScaler as SkMinMaxScaler,
)
from sklearn.preprocessing import (
    RobustScaler as SkRobustScaler,
)
from sklearn.preprocessing import (
    StandardScaler as SkStandardScaler,
)


class Normalizer:
    SCALER_TYPES: ClassVar[dict[str, type]] = {
        "standard": SkStandardScaler,
        "robust": SkRobustScaler,
        "minmax": SkMinMaxScaler,
    }

    def __init__(self, method: str = "auto") -> None:
        if method not in ("auto", "standard", "robust", "minmax"):
            msg = (
                f"Unknown scaler method '{method}'. Use 'auto', 'standard', 'robust', or 'minmax'."
            )
            raise ValueError(msg)
        self._method = method
        self._selected_method: str | None = None
        self._scaler: Any = None
        self._feature_names: list[str] | None = None
        self._fitted: bool = False

    @property
    def method(self) -> str | None:
        return self._selected_method or self._method

    @property
    def fitted(self) -> bool:
        return self._fitted

    @property
    def scaler(self) -> Any:
        return self._scaler

    def _auto_select(self, data: npt.NDArray[np.float64]) -> str:
        if len(data) < 4:
            logger.info("Too few samples ({n}), using StandardScaler", n=len(data))
            return "standard"

        kurtosis = scipy_stats.kurtosis(data, nan_policy="omit")
        skewness = scipy_stats.skew(data, nan_policy="omit")

        q1, q3 = np.percentile(data[~np.isnan(data)], [25, 75])
        iqr = q3 - q1
        outlier_ratio = 0.0
        if iqr > 0:
            lower = q1 - 1.5 * iqr
            upper = q3 + 1.5 * iqr
            outlier_ratio = np.mean((data < lower) | (data > upper))

        logger.debug(
            "Auto-select: kurtosis={k:.2f}, skewness={s:.2f}, outlier_ratio={o:.4f}",
            k=kurtosis,
            s=skewness,
            o=outlier_ratio,
        )

        if abs(kurtosis) > 3.0 or outlier_ratio > 0.05:
            logger.info(
                "High kurtosis ({k:.2f}) or outliers ({o:.2%}), selecting RobustScaler",
                k=kurtosis,
                o=outlier_ratio,
            )
            return "robust"

        if abs(skewness) > 1.5:
            logger.info("Highly skewed ({s:.2f}), selecting MinMaxScaler", s=skewness)
            return "minmax"

        logger.info("Stable distribution, selecting StandardScaler")
        return "standard"

    def fit(
        self,
        data: pd.Series | pd.DataFrame | npt.NDArray[np.float64],
        method: str | None = None,
    ) -> Normalizer:
        method = method or self._method

        if isinstance(data, pd.Series):
            values = data.values.reshape(-1, 1).astype(np.float64)
            self._feature_names = [data.name or "value"]
        elif isinstance(data, pd.DataFrame):
            values = data.select_dtypes(include=[np.number]).values.astype(np.float64)
            self._feature_names = data.select_dtypes(include=[np.number]).columns.tolist()
            if values.shape[1] == 0:
                msg = "DataFrame has no numeric columns to fit"
                raise ValueError(msg)
        elif isinstance(data, np.ndarray):
            if data.ndim == 1:
                values = data.reshape(-1, 1).astype(np.float64)
                self._feature_names = ["value"]
            else:
                values = data.astype(np.float64)
                self._feature_names = [f"feat_{i}" for i in range(values.shape[1])]
        else:
            msg = f"Unsupported data type: {type(data)}"
            raise TypeError(msg)

        values = np.where(np.isinf(values), np.nan, values)
        finite_mask = ~np.isnan(values).all(axis=1)
        fit_data = values[finite_mask]

        if fit_data.shape[0] == 0:
            msg = "No finite values available for fitting"
            raise ValueError(msg)

        if method == "auto":
            selected = self._auto_select(fit_data.flatten())
            self._selected_method = selected
        else:
            self._selected_method = method

        scaler_cls = self.SCALER_TYPES[self._selected_method]
        self._scaler = scaler_cls()
        self._scaler.fit(fit_data)
        self._fitted = True

        params = self.get_params()
        logger.info(
            "Fitted {method} scaler on {n} samples, {features} feature(s)",
            method=self._selected_method,
            n=fit_data.shape[0],
            features=fit_data.shape[1],
        )
        logger.debug("Scaler params: {params}", params=params)
        return self

    def transform(
        self,
        data: pd.Series | pd.DataFrame | npt.NDArray[np.float64],
    ) -> npt.NDArray[np.float64]:
        if not self._fitted or self._scaler is None:
            msg = "Normalizer has not been fitted yet. Call fit() first."
            raise RuntimeError(msg)

        if isinstance(data, pd.Series):
            values = data.values.reshape(-1, 1).astype(np.float64)
        elif isinstance(data, pd.DataFrame):
            values = data.select_dtypes(include=[np.number]).values.astype(np.float64)
        elif isinstance(data, np.ndarray):
            if data.ndim == 1:
                values = data.reshape(-1, 1).astype(np.float64)
            else:
                values = data.astype(np.float64)
        else:
            msg = f"Unsupported data type: {type(data)}"
            raise TypeError(msg)

        values = np.where(np.isinf(values), np.nan, values)
        finite_mask = ~np.isnan(values).all(axis=1)

        if finite_mask.sum() == 0:
            logger.warning("No finite values to transform, returning zeros")
            return np.zeros_like(values)

        result = np.full_like(values, np.nan, dtype=np.float64)
        result[finite_mask] = self._scaler.transform(values[finite_mask])
        return result

    def inverse_transform(
        self,
        data: npt.NDArray[np.float64] | pd.Series | pd.DataFrame,
    ) -> npt.NDArray[np.float64]:
        if not self._fitted or self._scaler is None:
            msg = "Normalizer has not been fitted yet. Call fit() first."
            raise RuntimeError(msg)

        if isinstance(data, pd.Series):
            values = data.values.reshape(-1, 1).astype(np.float64)
        elif isinstance(data, pd.DataFrame):
            values = data.select_dtypes(include=[np.number]).values.astype(np.float64)
        elif isinstance(data, np.ndarray):
            if data.ndim == 1:
                values = data.reshape(-1, 1).astype(np.float64)
            else:
                values = data.astype(np.float64)
        else:
            msg = f"Unsupported data type: {type(data)}"
            raise TypeError(msg)

        finite_mask = ~np.isnan(values).all(axis=1)
        if finite_mask.sum() == 0:
            logger.warning("No finite values to inverse-transform, returning zeros")
            return np.zeros_like(values)

        result = np.full_like(values, np.nan, dtype=np.float64)
        result[finite_mask] = self._scaler.inverse_transform(values[finite_mask])
        return result

    def fit_transform(
        self,
        data: pd.Series | pd.DataFrame | npt.NDArray[np.float64],
        method: str | None = None,
    ) -> npt.NDArray[np.float64]:
        self.fit(data, method=method)
        return self.transform(data)

    def get_params(self) -> dict[str, Any]:
        if not self._fitted or self._scaler is None:
            return {"method": self._method, "fitted": False}

        params: dict[str, Any] = {
            "method": self._selected_method,
            "fitted": True,
            "feature_names": self._feature_names,
        }

        if hasattr(self._scaler, "mean_") and self._scaler.mean_ is not None:
            params["mean"] = self._scaler.mean_.tolist()
        if hasattr(self._scaler, "scale_") and self._scaler.scale_ is not None:
            params["scale"] = self._scaler.scale_.tolist()
        if hasattr(self._scaler, "var_") and self._scaler.var_ is not None:
            params["var"] = self._scaler.var_.tolist()
        if hasattr(self._scaler, "center_") and self._scaler.center_ is not None:
            params["center"] = self._scaler.center_.tolist()
        if hasattr(self._scaler, "data_min_") and self._scaler.data_min_ is not None:
            params["data_min"] = self._scaler.data_min_.tolist()
        if hasattr(self._scaler, "data_max_") and self._scaler.data_max_ is not None:
            params["data_max"] = self._scaler.data_max_.tolist()
        if hasattr(self._scaler, "data_range_") and self._scaler.data_range_ is not None:
            params["data_range"] = self._scaler.data_range_.tolist()
        if hasattr(self._scaler, "n_features_in_"):
            params["n_features_in_"] = self._scaler.n_features_in_
        if hasattr(self._scaler, "feature_names_in_"):
            params["feature_names_in_"] = list(self._scaler.feature_names_in_)

        return params

    def save_params(self, path: str | Path) -> None:
        params = self.get_params()
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            json.dump(params, f, indent=2, default=str)
        logger.info("Scaler parameters saved to {path}", path=path)

    @classmethod
    def load_params(cls, path: str | Path) -> Normalizer:
        path = Path(path)
        with open(path) as f:
            params = json.load(f)

        method = params.get("method", "standard")
        instance = cls(method=method)

        if params.get("fitted", False):
            scaler_cls = instance.SCALER_TYPES.get(method, SkStandardScaler)
            scaler = scaler_cls()

            if "mean" in params and "scale" in params and "var" in params:
                scaler.mean_ = np.array(params["mean"])
                scaler.scale_ = np.array(params["scale"])
                scaler.var_ = np.array(params["var"])
            if "center_" in params:
                scaler.center_ = np.array(params["center"])
            if "data_min_" in params:
                scaler.data_min_ = np.array(params["data_min"])
                scaler.data_max_ = np.array(params["data_max"])
                scaler.data_range_ = np.array(params["data_range"])
            if "n_features_in_" in params:
                scaler.n_features_in_ = params["n_features_in_"]
            if "feature_names_in_" in params:
                scaler.feature_names_in_ = np.array(params["feature_names_in_"])

            instance._scaler = scaler
            instance._selected_method = method
            instance._feature_names = params.get("feature_names")
            instance._fitted = True

        logger.info("Scaler parameters loaded from {path}", path=path)
        return instance
