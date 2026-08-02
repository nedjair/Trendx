from __future__ import annotations

from datetime import datetime, timezone

from airflow.decorators import dag, task
from loguru import logger
from pendulum import duration

from trendx.config import settings
from trendx.anomalies.features import FeatureExtractor
from trendx.anomalies.detectors import BaseDetector, create_detector, DETECTOR_REGISTRY


@dag(
    schedule="0 3 * * *",
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    default_args={
        "retries": 3,
        "retry_delay": duration(minutes=5),
        "retry_exponential_backoff": True,
        "execution_timeout": duration(minutes=120),
    },
    max_active_runs=1,
    tags=["trendx", "anomalies", "training"],
    doc_md="Train anomaly detection models for eligible devices. Extracts statistical features and fits detectors.",
)
def trendx_anomaly_training() -> None:
    @task(
        execution_timeout=duration(minutes=5),
    )
    def get_eligible_devices() -> list[dict]:
        """Identify devices with sufficient telemetry data for anomaly detector training."""
        from trendx.database.connection import manager as db_manager
        from sqlalchemy import text

        engine = db_manager.get_engine("analytics")
        min_points = settings.anomaly_window_size * 7
        stmt = text("""
            SELECT entity_id, metric_key, COUNT(*) AS point_count
            FROM ts_kv
            WHERE ts >= NOW() - INTERVAL '30 days'
              AND dbl_v IS NOT NULL
            GROUP BY entity_id, metric_key
            HAVING COUNT(*) >= :min_pts
        """)
        pairs: list[dict] = []
        with engine.connect() as conn:
            rows = conn.execute(stmt, {"min_pts": min_points}).fetchall()
            for row in rows:
                pairs.append({
                    "entity_id": row[0],
                    "metric_key": row[1],
                    "point_count": row[2],
                })
        logger.info(
            "Found {n} eligible device/metric pairs for anomaly training (min_points={m})",
            n=len(pairs),
            m=min_points,
        )
        return pairs

    @task(
        execution_timeout=duration(minutes=30),
        retries=2,
        max_active_tis_per_dag=4,
    )
    def extract_features(pair: dict) -> dict:
        """Extract windowed statistical features from a device/metric time series."""
        import pandas as pd
        import numpy as np
        from trendx.database.connection import manager as db_manager
        from sqlalchemy import text

        engine = db_manager.get_engine("analytics")
        stmt = text("""
            SELECT ts, dbl_v AS value
            FROM ts_kv
            WHERE entity_id = :eid
              AND metric_key = :key
              AND ts >= NOW() - INTERVAL '30 days'
              AND dbl_v IS NOT NULL
            ORDER BY ts ASC
        """)
        with engine.connect() as conn:
            rows = conn.execute(stmt, {"eid": pair["entity_id"], "key": pair["metric_key"]}).fetchall()

        if not rows:
            return {"entity_id": pair["entity_id"], "metric_key": pair["metric_key"], "status": "insufficient_data"}

        df = pd.DataFrame(rows, columns=["ts", "value"])
        df["ts"] = pd.to_datetime(df["ts"])
        df = df.set_index("ts").resample("1h").mean().interpolate(limit=3)
        series = df["value"].dropna()

        extractor = FeatureExtractor(window_size=settings.anomaly_window_size)
        features_df = extractor.extract_features(series)

        if features_df.empty:
            return {"entity_id": pair["entity_id"], "metric_key": pair["metric_key"], "status": "insufficient_data"}

        features_df = features_df.select_dtypes(include=[np.number])
        internal_cols = {"_start", "_end"}
        feature_cols = [c for c in features_df.columns if c not in internal_cols]

        if len(feature_cols) < 3:
            return {"entity_id": pair["entity_id"], "metric_key": pair["metric_key"], "status": "insufficient_features"}

        feature_array = features_df[feature_cols].values.astype(np.float64)
        finite_mask = np.isfinite(feature_array).all(axis=1)
        if finite_mask.sum() < 10:
            return {"entity_id": pair["entity_id"], "metric_key": pair["metric_key"], "status": "insufficient_clean_data"}

        feature_array = feature_array[finite_mask]
        timestamps = features_df.index[finite_mask]

        import json

        def convert_timestamp(ts):
            if isinstance(ts, pd.Timestamp):
                return ts.isoformat()
            return str(ts)

        return {
            "entity_id": pair["entity_id"],
            "metric_key": pair["metric_key"],
            "status": "features_extracted",
            "n_windows": len(feature_array),
            "n_features": len(feature_cols),
        }

    @task(
        execution_timeout=duration(minutes=30),
        retries=2,
        max_active_tis_per_dag=2,
    )
    def train_detectors(feature_result: dict) -> dict:
        """Train an anomaly detector on the extracted features."""
        if feature_result.get("status") != "features_extracted":
            return {**feature_result, "algorithm": None, "train_status": "skipped"}

        entity_id = feature_result["entity_id"]
        metric_key = feature_result["metric_key"]

        import pandas as pd
        import numpy as np
        from trendx.database.connection import manager as db_manager
        from sqlalchemy import text

        engine = db_manager.get_engine("analytics")
        stmt = text("""
            SELECT ts, dbl_v AS value
            FROM ts_kv
            WHERE entity_id = :eid
              AND metric_key = :key
              AND ts >= NOW() - INTERVAL '30 days'
              AND dbl_v IS NOT NULL
            ORDER BY ts ASC
        """)
        with engine.connect() as conn:
            rows = conn.execute(stmt, {"eid": entity_id, "key": metric_key}).fetchall()

        df = pd.DataFrame(rows, columns=["ts", "value"])
        df["ts"] = pd.to_datetime(df["ts"])
        df = df.set_index("ts").resample("1h").mean().interpolate(limit=3)
        series = df["value"].dropna()

        extractor = FeatureExtractor(window_size=settings.anomaly_window_size)
        features_df = extractor.extract_features(series)
        if features_df.empty:
            return {**feature_result, "train_status": "no_features"}

        features_df = features_df.select_dtypes(include=[np.number])
        internal_cols = {"_start", "_end"}
        feature_cols = [c for c in features_df.columns if c not in internal_cols]
        feature_array = features_df[feature_cols].values.astype(np.float64)
        finite_mask = np.isfinite(feature_array).all(axis=1)
        feature_array = feature_array[finite_mask]

        algorithm = "IsolationForest"
        detector = create_detector(algorithm, {
            "contamination": settings.anomaly_contamination,
            "random_state": 42,
        })
        detector.fit(feature_array)

        from trendx.mlops.tracking import MLflowTracker
        tracker = MLflowTracker()
        run_id = tracker.start_run(
            experiment_name=f"anomaly_{entity_id[:12]}_{metric_key}",
            run_name=f"IsolationForest_{int(datetime.now(timezone.utc).timestamp())}",
            tags={
                "entity_id": entity_id,
                "metric_key": metric_key,
                "algorithm": algorithm,
                "type": "anomaly_detector",
            },
        )

        try:
            from trendx.mlops.registry import ModelRegistry
            registry = ModelRegistry()
            import json
            registry._get_repo()

            tracker.log_params({
                "algorithm": algorithm,
                "contamination": settings.anomaly_contamination,
                "window_size": settings.anomaly_window_size,
                "n_windows": len(feature_array),
                "n_features": len(feature_cols),
            })
            tracker.log_tags({
                "entity_id": entity_id,
                "metric_key": metric_key,
                "type": "anomaly_detector",
            })
            tracker.end_run("FINISHED")
        except Exception as exc:
            logger.error("MLflow logging failed for anomaly detector {eid}/{key}: {exc}",
                         eid=entity_id[:12], key=metric_key, exc=exc)
            tracker.end_run("FAILED")

        logger.info(
            "Anomaly detector trained for {eid}/{key}: algorithm={algo}, windows={w}, features={f}",
            eid=entity_id[:12],
            key=metric_key,
            algo=algorithm,
            w=len(feature_array),
            f=len(feature_cols),
        )
        return {
            "entity_id": entity_id,
            "metric_key": metric_key,
            "algorithm": algorithm,
            "train_status": "trained",
            "n_windows": len(feature_array),
            "n_features": len(feature_cols),
            "mlflow_run_id": run_id,
        }

    @task(
        execution_timeout=duration(minutes=5),
    )
    def register_anomaly_models(train_results: list[dict]) -> dict:
        """Register trained anomaly detectors and log summary."""
        trained = sum(1 for r in train_results if r.get("train_status") == "trained")
        skipped = sum(1 for r in train_results if r.get("train_status") == "skipped")
        failed = sum(1 for r in train_results if r.get("train_status") not in ("trained", "skipped"))
        logger.info(
            "Anomaly model training complete: {ok} trained, {skip} skipped, {fail} failed",
            ok=trained,
            skip=skipped,
            fail=failed,
        )
        return {
            "status": "completed",
            "trained": trained,
            "skipped": skipped,
            "failed": failed,
        }

    pairs = get_eligible_devices()
    features = extract_features.expand(pair=pairs)
    detectors = train_detectors.expand(feature_result=features)
    register_anomaly_models(train_results=detectors)


trendx_anomaly_training()
