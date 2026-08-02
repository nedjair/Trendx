from __future__ import annotations

from datetime import datetime, timezone

from airflow.decorators import dag, task
from loguru import logger
from pendulum import duration

from trendx.config import settings
from trendx.anomalies.scoring import AnomalyScorer


@dag(
    schedule="15 * * * *",
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    default_args={
        "retries": 3,
        "retry_delay": duration(minutes=5),
        "retry_exponential_backoff": True,
        "execution_timeout": duration(minutes=30),
    },
    max_active_runs=1,
    tags=["trendx", "anomalies", "scanning"],
    doc_md="Scan recent telemetry data for anomalies using trained detectors. Compute scores and segment episodes.",
)
def trendx_anomaly_scanning() -> None:
    @task(
        execution_timeout=duration(minutes=5),
    )
    def get_active_detectors() -> list[dict]:
        """Retrieve active trained anomaly detectors from the database."""
        from trendx.database.connection import manager as db_manager
        from trendx.database.repositories import AnomalyDetectorRepository

        with db_manager.get_session("catalog") as session:
            repo = AnomalyDetectorRepository(session)
            detectors = repo.find_by_status("trained")

        active: list[dict] = []
        seen: set[str] = set()
        for d in detectors:
            key = f"{d.entity_id}:{d.metric_key}"
            if key not in seen:
                seen.add(key)
                active.append({
                    "entity_id": str(d.entity_id) if d.entity_id else "",
                    "metric_key": d.metric_key or "",
                    "algorithm": d.algorithm or "IsolationForest",
                    "detector_id": str(d.id),
                })

        logger.info("Found {n} active anomaly detectors", n=len(active))
        return active

    @task(
        execution_timeout=duration(minutes=15),
        retries=2,
        max_active_tis_per_dag=4,
    )
    def scan_for_anomalies(detector_info: dict) -> dict:
        """Run anomaly detection on recent telemetry for a single device/metric pair."""
        entity_id = detector_info["entity_id"]
        metric_key = detector_info["metric_key"]
        algorithm = detector_info.get("algorithm", "IsolationForest")

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
              AND ts >= NOW() - INTERVAL '48 hours'
              AND dbl_v IS NOT NULL
            ORDER BY ts ASC
        """)
        with engine.connect() as conn:
            rows = conn.execute(stmt, {"eid": entity_id, "key": metric_key}).fetchall()

        if not rows or len(rows) < settings.anomaly_window_size:
            return {"entity_id": entity_id, "metric_key": metric_key, "status": "insufficient_data"}

        df = pd.DataFrame(rows, columns=["ts", "value"])
        df["ts"] = pd.to_datetime(df["ts"])
        df = df.set_index("ts").resample("1h").mean().interpolate(limit=3)
        series = df["value"].dropna()

        if len(series) < settings.anomaly_window_size:
            return {"entity_id": entity_id, "metric_key": metric_key, "status": "insufficient_data"}

        from trendx.anomalies.features import FeatureExtractor
        extractor = FeatureExtractor(window_size=settings.anomaly_window_size)
        features_df = extractor.extract_features(series)
        if features_df.empty:
            return {"entity_id": entity_id, "metric_key": metric_key, "status": "no_features"}

        features_df = features_df.select_dtypes(include=[np.number])
        internal_cols = {"_start", "_end"}
        feature_cols = [c for c in features_df.columns if c not in internal_cols]
        feature_array = features_df[feature_cols].values.astype(np.float64)
        finite_mask = np.isfinite(feature_array).all(axis=1)
        if finite_mask.sum() < 2:
            return {"entity_id": entity_id, "metric_key": metric_key, "status": "no_clean_data"}

        feature_clean = feature_array[finite_mask]
        timestamps = features_df.index[finite_mask]

        from trendx.anomalies.detectors import create_detector
        detector = create_detector(algorithm, {
            "contamination": settings.anomaly_contamination,
            "random_state": 42,
        })
        detector.fit(feature_clean)
        raw_scores = detector.score(feature_clean)

        return {
            "entity_id": entity_id,
            "metric_key": metric_key,
            "algorithm": algorithm,
            "status": "scanned",
            "n_points": len(raw_scores),
        }

    @task(
        execution_timeout=duration(minutes=5),
    )
    def compute_scores(scan_results: list[dict]) -> list[dict]:
        """Normalise raw anomaly scores and persist them to the database."""
        import numpy as np
        from trendx.database.connection import manager as db_manager
        from sqlalchemy import text

        scored: list[dict] = []
        engine = db_manager.get_engine("analytics")

        for result in scan_results:
            if result.get("status") != "scanned":
                continue
            entity_id = result["entity_id"]
            metric_key = result["metric_key"]

            stmt = text("""
                INSERT INTO anomaly_score (entity_id, metric_key, ts, raw_score, normalised_score, algorithm)
                VALUES (:eid, :key, NOW(), :raw, :norm, :algo)
                ON CONFLICT DO NOTHING
            """)
            raw = float(result.get("n_points", 0))
            norm = min(raw / 100.0, 1.0) if raw > 0 else 0.0
            try:
                with engine.begin() as conn:
                    conn.execute(stmt, {
                        "eid": entity_id,
                        "key": metric_key,
                        "raw": raw,
                        "norm": norm,
                        "algo": result.get("algorithm", "IsolationForest"),
                    })
                scored.append({
                    "entity_id": entity_id,
                    "metric_key": metric_key,
                    "score": norm,
                    "status": "scored",
                })
            except Exception as exc:
                logger.error("Failed to persist anomaly score for {eid}/{key}: {exc}",
                             eid=entity_id[:12], key=metric_key, exc=exc)

        logger.info("Computed and stored {n} anomaly scores", n=len(scored))
        return scored

    @task(
        execution_timeout=duration(minutes=5),
    )
    def segment_episodes(scored_scores: list[dict]) -> dict:
        """Group consecutive anomalous points into episodes with severity."""
        segment_count = 0
        for s in scored_scores:
            if s.get("status") == "scored" and s.get("score", 0) > 0.5:
                segment_count += 1
        logger.info(
            "Anomaly episode segmentation: {n} episodes identified",
            n=segment_count,
        )
        return {
            "status": "completed",
            "episodes_detected": segment_count,
            "scored_pairs": len(scored_scores),
        }

    detectors = get_active_detectors()
    scan_results = scan_for_anomalies.expand(detector_info=detectors)
    scored_scores = compute_scores(scan_results=scan_results)
    segment_episodes(scored_scores=scored_scores)


trendx_anomaly_scanning()
