"""Non-régression fix B1 : MLflowTracker.log_tags appelait
MlflowClient.set_tags, inexistent en MLflow 2.14.3 (set_tag singulier).

Prouvé sur backend filesystem jetable réel : les tags sont persistés et
relisibles via l'API MLflow. Aucun PG, aucun serveur MLflow.
"""

from __future__ import annotations

import pytest

pytestmark = [pytest.mark.unit]


def test_d_tags_persisted_on_file_store(tmp_path) -> None:
    """Test D : log_tags écrit réellement chaque tag (MLflow 2.14.3)."""
    from trendx.mlops.tracking import MLflowTracker

    tracker = MLflowTracker(tracking_uri=f"file://{tmp_path}/mlruns")
    run_id = tracker.start_run(experiment_name="b1-tags", run_name="tags-probe")
    tracker.log_tags({"algorithm": "LinearRegression", "entity": "b1-a", "freq": "1h"})
    tracker.end_run("FINISHED")
    stored = tracker.client.get_run(run_id).data.tags
    assert stored["algorithm"] == "LinearRegression"
    assert stored["entity"] == "b1-a"
    assert stored["freq"] == "1h"


def test_d_tags_empty_dict_is_noop(tmp_path) -> None:
    from trendx.mlops.tracking import MLflowTracker

    tracker = MLflowTracker(tracking_uri=f"file://{tmp_path}/mlruns")
    run_id = tracker.start_run(experiment_name="b1-tags-empty")
    tracker.log_tags({})
    tracker.end_run("FINISHED")
    assert isinstance(tracker.client.get_run(run_id).data.tags, dict)
