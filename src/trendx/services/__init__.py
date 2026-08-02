from trendx.services.alerting import AlertingService
from trendx.services.tasks import TaskService

try:
    from trendx.services.cache import CacheService
    HAS_CACHE = True
except RuntimeError:
    HAS_CACHE = False
    CacheService = None

try:
    from trendx.services.inference import InferenceService
    HAS_INFERENCE = True
except (RuntimeError, ModuleNotFoundError):
    HAS_INFERENCE = False
    InferenceService = None

try:
    from trendx.services.ingestion import IngestionService
    HAS_INGESTION = True
except (RuntimeError, ModuleNotFoundError):
    HAS_INGESTION = False
    IngestionService = None

try:
    from trendx.services.training import TrainingService
    HAS_TRAINING = True
except (RuntimeError, ModuleNotFoundError):
    HAS_TRAINING = False
    TrainingService = None

__all__ = [
    "AlertingService",
    "CacheService",
    "InferenceService",
    "IngestionService",
    "TaskService",
    "TrainingService",
]
