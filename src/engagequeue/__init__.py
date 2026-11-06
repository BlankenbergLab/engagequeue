"""EngageQueue: Local jobs. Make it so.

Importing the package starts no execution services or threads.
"""

from .errors import (
    AdmissionClosed,
    EngageQueueError,
    LostJobError,
    RuntimeUnavailable,
    SubmissionConflict,
    UnsupportedBackend,
)
from .executor import Executor
from .models import ContainerSpec, JobHandle, JobSpec, JobState, JobStatus, PoolConfig

__version__ = "0.1.0"
__all__ = [
    "Executor",
    "JobSpec",
    "JobHandle",
    "JobStatus",
    "JobState",
    "PoolConfig",
    "ContainerSpec",
    "EngageQueueError",
    "SubmissionConflict",
    "RuntimeUnavailable",
    "LostJobError",
    "UnsupportedBackend",
    "AdmissionClosed",
]
