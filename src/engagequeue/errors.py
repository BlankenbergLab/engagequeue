"""Public EngageQueue exceptions."""


class EngageQueueError(Exception):
    """Base exception raised by EngageQueue."""


class SubmissionConflict(EngageQueueError):
    """An idempotency key was reused for a different job specification."""


class RuntimeUnavailable(EngageQueueError):
    """The detached runtime could not be started or contacted."""


class LostJobError(EngageQueueError):
    """Execution ownership is uncertain and requires reconciliation."""


class UnsupportedBackend(EngageQueueError):
    """A requested execution backend or limit is unavailable."""


class AdmissionClosed(EngageQueueError):
    """The runtime has been explicitly drained or shut down."""
