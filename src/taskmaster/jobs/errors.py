"""Domain errors. The API maps them to HTTP status codes in one place (api/errors.py)."""


class JobError(Exception):
    """Base class: the message is safe to show to the API caller."""


class JobNotFound(JobError):
    """No such job, or it belongs to someone else (indistinguishable on purpose)."""


class JobConflict(JobError):
    """The job's current status doesn't allow the requested change."""


class IdempotencyConflict(JobError):
    """An Idempotency-Key was reused with a different request."""


class InputFileMissing(JobError):
    """A job payload references an upload that doesn't exist."""


class InputFileTooLarge(JobError):
    """A job payload references an upload above the size limit."""
