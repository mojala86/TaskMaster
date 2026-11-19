"""Exceptions shared by the worker, task handlers and the CPU subprocesses.

Kept dependency-free so subprocesses can import it cheaply.
"""


class PermanentError(Exception):
    """Raise from a handler to fail the job immediately, without retries.

    For errors a retry cannot fix: invalid input, a missing file, a malformed CSV.
    """
