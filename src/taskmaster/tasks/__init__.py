"""Task types. Importing this package registers all built-in tasks."""

from taskmaster.errors import PermanentError

# Registers the built-in task types.
from taskmaster.tasks import demo, files  # noqa: F401
from taskmaster.tasks.context import JobContext, OutputFile
from taskmaster.tasks.registry import TaskDefinition, all_tasks, get_task, task
from taskmaster.tasks.types import FileRef, file_refs

__all__ = [
    "FileRef",
    "JobContext",
    "OutputFile",
    "PermanentError",
    "TaskDefinition",
    "all_tasks",
    "file_refs",
    "get_task",
    "task",
]
