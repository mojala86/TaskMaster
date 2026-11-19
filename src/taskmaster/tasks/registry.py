"""Task type registry.

    @task("csv.aggregate", max_retries=3, timeout=600)
    async def aggregate(payload: CsvAggregateInput, ctx: JobContext) -> CsvAggregateOutput: ...

The first parameter's annotation must be a Pydantic model: the API validates submitted payloads
against it, and the worker parses the stored payload into it before calling the handler.
"""

import inspect
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, get_type_hints

from pydantic import BaseModel

from taskmaster.tasks.context import JobContext

TaskFn = Callable[[Any, JobContext], Awaitable[Any]]


@dataclass(frozen=True)
class TaskDefinition:
    name: str
    fn: TaskFn
    input_model: type[BaseModel]
    max_retries: int
    timeout: float

    @property
    def max_attempts(self) -> int:
        return self.max_retries + 1


_registry: dict[str, TaskDefinition] = {}


def task(name: str, *, max_retries: int = 3, timeout: float = 300.0) -> Callable[[TaskFn], TaskFn]:
    def register(fn: TaskFn) -> TaskFn:
        if name in _registry:
            raise ValueError(f"Task {name!r} is already registered")
        if not inspect.iscoroutinefunction(fn):
            raise TypeError(f"Task {name!r} must be an async function")
        params = list(inspect.signature(fn).parameters)
        input_model = get_type_hints(fn).get(params[0]) if params else None
        if not (isinstance(input_model, type) and issubclass(input_model, BaseModel)):
            raise TypeError(f"Task {name!r}: first parameter must be annotated with a BaseModel")
        _registry[name] = TaskDefinition(name, fn, input_model, max_retries, timeout)
        return fn

    return register


def get_task(name: str) -> TaskDefinition | None:
    return _registry.get(name)


def all_tasks() -> list[TaskDefinition]:
    return [_registry[name] for name in sorted(_registry)]
